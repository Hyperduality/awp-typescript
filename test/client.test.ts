import { test } from "node:test";
import assert from "node:assert/strict";
import { AwpClient, type ClientOptions, type OpenOptions } from "../src/client.ts";
import { AwpError, ManifestInvalidError, TimeoutError, UsageError } from "../src/errors.ts";
import { validate } from "../src/schemas.ts";
import { MockWorld, type MockConn, type Msg, streamingManifest, lockstepManifest, multiBindManifest, sessionReady, pose, b64 } from "./helpers/mock-world.ts";

function client(world: MockWorld, extra: Partial<ClientOptions> = {}): AwpClient {
  return new AwpClient({
    url: world.url,
    token: "agent-secret",
    agent: { name: "test", version: "0", vendor: "test" },
    consumesModalities: ["proprio/json", "text/event+json"],
    obsReportIntervalMs: 200,
    ...extra,
  });
}

/** Connect, initialize, and open a session against the mock world; returns the world side. */
async function open(world: MockWorld, c: AwpClient, opts: { manifest?: Msg; ready?: Msg; mode?: "streaming" | "lockstep"; open?: OpenOptions } = {}): Promise<MockConn> {
  const accepted = world.accept();
  const init = c.initialize();
  const conn = await accepted;
  const i = await conn.next("initialize");
  conn.result(i.id, opts.manifest ?? streamingManifest());
  await init;
  const mode = opts.mode ?? "streaming";
  const opening = c.openSession(mode, opts.open ?? { embodiment: "arm_01", subscribe: ["proprio", "arm_state"] });
  const o = await conn.next("session.open");
  conn.result(o.id, opts.ready ?? sessionReady(mode === "lockstep" ? { tick: 0, granted: { ...sessionReady().granted, channels: [{ channel: "proprio", rate_hz: null, channel_id: 1 }, { channel: "arm_state", rate_hz: null, channel_id: 2 }] } } : {}));
  conn.notify("session.state", { state: "ready", status_seq: 1, ts_mono_ns: 0, reason: "opened" });
  await opening;
  return conn;
}

function frame(channel_id: number, seq: number, extra: Msg = {}): Msg {
  return { channel_id, seq, ts_mono_ns: 1000 * seq, ts_send_ns: 1000 * seq + 10, flags: 1, payload_b64: b64({ p_m: [0, 0, 0.4] }), ...extra };
}

async function withWorld(fn: (world: MockWorld) => Promise<void>): Promise<void> {
  const world = await MockWorld.start();
  try {
    await fn(world);
  } finally {
    await world.stop();
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test("connects with the awp subprotocol and a bearer token; initialize carries the agent manifest", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    assert.equal(conn.protocol, "awp");
    assert.equal(conn.headers.authorization, "Bearer agent-secret");
    const init = conn.received[0]!;
    assert.equal(init.jsonrpc, "2.0");
    assert.deepEqual(init.params.protocol_versions, ["0.1"]);
    assert.deepEqual(validate("agent-manifest", init.params, "sender"), []);
    assert.ok(!conn.received.some(Array.isArray), "no batches (AWP-CTL-006)");
    c.disconnect();
  }));

test("an invalid manifest is refused: no session.open is ever sent (AWP-AGT-002)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const accepted = world.accept();
    const init = c.initialize();
    const conn = await accepted;
    const i = await conn.next("initialize");
    const bad = streamingManifest();
    delete bad.safety_policy.safe_state; // streaming requires safe_state (AWP-MAN-006)
    conn.result(i.id, bad);
    await assert.rejects(init, ManifestInvalidError);
    await assert.rejects(c.openSession("streaming", { embodiment: "arm_01" }), UsageError);
    await sleep(50);
    assert.equal(conn.all("session.open").length, 0);
    c.disconnect();
  }));

test("unknown world requests get -32601, batches -32600, unknown fields are tolerated (AWP-CTL-002, AWP-VER-003)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    conn.send({ jsonrpc: "2.0", id: "u1", method: "x-acme.probe", params: {} });
    const r = await conn.next((m) => m.id === "u1");
    assert.equal(r.error.code, -32601);
    assert.deepEqual(validate("error", r.error, "sender"), []);
    conn.sendRaw(JSON.stringify([{ jsonrpc: "2.0", id: "b1", method: "ping", params: { origin_ns: 1 } }]));
    const b = await conn.next((m) => m.id === null && m.error);
    assert.equal(b.error.code, -32600);
    // Unknown fields everywhere, including an unknown notification.
    conn.notify("session.state", { state: "active", status_seq: 2, ts_mono_ns: 5, reason: "first_activity", future: { x: 1 }, "x-acme.n": 1 });
    conn.notify("x-acme.gossip", { hello: true });
    conn.notify("obs.frame", { ...frame(1, 1), future: 1 });
    await sleep(50);
    assert.equal(c.lastStatusSeq, 2);
    assert.equal(c.latest("proprio")?.frame.seq, 1);
    c.disconnect();
  }));

test("world pings are answered on the agent clock with receive_ns and transmit_ns (AWP-CLK-007)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    conn.send({ jsonrpc: "2.0", id: "p1", method: "ping", params: { origin_ns: 42 } });
    const r = await conn.next((m) => m.id === "p1");
    assert.equal(r.result.origin_ns, 42);
    assert.ok(r.result.transmit_ns >= r.result.receive_ns);
    assert.deepEqual(validate("ping-result", r.result, "sender"), []);
    c.disconnect();
  }));

test("pre-session pongs never enter the clock-offset estimate (AWP-SES-012, AWP-CLK-008)", () =>
  withWorld(async (world) => {
    const c = client(world, { heartbeat: "manual" });
    const accepted = world.accept();
    const init = c.initialize();
    const conn = await accepted;
    conn.sessionClock = () => 7_777_777_777_777; // a world clock that is not the session clock
    const i = await conn.next("initialize");
    conn.result(i.id, streamingManifest());
    await init;
    await c.heartbeat();
    assert.equal(c.estimator.offsetNs, undefined);
    const pre = conn.all("ping")[0]!;
    assert.equal(pre.params.last_status_seq, undefined, "no ack before a session");
    conn.sessionClock = () => 1_000_000;
    const opening = c.openSession("streaming", { embodiment: "arm_01", subscribe: ["proprio"] });
    const o = await conn.next("session.open");
    conn.result(o.id, sessionReady());
    await opening;
    await c.heartbeat();
    assert.ok(c.estimator.samples >= 1);
    // Every sample in the estimate is on the session clock, never the pre-session world clock.
    assert.ok(Math.abs(c.estimator.offsetNs! - (1_000_000 - c.clock.now())) < 50_000_000);
    const ping = conn.all("ping").at(-1)!;
    assert.equal(typeof ping.params.last_status_seq, "number");
    c.disconnect();
  }));

test("heartbeats at the negotiated interval, and twice per watchdog_ms in streaming (AWP-SAF-001, AWP-SAF-005)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const m = streamingManifest();
    m.safety_policy.safe_state.watchdog_ms = 400;
    const conn = await open(world, c, { manifest: m, ready: sessionReady({ heartbeat_interval_ms: 1000 }) });
    const t0 = Date.now();
    await sleep(1300);
    const pings = conn.received.filter((x) => x.method === "ping");
    assert.ok(pings.length >= 4 + Math.floor((Date.now() - t0) / 200) - 1, `only ${pings.length} pings`);
    for (const p of pings) assert.deepEqual(validate("ping", p.params, "sender"), []);
    c.disconnect();
  }));

test("status notifications are deduplicated on status_seq and last_status_seq is gapless (AWP-LIF-009, AWP-CTL-008)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    const submitting = c.submit("move_to_pose", pose(0.1), { preempt: "replace" });
    const s = await conn.next("action.submit");
    assert.deepEqual(validate("action-submit", s.params, "sender"), []);
    conn.result(s.id, { action_id: s.params.action_id, state: "accepted", status_seq: 2, received_ts_mono_ns: 10, ts_mono_ns: 11 });
    conn.notify("action.status", { action_id: s.params.action_id, state: "executing", status_seq: 3, ts_mono_ns: 12, progress: 0 });
    const rec = await submitting;
    conn.notify("action.status", { action_id: s.params.action_id, state: "completed", status_seq: 4, ts_mono_ns: 13, progress: 1 });
    conn.notify("action.status", { action_id: s.params.action_id, state: "completed", status_seq: 4, ts_mono_ns: 13, progress: 1 });
    await rec.settled();
    await sleep(20);
    assert.equal(rec.state, "completed");
    assert.equal(rec.history.length, 3);
    assert.equal(c.lastStatusSeq, 4);
    // A gap holds last_status_seq back so a resumption replays what was missed.
    conn.notify("world.event", { event: "collision", status_seq: 6, ts_mono_ns: 20 });
    await sleep(20);
    assert.equal(c.lastStatusSeq, 4);
    conn.notify("world.event", { event: "collision", status_seq: 5, ts_mono_ns: 19 });
    await sleep(20);
    assert.equal(c.lastStatusSeq, 6);
    c.disconnect();
  }));

test("ungranted types, undeclared preemption, invalid params, and observer sessions never reach the wire (AWP-AGT-003)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const ready = sessionReady();
    ready.granted.action_types = ["stop"];
    const conn = await open(world, c, { ready });
    await assert.rejects(c.submit("move_to_pose", pose(0.1)), UsageError);
    await assert.rejects(c.submit("stop", {}, { preempt: "queue" }), UsageError);
    await assert.rejects(c.submit("stop", { force: true }), UsageError);
    await sleep(20);
    assert.equal(conn.all("action.submit").length, 0);
    c.disconnect();
  }));

test("a non-retryable refusal is never retried identically; a retryable one may be (AWP-ERR-001, AWP-ACT-010)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    const first = c.submit("move_to_pose", pose(0.3));
    const s1 = await conn.next("action.submit");
    conn.error(s1.id, 4001, "AWP_FORBIDDEN", false);
    await assert.rejects(first, (e: unknown) => e instanceof AwpError && !e.retryable);
    await assert.rejects(c.submit("move_to_pose", pose(0.3)), UsageError);
    await assert.rejects(c.submit("move_to_pose", pose(0.3), { preempt: "queue", deadlineMs: 900 }), UsageError, "same type, params, embodiment");
    const busy = c.submit("move_to_pose", pose(0.2));
    const s2 = await conn.next("action.submit");
    conn.error(s2.id, 3002, "AWP_BUSY", true);
    let refusedRec: any;
    await busy.catch((e) => (refusedRec = e));
    assert.ok(refusedRec instanceof AwpError && refusedRec.retryable);
    const rec = c.records.get(s2.params.action_id);
    assert.ok(rec?.refused);
    const again = c.resubmit(rec!);
    const s3 = await conn.next("action.submit");
    assert.deepEqual(s3.params, s2.params, "identical content, same action_id (AWP-ACT-001, AWP-ACT-010)");
    conn.result(s3.id, { action_id: s3.params.action_id, state: "accepted", status_seq: 2, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    const admitted = await again;
    assert.equal(admitted.state, "accepted");
    await assert.rejects(c.submit("move_to_pose", pose(0.9), { actionId: s3.params.action_id }), UsageError, "an admitted id is never reused");
    assert.equal(conn.all("action.submit").length, 3);
    c.disconnect();
  }));

test("action ids are unique; basis is copied unchanged and valid_until goes through the offset (AWP-AGT-004, AWP-CLK-009)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    conn.notify("obs.frame", frame(1, 5, { ts_mono_ns: 123456789 }));
    await c.waitForFrame("proprio");
    const basis = c.latest("proprio")!;
    const ids = new Set<string>();
    for (let i = 0; i < 3; i++) {
      const p = c.submit("move_to_pose", pose(0.1 * i), { basis, validForMs: 200 });
      const s = await conn.next("action.submit");
      ids.add(s.params.action_id);
      assert.equal(s.params.basis_ts_mono_ns, 123456789);
      const expected = c.estimator.toSession(c.clock.now()) + 200e6;
      assert.ok(Math.abs(s.params.valid_until_ns - expected) < 100e6);
      conn.result(s.id, { action_id: s.params.action_id, state: "accepted", status_seq: 2 + i, received_ts_mono_ns: 1, ts_mono_ns: 1 });
      await p;
    }
    assert.equal(ids.size, 3);
    c.disconnect();
  }));

test("cancel and every lifecycle state, including queued and cancelling (AWP-AGT-005)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    const p = c.submit("move_to_pose", pose(0.1), { preempt: "queue" });
    const s = await conn.next("action.submit");
    const id = s.params.action_id;
    conn.result(s.id, { action_id: id, state: "queued", status_seq: 2, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    const rec = await p;
    conn.notify("action.status", { action_id: id, state: "accepted", status_seq: 3, ts_mono_ns: 2 });
    conn.notify("action.status", { action_id: id, state: "executing", status_seq: 4, ts_mono_ns: 3, progress: 0.1 });
    await rec.until((r) => r.state === "executing");
    const cancelling = c.cancel(id);
    const x = await conn.next("action.cancel");
    assert.deepEqual(x.params, { action_id: id });
    // The transition the cancel causes is reported only in its result (AWP-LIF-001).
    conn.result(x.id, { action_id: id, state: "cancelling", status_seq: 5 });
    await cancelling;
    conn.notify("action.status", { action_id: id, state: "cancelled", status_seq: 6, ts_mono_ns: 5, reason: "cancelled_by_agent", aborted_at_progress: 0.1 });
    await rec.settled();
    assert.deepEqual(rec.history.map((h) => h.state), ["queued", "accepted", "executing", "cancelling", "cancelled"]);
    assert.deepEqual(rec.violations, []);
    c.disconnect();
  }));

test("a dropped connection is resumed with last_status_seq; replay is processed; safe state until a new action executes (AWP-AGT-007)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    const p = c.submit("move_to_pose", pose(0.1));
    const s = await conn.next("action.submit");
    const id = s.params.action_id;
    conn.result(s.id, { action_id: id, state: "accepted", status_seq: 2, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    conn.notify("action.status", { action_id: id, state: "executing", status_seq: 3, ts_mono_ns: 2 });
    const rec = await p;
    await rec.until((r) => r.state === "executing");
    const reconnect = world.accept();
    conn.drop();
    const conn2 = await reconnect;
    const i = await conn2.next("initialize");
    conn2.result(i.id, streamingManifest());
    const r = await conn2.next("session.resume");
    assert.deepEqual(r.params, { session_token: "st_abcdefghijklmnop", last_status_seq: 3 });
    assert.deepEqual(validate("session-resume", r.params, "sender"), []);
    conn2.result(r.id, sessionReady({ replay_to_status_seq: 6, safe_state: true }));
    conn2.notify("action.status", { action_id: id, state: "failed", status_seq: 4, ts_mono_ns: 9, reason: "connection_lost" });
    conn2.notify("world.event", { event: "safe_state_entered", status_seq: 5, ts_mono_ns: 9, detail: { embodiment: "arm_01" } });
    conn2.notify("session.state", { state: "suspended", status_seq: 6, ts_mono_ns: 10, reason: "connection_lost" });
    conn2.notify("session.state", { state: "active", status_seq: 7, ts_mono_ns: 11, reason: "resumed" });
    await rec.settled();
    await c.waitReplay();
    assert.equal(rec.state, "failed");
    assert.equal(c.lastStatusSeq, 7);
    assert.equal(c.inSafeState, true);
    // A redelivered replay is deduplicated.
    conn2.notify("action.status", { action_id: id, state: "failed", status_seq: 4, ts_mono_ns: 9, reason: "connection_lost" });
    const p2 = c.submit("move_to_pose", pose(0.2));
    const s2 = await conn2.next("action.submit");
    conn2.result(s2.id, { action_id: s2.params.action_id, state: "accepted", status_seq: 8, received_ts_mono_ns: 12, ts_mono_ns: 12 });
    await p2;
    assert.equal(c.inSafeState, true, "admission alone does not end safe state");
    conn2.notify("world.event", { event: "safe_state_exited", status_seq: 9, ts_mono_ns: 13, detail: { embodiment: "arm_01" } });
    conn2.notify("action.status", { action_id: s2.params.action_id, state: "executing", status_seq: 10, ts_mono_ns: 13 });
    await sleep(30);
    assert.equal(c.inSafeState, false);
    assert.equal(c.resumes, 1);
    c.disconnect();
  }));

test("an unacknowledged submission is re-sent identically after resumption, unless the replay reported it", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    const p = c.submit("move_to_pose", pose(0.1), { preempt: "queue" });
    const s = await conn.next("action.submit");
    const reconnect = world.accept();
    conn.drop(); // the result is lost with the connection
    const conn2 = await reconnect;
    conn2.result((await conn2.next("initialize")).id, streamingManifest());
    const r = await conn2.next("session.resume");
    assert.equal(r.params.last_status_seq, 1);
    conn2.result(r.id, sessionReady({ replay_to_status_seq: 1, safe_state: false }));
    const s2 = await conn2.next("action.submit");
    assert.deepEqual(s2.params, s.params, "identical content (AWP-ACT-001)");
    conn2.result(s2.id, { action_id: s.params.action_id, state: "queued", status_seq: 2, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    const rec = await p;
    assert.equal(rec.state, "queued");

    // Second case: the admission is replayed, so nothing is re-sent.
    const p3 = c.submit("move_to_pose", pose(0.2), { preempt: "queue" });
    const s3 = await conn2.next("action.submit");
    const reconnect2 = world.accept();
    conn2.drop();
    const conn3 = await reconnect2;
    conn3.result((await conn3.next("initialize")).id, streamingManifest());
    const r3 = await conn3.next("session.resume");
    assert.equal(r3.params.last_status_seq, 2);
    conn3.result(r3.id, sessionReady({ replay_to_status_seq: 4, safe_state: false }));
    conn3.notify("action.status", { action_id: s3.params.action_id, state: "queued", status_seq: 3, ts_mono_ns: 2 });
    conn3.notify("session.state", { state: "suspended", status_seq: 4, ts_mono_ns: 3, reason: "connection_replaced" });
    const rec3 = await p3;
    assert.equal(rec3.state, "queued");
    await sleep(50);
    assert.equal(conn3.all("action.submit").length, 0);
    c.disconnect();
  }));

test("resume refused with AWP_SESSION_UNKNOWN closes the session; nothing is assumed to survive (AWP-SES-008)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    const p = c.submit("move_to_pose", pose(0.1));
    const s = await conn.next("action.submit");
    conn.result(s.id, { action_id: s.params.action_id, state: "accepted", status_seq: 2, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    const rec = await p;
    const reconnect = world.accept();
    conn.drop();
    const conn2 = await reconnect;
    conn2.result((await conn2.next("initialize")).id, streamingManifest());
    const r = await conn2.next("session.resume");
    conn2.error(r.id, 2005, "AWP_SESSION_UNKNOWN", false);
    assert.equal(await c.whenClosed(), "AWP_SESSION_UNKNOWN");
    await rec.settled();
    assert.equal(rec.lost, true);
  }));

test("three silent heartbeat intervals mean the connection is lost (AWP-SAF-002)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c, { ready: sessionReady({ heartbeat_interval_ms: 200 }) });
    conn.autoPong = false;
    const reconnect = world.accept(4000);
    const t0 = Date.now();
    const conn2 = await reconnect;
    const elapsed = Date.now() - t0;
    assert.ok(elapsed >= 550 && elapsed < 1500, `detected after ${elapsed} ms`);
    await conn2.next("initialize");
    c.disconnect();
  }));

test("an integer beyond 2^53 − 1 ends the session: session.close, then close 1002 AWP_INTEGER_RANGE, no resume (AWP-CTL-009)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    conn.sendRaw('{"jsonrpc":"2.0","method":"world.event","params":{"event":"collision","status_seq":9007199254740993,"ts_mono_ns":1}}');
    const closed = await conn.whenClosed();
    assert.deepEqual(closed, { code: 1002, reason: "AWP_INTEGER_RANGE" });
    assert.equal(conn.all("session.close").length, 1);
    assert.equal(await c.whenClosed(), "AWP_INTEGER_RANGE");
    await sleep(300);
    assert.equal(world.connections.length, 1, "never resumed");
  }));

test("an out-of-range u64 in a binary frame ends the session the same way (AWP-CTL-009)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const streamUp = world.acceptStream();
    const conn = await open(world, c, { ready: sessionReady({ stream_endpoints: [{ binding: "ws", url: `${world.url}/stream` }, { binding: "inline" }] }) });
    const stream = await streamUp;
    await sleep(30);
    // seq = 2^53 (vector seq_over_2p53, retargeted to channel 1).
    stream.sendFrame(new Uint8Array(Buffer.from("41575046010001000000000000002000000000000000000000000000", "hex")));
    const closed = await conn.whenClosed();
    assert.deepEqual(closed, { code: 1002, reason: "AWP_INTEGER_RANGE" });
    assert.equal(conn.all("session.close").length, 1);
    assert.equal(await c.whenClosed(), "AWP_INTEGER_RANGE");
  }));

test("no loss detection by silence before session.ready, and sessionless pings at least every 5 s (AWP-SES-012)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const accepted = world.accept();
    await c.connect();
    assert.ok(c.pingPeriodMs <= 5000);
    const conn = await accepted;
    conn.autoPong = false;
    const init = c.initialize();
    const i = await conn.next("initialize");
    // A sessionless connection stays up however long the world is silent; here, a resumption handshake
    // after a session with a 200 ms heartbeat interval (three intervals would be 600 ms).
    conn.result(i.id, streamingManifest());
    await init;
    const opening = c.openSession("streaming", { embodiment: "arm_01", subscribe: ["proprio"] });
    conn.autoPong = true;
    conn.result((await conn.next("session.open")).id, sessionReady({ heartbeat_interval_ms: 200 }));
    await opening;
    const reconnect = world.accept();
    conn.drop();
    const conn2 = await reconnect;
    conn2.autoPong = false;
    const i2 = await conn2.next("initialize");
    await sleep(1000);
    assert.equal(conn2.closed, undefined, "the sessionless connection was not dropped for silence");
    conn2.result(i2.id, streamingManifest());
    const r = await conn2.next("session.resume");
    conn2.autoPong = true;
    conn2.result(r.id, sessionReady({ heartbeat_interval_ms: 200, replay_to_status_seq: 1 }));
    await sleep(50);
    assert.equal(c.state, "active");
    c.disconnect();
  }));

test("lockstep resumption: resync keyframes with the current tick complete an advance whose frames were lost (AWP-TIM-009)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c, { manifest: lockstepManifest(), mode: "lockstep" });
    conn.notify("obs.frame", frame(1, 1, { tick: 0, ts_send_ns: undefined }));
    conn.notify("obs.frame", frame(2, 1, { tick: 0, ts_send_ns: undefined }));
    await c.initialObservations();
    const adv = c.advance(1);
    await conn.next("world.tick");
    const reconnect = world.accept();
    conn.drop(); // the advance happened, but its frames and result are lost
    const conn2 = await reconnect;
    conn2.result((await conn2.next("initialize")).id, lockstepManifest());
    const r = await conn2.next("session.resume");
    const ready = sessionReady({ tick: 1, replay_to_status_seq: 1, safe_state: false });
    ready.granted.channels = [{ channel: "proprio", rate_hz: null, channel_id: 1 }, { channel: "arm_state", rate_hz: null, channel_id: 2 }];
    conn2.result(r.id, ready);
    await sleep(30);
    let done = false;
    void adv.then(() => (done = true));
    assert.equal(done, false);
    conn2.notify("obs.frame", frame(1, 3, { tick: 1, flags: 9, ts_send_ns: undefined }));
    conn2.notify("obs.frame", frame(2, 3, { tick: 1, flags: 9, ts_send_ns: undefined }));
    assert.equal(await adv, 1);
    await sleep(20);
    assert.equal(conn2.all("world.tick").length, 0, "the advance is not repeated");
    assert.equal(c.channel("arm_state")!.tracker.lost, 0, "the gap before the resync is not loss");
    c.disconnect();
  }));

test("frames: gaps, resync, reserved bits, ungranted and unconsumed channels (AWP-DAT-001/005/009, AWP-MOD-002)", () =>
  withWorld(async (world) => {
    const c = client(world, { consumesModalities: ["proprio/json"] });
    const accepted = world.accept();
    const init = c.initialize();
    const conn = await accepted;
    conn.result((await conn.next("initialize")).id, streamingManifest());
    await init;
    const opening = c.openSession("streaming", { embodiment: "arm_01", subscribe: ["proprio"] });
    conn.result((await conn.next("session.open")).id, sessionReady());
    await opening;
    const losses: number[] = [];
    c.on("loss", (_n: string, g: number) => losses.push(g));
    conn.notify("obs.frame", frame(1, 1, { flags: 0xf1 }));
    conn.notify("obs.frame", frame(1, 2));
    conn.notify("obs.frame", frame(1, 6)); // latest-wins: gap counted, no loss event
    conn.notify("obs.frame", frame(1, 9, { flags: 9 })); // resync: gap excused
    conn.notify("obs.frame", frame(2, 1)); // text/event+json: not consumed, ignored
    conn.notify("obs.frame", frame(7, 1)); // not granted
    await sleep(50);
    const t = c.channel("proprio")!.tracker;
    assert.equal(t.received, 4);
    assert.equal(t.lost, 3);
    assert.equal(t.resyncSkipped, 2);
    assert.equal(c.channel("arm_state")?.tracker.received ?? 0, 0);
    assert.deepEqual(losses, []);
    c.disconnect();
  }));

test("malformed inline frames are dropped, not processed (AWP-DAT-010)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    conn.notify("obs.frame", frame(2, 1));
    conn.notify("obs.frame", frame(2, 2, { flags: 8 })); // resync without keyframe
    conn.notify("obs.frame", frame(2, 3, { payload_b64: "@@@" })); // not base64
    conn.notify("obs.frame", { channel_id: 2, ts_mono_ns: 1, flags: 1, payload_b64: "" }); // no seq
    conn.notify("obs.frame", frame(2, 4));
    await sleep(50);
    const t = c.channel("arm_state")!.tracker;
    assert.equal(t.received, 2);
    assert.equal(t.lastSeq, 4);
    assert.equal(conn.closed, undefined, "the control connection stays up");
    c.disconnect();
  }));

test("obs.report receiver reports in streaming (AWP-OBS-007)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    for (let i = 1; i <= 5; i++) conn.notify("obs.frame", frame(1, i));
    const report = await conn.next("obs.report", 3000);
    assert.deepEqual(validate("obs-report", report.params, "sender"), []);
    assert.equal(report.params.channels["1"].frames, 5);
    assert.equal(typeof report.params.sync.offset_ns, "number");
    c.disconnect();
  }));

test("lockstep: initial observations, advance completes only with result and per-tick frames (AWP-TIM-003, AWP-TIM-009)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c, { manifest: lockstepManifest(), mode: "lockstep" });
    conn.notify("obs.frame", frame(1, 1, { tick: 0, ts_send_ns: undefined }));
    conn.notify("obs.frame", frame(2, 1, { tick: 0, ts_send_ns: undefined }));
    await c.initialObservations();
    // The result arrives before the frame of one channel (as on a stream binding): not yet complete.
    const adv = c.advance(1);
    const t = await conn.next("world.tick");
    assert.deepEqual(t.params, { expected_tick: 0 });
    conn.notify("obs.frame", frame(1, 2, { tick: 1, ts_send_ns: undefined }));
    conn.result(t.id, { tick: 1 });
    let done = false;
    void adv.then(() => (done = true));
    await sleep(50);
    assert.equal(done, false, "an advance is complete only with a frame for the tick on every per-tick channel");
    conn.notify("obs.frame", frame(2, 2, { tick: 1, ts_send_ns: undefined }));
    assert.equal(await adv, 1);
    assert.equal(c.tick, 1);
    c.disconnect();
  }));

test("AWP_TICK_MISMATCH updates the known tick and nothing is retried (AWP-TIM-011)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c, { manifest: lockstepManifest(), mode: "lockstep" });
    const bad = c.advance(1);
    const t = await conn.next("world.tick");
    conn.error(t.id, 3009, "AWP_TICK_MISMATCH", false, { tick: 4 });
    await assert.rejects(bad, AwpError);
    assert.equal(c.tick, 4);
    await sleep(30);
    assert.equal(conn.all("world.tick").length, 1);
    c.disconnect();
  }));

const lockstepFrame = (channel_id: number, seq: number, tick: number) => frame(channel_id, seq, { tick, ts_send_ns: undefined });

test("lockstep: an advance by another session moves the known tick through its frames (AWP-TIM-012, AWP-TIM-003)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c, { manifest: lockstepManifest(), mode: "lockstep" });
    conn.notify("obs.frame", lockstepFrame(1, 1, 0));
    conn.notify("obs.frame", lockstepFrame(2, 1, 0));
    await c.initialObservations();
    for (let tick = 1; tick <= 2; tick++) {
      conn.notify("obs.frame", lockstepFrame(1, 1 + tick, tick));
      conn.notify("obs.frame", lockstepFrame(2, 1 + tick, tick));
    }
    await sleep(50);
    assert.equal(c.tick, 2);
    const adv = c.advance(1);
    const t = await conn.next("world.tick");
    assert.deepEqual(t.params, { expected_tick: 2 });
    conn.notify("obs.frame", lockstepFrame(1, 4, 3));
    conn.notify("obs.frame", lockstepFrame(2, 4, 3));
    conn.result(t.id, { tick: 3 });
    assert.equal(await adv, 3);
    c.disconnect();
  }));

test("one world.tick is in flight at a time, and an observer session has no tick authority (AWP-TIM-003, AWP-TIM-012)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c, { manifest: lockstepManifest(), mode: "lockstep" });
    // Under a barrier the result waits for the other sessions; a second call meanwhile is refused locally.
    const adv = c.advance(1);
    const t = await conn.next("world.tick");
    await assert.rejects(c.advance(1), UsageError);
    conn.notify("obs.frame", lockstepFrame(1, 1, 1));
    conn.notify("obs.frame", lockstepFrame(2, 1, 1));
    conn.result(t.id, { tick: 1 });
    assert.equal(await adv, 1);
    assert.equal(conn.all("world.tick").length, 1);
    c.disconnect();

    const observer = client(world);
    const oconn = await open(world, observer, { manifest: lockstepManifest(), mode: "lockstep", open: { subscribe: ["proprio"] } });
    assert.equal(oconn.all("session.open")[0]!.params.embodiment, undefined);
    await assert.rejects(observer.advance(1), UsageError);
    await assert.rejects(observer.submit("stop", {}), UsageError);
    assert.equal(oconn.all("world.tick").length + oconn.all("action.submit").length, 0);
    observer.disconnect();
  }));

test("obs.subscribe and obs.unsubscribe replace the grant list; in lockstep a new channel first delivers the current tick (AWP-AGT-003, AWP-TIM-009)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const ready = sessionReady({ tick: 5, granted: { ...sessionReady().granted, channels: [{ channel: "proprio", rate_hz: null, channel_id: 1 }] } });
    const conn = await open(world, c, { manifest: lockstepManifest(), mode: "lockstep", ready, open: { embodiment: "arm_01", subscribe: ["proprio"] } });
    await assert.rejects(c.subscribe(["nonexistent"]), UsageError);
    const subscribing = c.subscribe(["arm_state"]);
    const sub = await conn.next("obs.subscribe");
    assert.deepEqual(sub.params, { channels: [{ channel: "arm_state" }] });
    conn.result(sub.id, { granted: [{ channel: "proprio", rate_hz: null, channel_id: 1 }, { channel: "arm_state", rate_hz: null, channel_id: 2 }] });
    let done = false;
    void subscribing.then(() => (done = true));
    await sleep(50);
    assert.equal(done, false, "the new per-tick channel has not delivered tick 5 yet");
    conn.notify("obs.frame", lockstepFrame(2, 1, 5));
    assert.equal((await subscribing).length, 2);
    assert.equal(c.channel("arm_state")?.grant.channel_id, 2);

    const unsubscribing = c.unsubscribe(["arm_state"]);
    const un = await conn.next("obs.unsubscribe");
    assert.deepEqual(un.params, { channels: ["arm_state"] });
    conn.result(un.id, { granted: [{ channel: "proprio", rate_hz: null, channel_id: 1 }] });
    await unsubscribing;
    assert.equal(c.channel("arm_state"), undefined);
    c.disconnect();
  }));

test("world.snapshot, world.restore, and world.reset need their capability and admin grant; the restored tick is observed (AWP-PRM-005, AWP-PRM-006)", () =>
  withWorld(async (world) => {
    const manifest = lockstepManifest();
    manifest.capabilities = { snapshot: true };
    const granted = { ...sessionReady().granted, channels: [{ channel: "proprio", rate_hz: null, channel_id: 1 }], admin: ["snapshot", "restore"] };
    const c = client(world);
    const conn = await open(world, c, { manifest, mode: "lockstep", ready: sessionReady({ tick: 7, granted }), open: { embodiment: "arm_01", subscribe: ["proprio"], admin: ["snapshot", "restore"] } });
    await assert.rejects(c.reset(), UsageError, "reset is not granted");

    const snap = c.snapshot();
    const s = await conn.next("world.snapshot");
    conn.result(s.id, { snapshot_token: "snap_0123456789" });
    assert.equal(await snap, "snap_0123456789");

    const restoring = c.restore("snap_0123456789");
    const r = await conn.next("world.restore");
    assert.deepEqual(r.params, { snapshot_token: "snap_0123456789" });
    conn.result(r.id, { tick: 3 });
    conn.notify("obs.frame", lockstepFrame(1, 1, 3));
    assert.equal(await restoring, 3);
    assert.equal(c.tick, 3);
    assert.equal(conn.all("world.reset").length, 0);
    c.disconnect();
  }));

test("multi-bind: session.open names embodiments of one multi_bind_group, and every submission names its embodiment (AWP-EMB-005)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const accepted = world.accept();
    const init = c.initialize();
    const conn = await accepted;
    conn.result((await conn.next("initialize")).id, multiBindManifest());
    await init;
    await assert.rejects(c.openSession("streaming", { embodiment: "arm_01", embodiments: ["arm_01", "gripper_01"] }), UsageError, "exclusive");
    await assert.rejects(c.openSession("streaming", { embodiments: ["arm_01", "cart_01"] }), UsageError, "different groups");
    await assert.rejects(c.openSession("streaming", { embodiments: ["arm_01"] }), UsageError, "one embodiment");
    assert.equal(conn.all("session.open").length, 0);

    const opening = c.openSession("streaming", { embodiments: ["arm_01", "gripper_01"], subscribe: ["proprio"] });
    const o = await conn.next("session.open");
    assert.deepEqual(o.params.embodiments, ["arm_01", "gripper_01"]);
    assert.equal(o.params.embodiment, undefined);
    const granted = { ...sessionReady().granted, channels: [{ channel: "proprio", rate_hz: 100, channel_id: 1 }], action_types: ["move_to_pose", "stop", "gripper_move"] };
    conn.result(o.id, sessionReady({ granted }));
    await opening;
    assert.deepEqual(c.embodiments, ["arm_01", "gripper_01"]);
    assert.equal(c.embodiment, undefined);

    await assert.rejects(c.submit("gripper_move", { width_m: 0.04 }), UsageError, "no embodimentId");
    await assert.rejects(c.submit("move_to_pose", pose(0.1), { embodimentId: "gripper_01" }), UsageError, "not offered there");
    await assert.rejects(c.submit("stop", {}, { embodimentId: "cart_01" }), UsageError, "not bound");
    assert.equal(conn.all("action.submit").length, 0);

    const p = c.submit("gripper_move", { width_m: 0.04 }, { embodimentId: "gripper_01" });
    const sub = await conn.next("action.submit");
    assert.equal(sub.params.embodiment_id, "gripper_01");
    conn.result(sub.id, { action_id: sub.params.action_id, state: "accepted", status_seq: 1, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    assert.equal((await p).state, "accepted");

    // Channels of every bound embodiment may be subscribed.
    const subscribing = c.subscribe(["gripper_state"]);
    const s = await conn.next("obs.subscribe");
    conn.result(s.id, { granted: [...granted.channels, { channel: "gripper_state", rate_hz: 10, channel_id: 3 }] });
    await subscribing;
    c.disconnect();
  }));

test("world.tick is explicit: never sent in streaming, and the combined helper is lockstep-only (AWP-AGT-009)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    await assert.rejects(c.advance(1), UsageError);
    await assert.rejects(c.submitAndAdvance("stop", {}), UsageError);
    const p = c.submit("stop", {});
    const s = await conn.next("action.submit");
    conn.result(s.id, { action_id: s.params.action_id, state: "accepted", status_seq: 2, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    await p;
    await sleep(30);
    assert.equal(conn.all("world.tick").length, 0);
    c.disconnect();
  }));

test("session.close waits for the result sent after session.state closed (AWP-SES-011)", () =>
  withWorld(async (world) => {
    const c = client(world);
    const conn = await open(world, c);
    const closing = c.close();
    const x = await conn.next("session.close");
    assert.deepEqual(x.params, {});
    await assert.rejects(c.submit("stop", {}), UsageError);
    conn.notify("session.state", { state: "closed", status_seq: 2, ts_mono_ns: 5, reason: "session_closed" });
    conn.result(x.id, {});
    await closing;
    assert.equal(c.state, "closed");
    await conn.whenClosed();
  }));

test("a plaintext URL to a non-loopback host is refused (AWP-SEC-001)", () => {
  assert.throws(
    () => new AwpClient({ url: "ws://192.0.2.1:8710", agent: { name: "a", version: "1", vendor: "v" }, consumesModalities: ["proprio/json"] }),
    UsageError,
  );
});

test("ws stream binding: bearer session token, resync on move, late inline frames discarded, malformed frames close the stream (AWP-TRN-012/013)", () =>
  withWorld(async (world) => {
    const { encodeBinaryFrame } = await import("../src/frames.ts");
    const c = client(world, { logger: (l, m) => process.env.DEBUG_TEST && console.error(l, m) });
    const streamUp = world.acceptStream();
    const conn = await open(world, c, {
      ready: sessionReady({ stream_endpoints: [{ binding: "ws", url: `${world.url}/stream` }, { binding: "inline" }] }),
    });
    conn.notify("obs.frame", frame(2, 1));
    conn.notify("obs.frame", frame(2, 2));
    const stream = await streamUp;
    assert.equal(stream.headers.authorization, "Bearer st_abcdefghijklmnop");
    assert.equal(stream.ws.protocol, "awp");
    assert.ok(!stream.url.includes("st_"), "no credential in the URL (AWP-SEC-006)");
    await sleep(30);
    // First frame on the stream: a resync keyframe; the gap it reveals is not loss.
    stream.sendFrame(encodeBinaryFrame({ channel_id: 2, seq: 5, ts_mono_ns: 5000, keyframe: true, resync: true, ts_send_ns: 5010 }));
    await sleep(30);
    // A late inline frame of the same channel is discarded, not counted (AWP-TRN-012).
    conn.notify("obs.frame", frame(2, 3));
    stream.sendFrame(encodeBinaryFrame({ channel_id: 2, seq: 6, ts_mono_ns: 6000, ts_send_ns: 6010 }));
    await sleep(30);
    const t = c.channel("arm_state")!.tracker;
    assert.equal(t.lastSeq, 6);
    assert.equal(t.lost, 0);
    assert.equal(t.lateDiscarded, 1);
    assert.equal(t.resyncSkipped, 2);
    // A message that is not a valid frame closes the stream with AWP_MALFORMED; the agent reconnects (AWP-TRN-010).
    const again = world.acceptStream();
    const v2 = encodeBinaryFrame({ channel_id: 2, seq: 7, ts_mono_ns: 7000, ts_send_ns: 7010 });
    v2[4] = 2; // version 2: malformed (AWP-DAT-010)
    const before = t.received;
    stream.sendFrame(v2);
    const closed = await stream.whenClosed();
    assert.deepEqual(closed, { code: 1002, reason: "AWP_MALFORMED" });
    assert.equal(t.received, before, "the malformed frame was dropped");
    assert.equal(t.lastSeq, 6);
    const s2 = await again;
    assert.equal(s2.headers.authorization, "Bearer st_abcdefghijklmnop");
    // The re-established stream resumes with a resync keyframe (AWP-DAT-010).
    s2.sendFrame(encodeBinaryFrame({ channel_id: 2, seq: 9, ts_mono_ns: 9000, keyframe: true, resync: true, ts_send_ns: 9010 }));
    await sleep(30);
    assert.equal(t.lastSeq, 9);
    assert.equal(t.lost, 0);
    s2.ws.send("text is not a frame");
    assert.deepEqual(await s2.whenClosed(), { code: 1002, reason: "AWP_MALFORMED" });
    c.disconnect();
  }));

test("command frames: only while the bound streaming action executes, within the rate, latest-wins replaces (AWP-CMD-003, AWP-DAT-002/003)", () =>
  withWorld(async (world) => {
    const m = streamingManifest();
    m.capabilities = { command_channels: true };
    m.command_channels = [{ id: "servo_arm", modality: "servo/json", rate_hz: 10, loss_class: "latest-wins", schema: {} }];
    m.action_schemas.push({
      type: "servo",
      params_schema: { type: "object" },
      duration: "streaming",
      command_channel: "servo_arm",
      watchdog_ms: 200,
      preemption: "replace",
    });
    m.embodiments[0].action_types.push("servo");
    const ready = sessionReady();
    ready.granted.action_types.push("servo");
    ready.granted.channels.push({ channel: "servo_arm", rate_hz: 10, channel_id: 3 });
    const c = client(world);
    const conn = await open(world, c, { manifest: m, ready });
    const payload = (n: number) => new Uint8Array(Buffer.from(JSON.stringify({ q_target_rad: [n] })));
    await assert.rejects(c.sendCommandFrame("servo_arm", payload(0)), UsageError, "no bound action executing");
    const p = c.submit("servo", {});
    const s = await conn.next("action.submit");
    conn.result(s.id, { action_id: s.params.action_id, state: "accepted", status_seq: 2, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    conn.notify("action.status", { action_id: s.params.action_id, state: "executing", status_seq: 3, ts_mono_ns: 2 });
    const rec = await p;
    await rec.until((r) => r.state === "executing");
    assert.equal(await c.sendCommandFrame("servo_arm", payload(1)), "sent");
    assert.equal(await c.sendCommandFrame("servo_arm", payload(2)), "held");
    assert.equal(await c.sendCommandFrame("servo_arm", payload(3)), "replaced");
    await sleep(250);
    const frames = conn.all("cmd.frame").map((x) => x.params);
    assert.equal(frames.length, 2);
    assert.deepEqual(frames.map((f) => JSON.parse(Buffer.from(f.payload_b64, "base64").toString()).q_target_rad[0]), [1, 3]);
    assert.deepEqual(frames.map((f) => f.seq), [1, 2]);
    for (const f of frames) {
      assert.deepEqual(validate("frame-inline", f, "sender"), []);
      assert.equal(f.ts_send_ns, undefined);
    }
    c.disconnect();
  }));

test("a lost stream connection's command channel is not sent inline until it is re-established (AWP-TRN-010, AWP-TRN-008)", () =>
  withWorld(async (world) => {
    const { decodeBinaryFrame } = await import("../src/frames.ts");
    const m = streamingManifest();
    m.capabilities = { command_channels: true };
    m.command_channels = [{ id: "servo_arm", modality: "servo/json", rate_hz: 1000, loss_class: "latest-wins", schema: {} }];
    m.action_schemas.push({ type: "servo", params_schema: { type: "object" }, duration: "streaming", command_channel: "servo_arm", watchdog_ms: 200, preemption: "replace" });
    m.embodiments[0].action_types.push("servo");
    const ready = sessionReady({ stream_endpoints: [{ binding: "ws", url: `${world.url}/stream` }, { binding: "inline" }] });
    ready.granted.action_types.push("servo");
    ready.granted.channels.push({ channel: "servo_arm", rate_hz: 1000, channel_id: 3 });
    const c = client(world);
    const streamUp = world.acceptStream();
    const conn = await open(world, c, { manifest: m, ready });
    const stream = await streamUp;
    await sleep(30);
    const p = c.submit("servo", {});
    const s = await conn.next("action.submit");
    conn.result(s.id, { action_id: s.params.action_id, state: "accepted", status_seq: 2, received_ts_mono_ns: 1, ts_mono_ns: 1 });
    conn.notify("action.status", { action_id: s.params.action_id, state: "executing", status_seq: 3, ts_mono_ns: 2 });
    await (await p).until((r) => r.state === "executing");
    const payload = new Uint8Array(Buffer.from('{"v_mps":[0,0,0]}'));
    assert.equal(await c.sendCommandFrame("servo_arm", payload), "sent");
    await sleep(30);
    assert.equal(stream.received.length, 1, "the command channel travels on the stream connection");
    assert.equal(decodeBinaryFrame(new Uint8Array(stream.received[0]!)).channel_id, 3);

    // The stream is lost while the control connection remains: setpoints are dropped, never sent inline.
    const dropped: string[] = [];
    c.on("command_dropped", (name: string) => dropped.push(name));
    const again = world.acceptStream();
    stream.ws.terminate();
    await sleep(20);
    assert.equal(await c.sendCommandFrame("servo_arm", payload), "dropped");
    assert.deepEqual(dropped, ["servo_arm"]);
    assert.equal(conn.all("cmd.frame").length, 0);

    // Re-established: the channel is back on the stream connection.
    const s2 = await again;
    await sleep(30);
    await sleep(5);
    assert.equal(await c.sendCommandFrame("servo_arm", payload), "sent");
    await sleep(30);
    assert.equal(s2.received.length, 1);
    assert.equal(conn.all("cmd.frame").length, 0);

    // After session.resume every channel is inline until a stream is re-established (AWP-TRN-008).
    const reconnect = world.accept();
    conn.drop();
    const conn2 = await reconnect;
    conn2.result((await conn2.next("initialize")).id, m);
    const r = await conn2.next("session.resume");
    const resumed = sessionReady({ replay_to_status_seq: 3, safe_state: false });
    resumed.granted = ready.granted;
    conn2.result(r.id, resumed);
    await c.waitReplay();
    await sleep(20);
    assert.equal(await c.sendCommandFrame("servo_arm", payload), "sent");
    await sleep(20);
    assert.equal(conn2.all("cmd.frame").length, 1, "inline after resumption");
    c.disconnect();
  }));
