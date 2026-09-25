import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { checkManifest, Manifest } from "../src/manifest.ts";
import { schemaNames, validate } from "../src/schemas.ts";
import { SPEC_REVISION } from "../src/version.ts";

import { repoRoot } from "./helpers/paths.ts";

const root = repoRoot();
const specRepo = process.env.AWP_SPEC_REPO ?? join(root, "..", "agent-world-protocol");
const specManifest = () => JSON.parse(readFileSync(join(root, "test", "fixtures", "spec-world-manifest.json"), "utf8"));
const simManifest = () => JSON.parse(readFileSync(join(root, "conformance", "manifest-streaming.json"), "utf8"));

test("SPEC_REVISION names the targeted draft (AWP-VER-009)", () => {
  assert.equal(SPEC_REVISION, "0.1-draft.9");
});

test("every canonical schema is vendored and compiles in both forms", () => {
  const names = schemaNames();
  for (const n of ["world-manifest", "session-ready", "action-status", "frame-inline", "obs-report", "common"]) assert.ok(names.includes(n), n);
  for (const n of names) {
    validate(n, {}, "receiver");
    validate(n, {}, "sender");
  }
});

test("vendored schemas match the specification repository (sync --check)", { skip: !existsSync(join(specRepo, "schemas")) }, () => {
  const out = execFileSync(process.execPath, [join(root, "scripts", "sync-schemas.mjs"), "--check", "--from", specRepo], { encoding: "utf8" });
  assert.match(out, /match/);
});

test("receiver form tolerates unknown fields; sender form rejects them on closed objects", () => {
  const pong = { origin_ns: 1, receive_ns: 2, transmit_ns: 3, extra: true };
  assert.deepEqual(validate("ping-result", pong, "receiver"), []);
  assert.notDeepEqual(validate("ping-result", pong, "sender"), []);
  // Vendor x- fields stay admissible where the schema declares them.
  const frame = { channel_id: 1, seq: 1, ts_mono_ns: 0, flags: 0, payload_b64: "", "x-acme.note": 1 };
  assert.deepEqual(validate("frame-inline", frame, "sender"), []);
});

test("64-bit fields are bounded by 2^53 − 1 in the schemas", () => {
  assert.notDeepEqual(validate("ping", { origin_ns: 2 ** 53 }, "receiver"), []);
  assert.deepEqual(validate("ping", { origin_ns: 2 ** 53 - 1 }, "receiver"), []);
});

test("the specification's example manifest validates (AWP-AGT-002)", () => {
  const r = checkManifest(specManifest(), ["0.1"]);
  assert.deepEqual(r.problems, []);
});

test("the reference world's manifest validates and its params_schema resolves $defs and common.schema.json", () => {
  const m = new Manifest(simManifest());
  assert.deepEqual(checkManifest(m.raw, ["0.1"]).problems, []);
  assert.deepEqual(m.checkParams("move_to_pose", { pose: { frame: "base", p_m: [0, 0, 0.4], q: [0, 0, 0, 1] } }), []);
  assert.notDeepEqual(m.checkParams("move_to_pose", { pose: { frame: "base", p_m: [0, 0], q: [0, 0, 0, 1] } }), []);
  assert.notDeepEqual(m.checkParams("move_to_pose", { pose: { frame: "base", p_m: [0, 0, 0], q: [0, 0, 0, 1] }, extra: 1 }), []);
  assert.deepEqual(m.preemptionPolicies("stop"), ["replace"]);
  assert.deepEqual(m.preemptionPolicies("move_to_pose"), ["replace", "queue", "reject"]);
});

test("manifests the agent must refuse", () => {
  const cases: [string, (m: Record<string, any>) => void][] = [
    ["streaming without safe_state (AWP-MAN-006)", (m) => delete m.safety_policy.safe_state],
    ["lockstep without tick_policy", (m) => delete m.tick_policy],
    ["embodiment referencing an undeclared channel (AWP-MAN-001)", (m) => m.embodiments[0].channels.push("nope")],
    ["embodiment referencing an undeclared action type (AWP-MAN-001)", (m) => m.embodiments[0].action_types.push("fly")],
    ["protocol_version with a draft suffix (AWP-VER-008)", (m) => (m.protocol_version = "0.1-draft.8")],
    ["protocol_version not offered (AWP-VER-002)", (m) => (m.protocol_version = "0.2")],
    ["params_schema that is not a JSON Schema (AWP-MAN-002)", (m) => (m.action_schemas[2].params_schema = { type: 12 })],
    ["params_schema with an unresolvable $ref", (m) => (m.action_schemas[0].params_schema = { $ref: "#/$defs/missing" })],
    ["no embodiments", (m) => (m.embodiments = [])],
    ["duplicate channel ids", (m) => m.observation_channels.push({ ...m.observation_channels[0] })],
    ["time_models not an array", (m) => (m.time_models = "streaming")],
  ];
  for (const [name, mutate] of cases) {
    const m = specManifest();
    mutate(m);
    assert.equal(checkManifest(m, ["0.1"]).valid, false, name);
  }
});

test("unknown manifest fields and capability keys are tolerated (AWP-VER-003, AWP-MAN-004)", () => {
  const m = specManifest();
  m.future_field = { any: 1 };
  m.capabilities["x-acme.turbo"] = true;
  m.capabilities.teleport = true;
  m.embodiments[0].new_thing = [];
  assert.deepEqual(checkManifest(m, ["0.1"]).problems, []);
});

test("sender form: closed objects admit x-<vendor>. fields, and x-awp-lint constraints apply (AWP-VER-004)", async () => {
  const { senderForm } = await import("../src/schemas.ts");
  assert.deepEqual(validate("ping", { origin_ns: 1, "x-acme.trace": "t" }, "sender"), []);
  assert.notDeepEqual(validate("ping", { origin_ns: 1, trace: "t" }, "sender"), []);
  const frame = { channel_id: 1, seq: 1, ts_mono_ns: 0, payload_b64: "" };
  assert.deepEqual(validate("frame-inline", { ...frame, flags: 9 }, "sender"), []);
  assert.notDeepEqual(validate("frame-inline", { ...frame, flags: 8 }, "sender"), [], "resync without keyframe fails the lint enum");
  assert.notDeepEqual(validate("frame-inline", { ...frame, flags: 0x11 }, "sender"), [], "reserved bits fail the lint enum");
  assert.deepEqual(validate("frame-inline", { ...frame, flags: 0x11 }, "receiver"), [], "receivers ignore x-awp-lint");
  assert.deepEqual(senderForm({ type: "object", "x-awp-closed": true }), {
    type: "object",
    "x-awp-closed": true,
    patternProperties: { "^x-[a-z0-9]+\\.": {} },
    additionalProperties: false,
  });
});

test("draft.9 schema changes: channel_id ≥ 1, no negotiating, clamped_count required, session.ready fields required", () => {
  assert.notDeepEqual(validate("subscribe-result", { granted: [{ channel: "a", rate_hz: null, channel_id: 0 }] }, "receiver"), []);
  assert.notDeepEqual(validate("session-state", { state: "negotiating", status_seq: 1, ts_mono_ns: 0 }, "receiver"), []);
  const status = { action_id: "a", state: "executing", status_seq: 1, ts_mono_ns: 0 };
  assert.notDeepEqual(validate("action-status", { ...status, stream: { frames_applied: 1, last_seq: 1 } }, "receiver"), []);
  assert.deepEqual(validate("action-status", { ...status, stream: { frames_applied: 1, last_seq: 1, clamped_count: 0 } }, "receiver"), []);
  assert.deepEqual(validate("error", { code: -32600, message: "Invalid Request" }, "receiver"), []);
});
