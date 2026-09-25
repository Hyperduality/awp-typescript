import { test } from "node:test";
import assert from "node:assert/strict";
import { ChannelTracker, latencyStats } from "../src/channels.ts";
import type { Frame } from "../src/frames.ts";

let clock = 0;
function frame(channel_id: number, seq: number, extra: Partial<Frame> = {}): Frame {
  return {
    channel_id,
    seq,
    ts_mono_ns: seq * 1000,
    flags: extra.resync ? 9 : extra.keyframe ? 1 : 0,
    keyframe: false,
    end_of_burst: false,
    resync: false,
    vendor: [],
    vendorFields: {},
    payload: new Uint8Array(),
    ...extra,
  };
}
const rx = (f: Frame, connection = 1) => ({ frame: f, receivedAt: (clock += 1000), connection });

test("seq gaps count as loss (AWP-DAT-001)", () => {
  const t = new ChannelTracker(1, "reliable");
  assert.deepEqual(t.observe(rx(frame(1, 1, { keyframe: true }))), { kind: "accept", gap: 0, resync: false });
  assert.deepEqual(t.observe(rx(frame(1, 2))), { kind: "accept", gap: 0, resync: false });
  assert.deepEqual(t.observe(rx(frame(1, 5))), { kind: "accept", gap: 2, resync: false });
  assert.equal(t.lost, 2);
});

test("the gap before a resync frame is not loss and delta state is discarded (AWP-DAT-009)", () => {
  const t = new ChannelTracker(1, "reliable");
  t.observe(rx(frame(1, 1, { keyframe: true })));
  t.observe(rx(frame(1, 2)));
  t.observe(rx(frame(1, 3)));
  assert.equal(t.deltaState.length, 2);
  const v = t.observe(rx(frame(1, 40, { keyframe: true, resync: true })));
  assert.deepEqual(v, { kind: "accept", gap: 36, resync: true });
  assert.equal(t.lost, 0);
  assert.equal(t.resyncSkipped, 36);
  assert.equal(t.deltaState.length, 0);
});

test("a late frame from another connection is discarded silently (AWP-TRN-012)", () => {
  const t = new ChannelTracker(1, "latest-wins");
  t.observe(rx(frame(1, 10), 1));
  t.observe(rx(frame(1, 11), 2));
  assert.deepEqual(t.observe(rx(frame(1, 9), 1)), { kind: "discard_late" });
  assert.equal(t.lost, 0);
  assert.equal(t.violations, 0);
  // On the same connection a non-increasing seq is a sender violation.
  assert.equal(t.observe(rx(frame(1, 11), 2)).kind, "discard_violation");
});

test("channels are independent: interleaving across channels is not loss (AWP-OBS-003)", () => {
  const a = new ChannelTracker(1, "reliable");
  const b = new ChannelTracker(2, "reliable");
  const order: [ChannelTracker, number][] = [[a, 1], [b, 1], [b, 2], [a, 2], [b, 3], [a, 3], [a, 4], [b, 4]];
  for (const [t, s] of order) assert.equal(t.observe(rx(frame(t.channelId, s))).kind, "accept");
  assert.equal(a.lost + b.lost, 0);
});

test("receiver-report window: frames, gaps, jitter, staleness (AWP-OBS-007)", () => {
  const t = new ChannelTracker(3, "latest-wins");
  // Agent receipt R, send S: transit varies by 100 ns between frames.
  t.observe({ frame: frame(3, 1, { ts_send_ns: 1000 }), receivedAt: 5000, connection: 1 }, 0);
  t.observe({ frame: frame(3, 3, { ts_send_ns: 3000 }), receivedAt: 7100, connection: 1 }, 0);
  const w = t.takeWindow()!;
  assert.equal(w.frames, 2);
  assert.equal(w.gaps, 1);
  assert.equal(w.jitter_ns, 100);
  assert.equal(w.staleness_ns!.count, 2);
  assert.equal(t.takeWindow(), undefined);
});

test("latency stats are non-negative integers with p50 ≤ p95 ≤ max", () => {
  assert.equal(latencyStats([]), undefined);
  const s = latencyStats([5, 1, 3, 2, 4, -7, 1.6])!;
  assert.equal(s.count, 7);
  assert.ok(s.p50 <= s.p95 && s.p95 <= s.max!);
  assert.ok([s.p50, s.p95, s.max!].every((v) => Number.isInteger(v) && v >= 0));
});
