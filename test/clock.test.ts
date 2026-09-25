import { test } from "node:test";
import assert from "node:assert/strict";
import { OffsetEstimator, clockSample, MonotonicClock, OFFSET_WINDOW } from "../src/clock.ts";

test("the worked example of spec/loop/time-models: rtt 50 µs, offset 912998975000 ns", () => {
  const s = clockSample(5000000, 913004000000, 913004050000, 5100000);
  assert.equal(s.rtt_ns, 50000);
  assert.equal(s.offset_ns, 912998975000);
  // session clock = agent clock + offset
  assert.equal(5000000 + s.offset_ns + 25000, 913004000000);
});

test("the estimate is the minimum-RTT sample of the most recent eight (AWP-CLK-008)", () => {
  const e = new OffsetEstimator();
  assert.equal(e.offsetNs, undefined);
  // A very good early sample falls out of the window after eight newer ones.
  e.add(clockSample(0, 1000, 1000, 10)); // rtt 10, offset 995
  for (let i = 1; i <= OFFSET_WINDOW; i++) e.add(clockSample(i * 1000, i * 1000 + 500 + 50 * i, i * 1000 + 500 + 50 * i, i * 1000 + 100 * i));
  assert.equal(e.samples, OFFSET_WINDOW + 1);
  const best = e.best!;
  assert.equal(best.rtt_ns, 100); // i = 1
  assert.equal(e.errorBoundNs, 50);
  assert.equal(e.toSession(0), best.offset_ns);
  assert.equal(e.toAgent(e.toSession(1234)), 1234);
});

test("impossible negative-RTT samples never win", () => {
  const e = new OffsetEstimator();
  e.add(clockSample(0, 100, 200, 50)); // rtt = 50 - 100 = -50
  assert.equal(e.offsetNs, undefined);
});

test("the agent clock is monotonic, non-negative nanoseconds (AWP-CLK-006)", () => {
  const c = new MonotonicClock();
  const a = c.now();
  const b = c.now();
  assert.ok(a >= 0 && b >= a && Number.isSafeInteger(b));
});

test("clients share one process-wide agent clock, so origin_ns never goes backward across sessions (AWP-CLK-006)", async () => {
  const { AwpClient } = await import("../src/client.ts");
  const mk = () => new AwpClient({ url: "ws://127.0.0.1:1", agent: { name: "a", version: "1", vendor: "v" }, consumesModalities: ["proprio/json"] });
  const a = mk();
  const t = a.clock.now();
  const b = mk();
  assert.equal(a.clock, b.clock);
  assert.ok(b.clock.now() >= t);
});
