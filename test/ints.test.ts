import { test } from "node:test";
import assert from "node:assert/strict";
import { assertSendable, parseJsonChecked, MAX_SAFE } from "../src/ints.ts";
import { ProtocolError, ErrorCode } from "../src/errors.ts";

test("integer literals beyond ±(2^53 − 1) are detected exactly (AWP-CTL-009)", () => {
  assert.deepEqual(parseJsonChecked(`{"seq":${MAX_SAFE}}`).outOfRange, []);
  assert.deepEqual(parseJsonChecked(`{"seq":9007199254740992}`).outOfRange, ["seq"]);
  assert.deepEqual(parseJsonChecked(`{"a":{"ts_mono_ns":18446744073709551615}}`).outOfRange, ["ts_mono_ns"]);
  assert.deepEqual(parseJsonChecked(`{"ts_sim_ns":-9007199254740992}`).outOfRange, ["ts_sim_ns"]);
  assert.deepEqual(parseJsonChecked(`{"ts_sim_ns":-9007199254740991}`).outOfRange, []);
  // Floats are not integer literals (exact only where JSON.parse exposes source text, V8 ≥ 11.4).
  let sourceAccess = false;
  JSON.parse("1", ((_k: string, v: unknown, ctx?: { source?: string }) => ((sourceAccess = ctx?.source !== undefined), v)) as never);
  if (sourceAccess) assert.deepEqual(parseJsonChecked(`{"maximum":1e300,"x":0.5}`).outOfRange, []);
});

test("the sender never emits an out-of-range or non-finite number", () => {
  assert.doesNotThrow(() => assertSendable({ origin_ns: MAX_SAFE }));
  assert.throws(
    () => assertSendable({ origin_ns: MAX_SAFE + 1 }),
    (e: unknown) => e instanceof ProtocolError && e.code === ErrorCode.AWP_INTEGER_RANGE,
  );
  assert.throws(() => assertSendable({ v: [1, Number.NaN] }), ProtocolError);
  assert.throws(() => assertSendable({ v: 1n }), ProtocolError);
});
