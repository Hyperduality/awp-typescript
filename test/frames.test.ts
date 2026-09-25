import { test } from "node:test";
import assert from "node:assert/strict";
import {
  decodeBinaryFrame,
  decodeInlineFrame,
  encodeBinaryFrame,
  encodeInlineFrame,
  frameFields,
  Flag,
  type Frame,
} from "../src/frames.ts";
import { ProtocolError, ErrorCode } from "../src/errors.ts";
import { readVendored, validate } from "../src/schemas.ts";

interface Vector {
  name: string;
  description: string;
  hex: string;
  expect?: Record<string, unknown>;
  expect_error?: string;
}

const vectors = (readVendored("test-vectors/frames.json") as { vectors: Vector[] }).vectors;
const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");
const bytes = (h: string) => new Uint8Array(Buffer.from(h, "hex"));

/** Projects a decoded frame onto the field names the vectors use. */
function project(f: Frame, keys: string[]): Record<string, unknown> {
  const all: Record<string, unknown> = {
    channel_id: f.channel_id,
    seq: f.seq,
    ts_mono_ns: f.ts_mono_ns,
    flags: f.flags,
    keyframe: f.keyframe,
    end_of_burst: f.end_of_burst,
    resync: f.resync,
    payload_len: f.payload.length,
    payload_hex: hex(f.payload),
    tick: f.tick,
    ts_sim_ns: f.ts_sim_ns,
    ts_send_ns: f.ts_send_ns,
    vendor: f.vendor.map((v) => ({ type: v.type, value: [...v.value] })),
  };
  return Object.fromEntries(keys.map((k) => [k, all[k]]));
}

test("the vendored vector file covers every case AWP-DAT-008 lists (0.1-draft.9)", () => {
  const names = new Set(vectors.map((v) => v.name));
  for (const n of [
    "minimal",
    "keyframe_tick",
    "tick_and_ts_sim",
    "streaming_ts_send",
    "unknown_extension",
    "zero_payload",
    "resync_keyframe",
    "reserved_bits_set",
    "ext_len_exceeds_frame",
    "wrong_registered_len",
    "duplicate_type",
    "payload_len_mismatch",
    "trailing_bytes",
    "bad_magic",
    "unknown_version",
    "resync_without_keyframe",
    "seq_over_2p53",
  ]) {
    assert.ok(names.has(n), `missing vector ${n}`);
  }
});

for (const v of vectors.filter((x) => x.expect)) {
  test(`vector ${v.name} decodes to its listed fields`, () => {
    const f = decodeBinaryFrame(bytes(v.hex));
    const expect = v.expect!;
    assert.deepEqual(project(f, Object.keys(expect)), expect);
    // Fields the vector does not list are absent.
    for (const k of ["tick", "ts_sim_ns", "ts_send_ns"]) if (!(k in expect)) assert.equal(f[k as keyof Frame], undefined, k);
    if (!("vendor" in expect)) assert.equal(f.vendor.length, 0);
    if (!("resync" in expect)) assert.equal(f.resync, false);
  });

  test(`vector ${v.name} round-trips through the encoder and the inline form`, () => {
    const f = decodeBinaryFrame(bytes(v.hex));
    const again = decodeBinaryFrame(encodeBinaryFrame(frameFields(f)));
    assert.deepEqual(again, f);
    const inline = encodeInlineFrame(frameFields(f));
    assert.deepEqual(validate("frame-inline", inline, "sender"), []);
    assert.equal(inline.flags & Flag.HAS_EXTENSIONS, 0, "bit 2 is 0 inline (AWP-DAT-004)");
    const fromInline = decodeInlineFrame(inline);
    assert.deepEqual({ ...fromInline, flags: fromInline.flags, vendor: f.vendor }, { ...f, flags: f.flags & ~Flag.HAS_EXTENSIONS });
  });
}

for (const name of ["minimal", "keyframe_tick", "tick_and_ts_sim", "negative_ts_sim", "streaming_ts_send", "zero_payload", "resync_keyframe", "max_seq_safe"]) {
  test(`vector ${name} re-encodes byte for byte`, () => {
    const v = vectors.find((x) => x.name === name)!;
    assert.equal(hex(encodeBinaryFrame(frameFields(decodeBinaryFrame(bytes(v.hex))))), v.hex);
  });
}

for (const v of vectors.filter((x) => x.expect_error)) {
  test(`vector ${v.name} is rejected with ${v.expect_error}`, () => {
    assert.throws(
      () => decodeBinaryFrame(bytes(v.hex)),
      (err: unknown) => err instanceof ProtocolError && err.code === ErrorCode[v.expect_error as keyof typeof ErrorCode],
    );
  });
}

test("reserved flag bits never cause rejection and are masked on receipt (AWP-DAT-005)", () => {
  const f = decodeBinaryFrame(encodeBinaryFrame({ channel_id: 1, seq: 1, ts_mono_ns: 0, keyframe: true }));
  const raw = encodeBinaryFrame(frameFields(f));
  raw[5] = raw[5]! | 0xf0;
  const g = decodeBinaryFrame(raw);
  assert.equal(g.flags, Flag.KEYFRAME);
  const inline = decodeInlineFrame({ channel_id: 1, seq: 1, ts_mono_ns: 0, flags: 0xf1, payload_b64: "" });
  assert.equal(inline.flags, Flag.KEYFRAME);
  assert.equal(inline.keyframe, true);
});

test("the encoder sets bit 2 iff an extension block is present and never sets reserved bits", () => {
  const plain = encodeBinaryFrame({ channel_id: 2, seq: 5, ts_mono_ns: 9, end_of_burst: true });
  assert.equal(plain[5], Flag.END_OF_BURST);
  assert.equal(plain.length, 28);
  const ext = encodeBinaryFrame({ channel_id: 2, seq: 5, ts_mono_ns: 9, tick: 3 });
  assert.equal(ext[5], Flag.HAS_EXTENSIONS);
  assert.equal(ext.length, 28 + 2 + 10);
});

test("a resync frame is always sent as a keyframe (AWP-DAT-009)", () => {
  const f = decodeBinaryFrame(encodeBinaryFrame({ channel_id: 1, seq: 9, ts_mono_ns: 0, resync: true }));
  assert.equal(f.keyframe, true);
  assert.equal(f.resync, true);
  assert.throws(() => encodeBinaryFrame({ channel_id: 1, seq: 9, ts_mono_ns: 0, resync: true, keyframe: false }), ProtocolError);
});

test("vendor extensions are surfaced, duplicates refused on send", () => {
  const f = decodeBinaryFrame(
    encodeBinaryFrame({ channel_id: 1, seq: 1, ts_mono_ns: 0, vendor: [{ type: 0x9a, value: new Uint8Array([1, 2]) }] }),
  );
  assert.deepEqual(f.vendor.map((v) => [v.type, [...v.value]]), [[0x9a, [1, 2]]]);
  assert.throws(() =>
    encodeBinaryFrame({
      channel_id: 1,
      seq: 1,
      ts_mono_ns: 0,
      vendor: [
        { type: 0x9a, value: new Uint8Array([1]) },
        { type: 0x9a, value: new Uint8Array([2]) },
      ],
    }),
  );
});

test("inline frames with resync but no keyframe are malformed (AWP-DAT-010)", () => {
  assert.throws(() => decodeInlineFrame({ channel_id: 1, seq: 1, ts_mono_ns: 0, flags: 8, payload_b64: "" }), ProtocolError);
});

test("reserved extension types 0x00 and 0x04–0x7F are skipped", () => {
  // tick entry, then 0x00 (len 1) and 0x7f (len 0).
  const b = bytes("41575046010401000100000000000000000000000000000000000000" + "0f00" + "01080500000000000000" + "000100" + "7f00");
  const f = decodeBinaryFrame(b);
  assert.equal(f.tick, 5);
  assert.equal(f.vendor.length, 0);
});

test("trailing bytes after the payload are malformed", () => {
  const b = encodeBinaryFrame({ channel_id: 1, seq: 1, ts_mono_ns: 0, payload: new Uint8Array([1, 2]) });
  const longer = new Uint8Array(b.length + 1);
  longer.set(b);
  assert.throws(() => decodeBinaryFrame(longer), (e: unknown) => e instanceof ProtocolError && e.code === ErrorCode.AWP_MALFORMED);
});

test("a frame version other than 1 is malformed", () => {
  const b = encodeBinaryFrame({ channel_id: 1, seq: 1, ts_mono_ns: 0 });
  b[4] = 2;
  assert.throws(() => decodeBinaryFrame(b), ProtocolError);
});

test("an extension entry overrunning ext_len is malformed", () => {
  // ext_len = 3 but the tick entry needs 10.
  const b = bytes("415750460104010001000000000000000000000000000000000000000300010801000000000000000000");
  assert.throws(() => decodeBinaryFrame(b), ProtocolError);
});

test("inline frames: unknown fields ignored, vendor fields kept, integers bounded", () => {
  const f = decodeInlineFrame({
    channel_id: 3,
    seq: 7,
    ts_mono_ns: 10,
    ts_send_ns: 12,
    flags: 1,
    payload_b64: Buffer.from('{"a":1}').toString("base64"),
    future_field: { x: 1 },
    "x-acme.lamport": 44,
  });
  assert.equal(f.ts_send_ns, 12);
  assert.deepEqual(f.vendorFields, { "x-acme.lamport": 44 });
  assert.equal(Buffer.from(f.payload).toString(), '{"a":1}');
  assert.throws(
    () => decodeInlineFrame({ channel_id: 1, seq: 2 ** 53, ts_mono_ns: 0, flags: 0, payload_b64: "" }),
    (e: unknown) => e instanceof ProtocolError && e.code === ErrorCode.AWP_INTEGER_RANGE,
  );
  assert.throws(() => decodeInlineFrame({ channel_id: 1, seq: 1, ts_mono_ns: 0, flags: 0, payload_b64: "not base64!" }), ProtocolError);
  assert.throws(() => decodeInlineFrame({ channel_id: 1, ts_mono_ns: 0, flags: 0, payload_b64: "" }), ProtocolError);
});

test("inline encoding never carries bit 2 and matches the sender schema", () => {
  const inline = encodeInlineFrame({ channel_id: 7, seq: 1201, ts_mono_ns: 913005202000, payload: new Uint8Array([1]) });
  assert.deepEqual(inline, { channel_id: 7, seq: 1201, ts_mono_ns: 913005202000, flags: 0, payload_b64: "AQ==" });
  assert.deepEqual(validate("frame-inline", inline, "sender"), []);
});
