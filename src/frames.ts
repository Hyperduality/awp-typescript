/**
 * The frame envelope (spec/transport/frames): the 28-byte binary header, the optional TLV extension
 * block, and the inline JSON form carried by `obs.frame` / `cmd.frame` (AWP-DAT-004).
 */
import { ProtocolError } from "./errors.ts";
import { i64ToNumber, u64ToNumber, MAX_SAFE } from "./ints.ts";

export const FRAME_MAGIC = "AWPF";
export const FRAME_VERSION = 1;
export const HEADER_BYTES = 28;

export const Flag = {
  KEYFRAME: 0x01,
  END_OF_BURST: 0x02,
  HAS_EXTENSIONS: 0x04,
  RESYNC: 0x08,
} as const;

/** Bits 4–7 are reserved: zero on send, ignored on receive (AWP-DAT-005). */
export const DEFINED_FLAG_BITS = 0x0f;

export const ExtType = {
  TICK: 0x01,
  TS_SIM_NS: 0x02,
  TS_SEND_NS: 0x03,
} as const;

const REGISTERED_LEN: Record<number, number> = { [ExtType.TICK]: 8, [ExtType.TS_SIM_NS]: 8, [ExtType.TS_SEND_NS]: 8 };

export interface VendorExtension {
  /** 0x80–0xFF. */
  type: number;
  value: Uint8Array;
}

/** A decoded frame, identical whichever binding carried it. */
export interface Frame {
  channel_id: number;
  seq: number;
  ts_mono_ns: number;
  /** Flag bits 0–3 as received (reserved bits masked off). In the inline form bit 2 is always 0. */
  flags: number;
  keyframe: boolean;
  end_of_burst: boolean;
  resync: boolean;
  tick?: number;
  ts_sim_ns?: number;
  ts_send_ns?: number;
  /** Binary vendor extensions (0x80–0xFF). */
  vendor: VendorExtension[];
  /** Inline vendor extension fields (`x-<vendor>.<name>`). */
  vendorFields: Record<string, unknown>;
  payload: Uint8Array;
}

export interface FrameFields {
  channel_id: number;
  seq: number;
  ts_mono_ns: number;
  keyframe?: boolean;
  end_of_burst?: boolean;
  resync?: boolean;
  tick?: number;
  ts_sim_ns?: number;
  ts_send_ns?: number;
  vendor?: VendorExtension[];
  vendorFields?: Record<string, unknown>;
  payload?: Uint8Array;
}

function malformed(msg: string): never {
  throw new ProtocolError("AWP_MALFORMED", msg);
}

/**
 * Decodes one binary frame (AWP-DAT-006, AWP-DAT-008). Throws ProtocolError with AWP_MALFORMED for a
 * malformed frame (AWP-DAT-010: bad magic, version ≠ 1, a length other than the header and extension
 * block give, an inconsistent ext_len, a registered entry of the wrong len, a repeated type, resync
 * without keyframe) and AWP_INTEGER_RANGE for a 64-bit value above 2^53 − 1 (AWP-CTL-009).
 */
export function decodeBinaryFrame(bytes: Uint8Array): Frame {
  if (bytes.length < HEADER_BYTES) malformed(`frame is ${bytes.length} bytes; the header alone is ${HEADER_BYTES}`);
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes[0] !== 0x41 || bytes[1] !== 0x57 || bytes[2] !== 0x50 || bytes[3] !== 0x46) malformed("bad magic (expected AWPF)");
  const version = dv.getUint8(4);
  if (version !== FRAME_VERSION) malformed(`unsupported frame version ${version}`);
  const rawFlags = dv.getUint8(5);
  const flags = rawFlags & DEFINED_FLAG_BITS;
  const channel_id = dv.getUint16(6, true);
  const seq = u64ToNumber(dv.getBigUint64(8, true), "seq");
  const ts_mono_ns = u64ToNumber(dv.getBigUint64(16, true), "ts_mono_ns");
  const payloadLen = dv.getUint32(24, true);

  let offset = HEADER_BYTES;
  const frame: Frame = {
    channel_id,
    seq,
    ts_mono_ns,
    flags,
    keyframe: (flags & Flag.KEYFRAME) !== 0,
    end_of_burst: (flags & Flag.END_OF_BURST) !== 0,
    resync: (flags & Flag.RESYNC) !== 0,
    vendor: [],
    vendorFields: {},
    payload: new Uint8Array(0),
  };

  if (flags & Flag.HAS_EXTENSIONS) {
    if (bytes.length < HEADER_BYTES + 2) malformed("has_extensions set but no ext_len");
    const extLen = dv.getUint16(HEADER_BYTES, true);
    const start = HEADER_BYTES + 2;
    const end = start + extLen;
    if (end > bytes.length) malformed(`ext_len ${extLen} exceeds the frame`);
    const seen = new Set<number>();
    let p = start;
    while (p < end) {
      if (p + 2 > end) malformed("truncated extension entry header");
      const type = bytes[p]!;
      const len = bytes[p + 1]!;
      p += 2;
      if (p + len > end) malformed(`extension 0x${type.toString(16)} (len ${len}) overruns ext_len`);
      if (seen.has(type)) malformed(`duplicate extension type 0x${type.toString(16)}`);
      seen.add(type);
      const registered = REGISTERED_LEN[type];
      if (registered !== undefined && len !== registered) {
        malformed(`registered extension 0x${type.toString(16)} has len ${len}, expected ${registered}`);
      }
      if (type === ExtType.TICK) frame.tick = u64ToNumber(dv.getBigUint64(p, true), "tick");
      else if (type === ExtType.TS_SIM_NS) frame.ts_sim_ns = i64ToNumber(dv.getBigInt64(p, true), "ts_sim_ns");
      else if (type === ExtType.TS_SEND_NS) frame.ts_send_ns = u64ToNumber(dv.getBigUint64(p, true), "ts_send_ns");
      else if (type >= 0x80) frame.vendor.push({ type, value: bytes.slice(p, p + len) });
      // 0x00 and 0x04–0x7F are reserved: skipped using len like any unknown type (AWP-DAT-006).
      p += len;
    }
    offset = end;
  }

  if (offset + payloadLen !== bytes.length) {
    malformed(`payload_len ${payloadLen} does not match the ${bytes.length - offset} bytes after the header`);
  }
  if (frame.resync && !frame.keyframe) malformed("resync without keyframe (AWP-DAT-010)");
  frame.payload = bytes.slice(offset, offset + payloadLen);
  return frame;
}

function checkU53(v: number, field: string): void {
  if (!Number.isSafeInteger(v) || v < 0) throw new ProtocolError(v > MAX_SAFE ? "AWP_INTEGER_RANGE" : "AWP_MALFORMED", `${field} = ${v} is not an unsigned integer ≤ 2^53 − 1`);
}

function senderFlags(f: FrameFields): number {
  if (f.resync && f.keyframe === false) malformed("a resync frame must be a keyframe (AWP-DAT-009)");
  let flags = 0;
  if (f.keyframe || f.resync) flags |= Flag.KEYFRAME;
  if (f.end_of_burst) flags |= Flag.END_OF_BURST;
  if (f.resync) flags |= Flag.RESYNC;
  return flags;
}

/** Encodes a binary frame. Bit 2 is set iff an extension block is present; reserved bits are zero. */
export function encodeBinaryFrame(f: FrameFields): Uint8Array {
  checkU53(f.seq, "seq");
  checkU53(f.ts_mono_ns, "ts_mono_ns");
  if (f.channel_id < 0 || f.channel_id > 0xffff || !Number.isInteger(f.channel_id)) malformed(`channel_id ${f.channel_id} out of range`);
  const entries: { type: number; value: Uint8Array }[] = [];
  const u64 = (v: number, field: string) => {
    checkU53(v, field);
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, BigInt(v), true);
    return b;
  };
  if (f.tick !== undefined) entries.push({ type: ExtType.TICK, value: u64(f.tick, "tick") });
  if (f.ts_sim_ns !== undefined) {
    if (!Number.isSafeInteger(f.ts_sim_ns)) throw new ProtocolError("AWP_INTEGER_RANGE", "ts_sim_ns out of range");
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigInt64(0, BigInt(f.ts_sim_ns), true);
    entries.push({ type: ExtType.TS_SIM_NS, value: b });
  }
  if (f.ts_send_ns !== undefined) entries.push({ type: ExtType.TS_SEND_NS, value: u64(f.ts_send_ns, "ts_send_ns") });
  const seen = new Set(entries.map((e) => e.type));
  for (const v of f.vendor ?? []) {
    if (v.type < 0x80 || v.type > 0xff) malformed(`vendor extension type 0x${v.type.toString(16)} outside 0x80–0xFF`);
    if (v.value.length > 255) malformed("extension value longer than 255 bytes");
    if (seen.has(v.type)) malformed(`duplicate extension type 0x${v.type.toString(16)}`);
    seen.add(v.type);
    entries.push(v);
  }
  const payload = f.payload ?? new Uint8Array(0);
  const extLen = entries.reduce((n, e) => n + 2 + e.value.length, 0);
  if (extLen > 0xffff) malformed("extension block too large");
  const hasExt = entries.length > 0;
  const total = HEADER_BYTES + (hasExt ? 2 + extLen : 0) + payload.length;
  const out = new Uint8Array(total);
  const dv = new DataView(out.buffer);
  out.set([0x41, 0x57, 0x50, 0x46], 0);
  dv.setUint8(4, FRAME_VERSION);
  dv.setUint8(5, senderFlags(f) | (hasExt ? Flag.HAS_EXTENSIONS : 0));
  dv.setUint16(6, f.channel_id, true);
  dv.setBigUint64(8, BigInt(f.seq), true);
  dv.setBigUint64(16, BigInt(f.ts_mono_ns), true);
  dv.setUint32(24, payload.length, true);
  let p = HEADER_BYTES;
  if (hasExt) {
    dv.setUint16(p, extLen, true);
    p += 2;
    for (const e of entries) {
      out[p] = e.type;
      out[p + 1] = e.value.length;
      out.set(e.value, p + 2);
      p += 2 + e.value.length;
    }
  }
  out.set(payload, p);
  return out;
}

/** The inline JSON form (`obs.frame` / `cmd.frame` params, schemas/v0.1/frame-inline.schema.json). */
export interface InlineFrame {
  channel_id: number;
  seq: number;
  ts_mono_ns: number;
  flags: number;
  tick?: number;
  ts_sim_ns?: number;
  ts_send_ns?: number;
  payload_b64: string;
  [vendorField: `x-${string}`]: unknown;
}

const BASE64 = /^[A-Za-z0-9+/]*={0,2}$/;
const VENDOR_FIELD = /^x-[a-z0-9]+\./;

function intField(p: Record<string, unknown>, name: string, required: boolean, signed = false): number | undefined {
  const v = p[name];
  if (v === undefined) {
    if (required) malformed(`inline frame lacks ${name}`);
    return undefined;
  }
  if (typeof v !== "number" || !Number.isInteger(v)) malformed(`inline frame ${name} is not an integer`);
  if (!Number.isSafeInteger(v)) throw new ProtocolError("AWP_INTEGER_RANGE", `${name} = ${v} exceeds 2^53 − 1`);
  if (!signed && v < 0) malformed(`inline frame ${name} is negative`);
  return v;
}

/**
 * Decodes the inline JSON form. Unknown fields are ignored (AWP-VER-003); reserved flag bits are
 * ignored (AWP-DAT-005); bit 2 carries no meaning inline (AWP-DAT-004) and is masked off.
 */
export function decodeInlineFrame(params: unknown): Frame {
  if (params === null || typeof params !== "object" || Array.isArray(params)) malformed("inline frame params is not an object");
  const p = params as Record<string, unknown>;
  const channel_id = intField(p, "channel_id", true)!;
  if (channel_id > 0xffff) malformed(`channel_id ${channel_id} exceeds u16`);
  const seq = intField(p, "seq", true)!;
  const ts_mono_ns = intField(p, "ts_mono_ns", true)!;
  const rawFlags = intField(p, "flags", true)!;
  const flags = rawFlags & DEFINED_FLAG_BITS & ~Flag.HAS_EXTENSIONS;
  const b64 = p.payload_b64;
  if (typeof b64 !== "string" || !BASE64.test(b64) || b64.length % 4 === 1) malformed("payload_b64 is not base64");
  const frame: Frame = {
    channel_id,
    seq,
    ts_mono_ns,
    flags,
    keyframe: (flags & Flag.KEYFRAME) !== 0,
    end_of_burst: (flags & Flag.END_OF_BURST) !== 0,
    resync: (flags & Flag.RESYNC) !== 0,
    vendor: [],
    vendorFields: {},
    payload: new Uint8Array(Buffer.from(b64, "base64")),
  };
  if (frame.resync && !frame.keyframe) malformed("resync without keyframe (AWP-DAT-010)");
  const tick = intField(p, "tick", false);
  if (tick !== undefined) frame.tick = tick;
  const tsSim = intField(p, "ts_sim_ns", false, true);
  if (tsSim !== undefined) frame.ts_sim_ns = tsSim;
  const tsSend = intField(p, "ts_send_ns", false);
  if (tsSend !== undefined) frame.ts_send_ns = tsSend;
  for (const [k, v] of Object.entries(p)) if (VENDOR_FIELD.test(k)) frame.vendorFields[k] = v;
  return frame;
}

/** Encodes the inline JSON form; `flags` carries bits 0, 1, and 3 only (AWP-DAT-004). */
export function encodeInlineFrame(f: FrameFields): InlineFrame {
  checkU53(f.seq, "seq");
  checkU53(f.ts_mono_ns, "ts_mono_ns");
  const out: InlineFrame = {
    channel_id: f.channel_id,
    seq: f.seq,
    ts_mono_ns: f.ts_mono_ns,
    flags: senderFlags(f),
    payload_b64: Buffer.from(f.payload ?? new Uint8Array(0)).toString("base64"),
  };
  if (f.tick !== undefined) {
    checkU53(f.tick, "tick");
    out.tick = f.tick;
  }
  if (f.ts_sim_ns !== undefined) out.ts_sim_ns = f.ts_sim_ns;
  if (f.ts_send_ns !== undefined) {
    checkU53(f.ts_send_ns, "ts_send_ns");
    out.ts_send_ns = f.ts_send_ns;
  }
  for (const [k, v] of Object.entries(f.vendorFields ?? {})) {
    if (!VENDOR_FIELD.test(k)) malformed(`vendor field ${k} lacks the x-<vendor>. prefix (AWP-VER-004)`);
    (out as unknown as Record<string, unknown>)[k] = v;
  }
  return out;
}

/** Frame fields of a decoded frame, suitable for re-encoding in either form. */
export function frameFields(f: Frame): FrameFields {
  const out: FrameFields = {
    channel_id: f.channel_id,
    seq: f.seq,
    ts_mono_ns: f.ts_mono_ns,
    keyframe: f.keyframe,
    end_of_burst: f.end_of_burst,
    resync: f.resync,
    vendor: f.vendor,
    vendorFields: f.vendorFields,
    payload: f.payload,
  };
  if (f.tick !== undefined) out.tick = f.tick;
  if (f.ts_sim_ns !== undefined) out.ts_sim_ns = f.ts_sim_ns;
  if (f.ts_send_ns !== undefined) out.ts_send_ns = f.ts_send_ns;
  return out;
}

/** Decodes a JSON payload (the `*\/json` and `text/event+json` modalities are UTF-8 JSON). */
export function jsonPayload<T = unknown>(f: Frame): T {
  return JSON.parse(Buffer.from(f.payload).toString("utf8")) as T;
}
