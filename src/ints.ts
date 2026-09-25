/**
 * 64-bit integers in JSON (AWP-CTL-009): every 64-bit field travels as a JSON integer no larger than
 * 2^53 − 1 in magnitude. A receiver that observes a larger value closes the session with
 * AWP_INTEGER_RANGE; a sender never produces one.
 */
import { ProtocolError } from "./errors.ts";

export const MAX_SAFE = Number.MAX_SAFE_INTEGER; // 2^53 − 1
export const MAX_SAFE_BIG = BigInt(MAX_SAFE);

const INTEGER_LITERAL = /^-?\d+$/;

export interface ParsedJson {
  value: unknown;
  /** JSON paths of integer literals whose magnitude exceeds 2^53 − 1. */
  outOfRange: string[];
}

/**
 * Parses one JSON text and reports every integer literal outside ±(2^53 − 1).
 *
 * Where the runtime exposes the literal's source text to the reviver (V8 ≥ 11.4) the check is exact;
 * otherwise it falls back to `Number.isSafeInteger`, which flags every integer-valued number ≥ 2^53
 * (any literal above 2^53 − 1 parses to at least 2^53).
 */
export function parseJsonChecked(text: string): ParsedJson {
  const outOfRange: string[] = [];
  const value = JSON.parse(text, function (this: unknown, key: string, v: unknown, context?: { source?: string }) {
    if (typeof v === "number") {
      const source = context?.source;
      if (source !== undefined) {
        if (INTEGER_LITERAL.test(source)) {
          const big = BigInt(source);
          if (big > MAX_SAFE_BIG || big < -MAX_SAFE_BIG) outOfRange.push(key);
        }
      } else if (Number.isInteger(v) && !Number.isSafeInteger(v)) {
        outOfRange.push(key);
      }
    }
    return v;
  } as (this: unknown, key: string, value: unknown) => unknown);
  return { value, outOfRange };
}

/** Throws if any number in an outgoing value is an integer outside ±(2^53 − 1) or is not finite. */
export function assertSendable(value: unknown, path = "$"): void {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new ProtocolError("AWP_MALFORMED", `${path} is not a finite number`);
    if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
      throw new ProtocolError("AWP_INTEGER_RANGE", `${path} = ${value} exceeds 2^53 − 1`);
    }
    return;
  }
  if (typeof value === "bigint") throw new ProtocolError("AWP_MALFORMED", `${path} is a bigint; encode 64-bit fields as numbers`);
  if (Array.isArray(value)) {
    value.forEach((v, i) => assertSendable(v, `${path}[${i}]`));
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const [k, v] of Object.entries(value)) assertSendable(v, `${path}.${k}`);
  }
}

/** Converts an unsigned 64-bit value read from a binary frame, enforcing the 2^53 − 1 bound. */
export function u64ToNumber(v: bigint, field: string): number {
  if (v > MAX_SAFE_BIG) throw new ProtocolError("AWP_INTEGER_RANGE", `${field} = ${v} exceeds 2^53 − 1`);
  return Number(v);
}

/** Converts a signed 64-bit value read from a binary frame, enforcing the ±(2^53 − 1) bound. */
export function i64ToNumber(v: bigint, field: string): number {
  if (v > MAX_SAFE_BIG || v < -MAX_SAFE_BIG) throw new ProtocolError("AWP_INTEGER_RANGE", `${field} = ${v} exceeds 2^53 − 1`);
  return Number(v);
}
