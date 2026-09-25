#!/usr/bin/env node
// AWP-DAT-008 evidence: runs this SDK's frame decoder over the vendored schemas/test-vectors/frames.json
// and prints (or writes with --out) one result per vector. Exits non-zero if any vector fails.
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { decodeBinaryFrame, encodeBinaryFrame, frameFields, SPEC_REVISION, errorName } from "../dist/index.js";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const file = join(root, "schemas", "test-vectors", "frames.json");
const { vectors } = JSON.parse(readFileSync(file, "utf8"));
const hex = (b) => Buffer.from(b).toString("hex");
const results = vectors.map((v) => {
  const bytes = new Uint8Array(Buffer.from(v.hex, "hex"));
  try {
    const f = decodeBinaryFrame(bytes);
    if (v.expect_error) return { name: v.name, expect_error: v.expect_error, outcome: "fail", detail: "decoded without error" };
    const got = {
      channel_id: f.channel_id, seq: f.seq, ts_mono_ns: f.ts_mono_ns, flags: f.flags, keyframe: f.keyframe,
      end_of_burst: f.end_of_burst, resync: f.resync, payload_len: f.payload.length, payload_hex: hex(f.payload),
      tick: f.tick, ts_sim_ns: f.ts_sim_ns, ts_send_ns: f.ts_send_ns,
      vendor: f.vendor.map((x) => ({ type: x.type, value: [...x.value] })),
    };
    const mismatches = Object.keys(v.expect).filter((k) => JSON.stringify(got[k]) !== JSON.stringify(v.expect[k]));
    const roundTrip = JSON.stringify(decodeBinaryFrame(encodeBinaryFrame(frameFields(f)))) === JSON.stringify(f);
    return { name: v.name, outcome: mismatches.length === 0 && roundTrip ? "pass" : "fail", mismatches, round_trip: roundTrip };
  } catch (err) {
    const name = err && typeof err.code === "number" ? errorName(err.code) : undefined;
    if (v.expect_error) return { name: v.name, expect_error: v.expect_error, got_error: name, outcome: name === v.expect_error ? "pass" : "fail", detail: err.message };
    return { name: v.name, outcome: "fail", detail: String(err) };
  }
});
const source = JSON.parse(readFileSync(join(root, "schemas", "source.json"), "utf8"));
const report = {
  requirement: "AWP-DAT-008",
  implementation: "@hyperduality/awp (TypeScript)",
  spec_revision: SPEC_REVISION,
  vectors_file: `schemas/test-vectors/frames.json (vendored from ${source.ref}, commit ${source.commit})`,
  passed: results.filter((r) => r.outcome === "pass").length,
  failed: results.filter((r) => r.outcome !== "pass").length,
  results,
};
const text = JSON.stringify(report, null, 2) + "\n";
const outIdx = process.argv.indexOf("--out");
if (outIdx >= 0) writeFileSync(process.argv[outIdx + 1], text);
else process.stdout.write(text);
process.exit(report.failed === 0 ? 0 : 1);
