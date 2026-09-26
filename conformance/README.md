# Conformance evidence

The reports in this directory come from `awp-conformance` 0.1.0a5, which targets `0.1-draft.9`. It was
run against the demo agent (`node dist/demo.js`) in both time models:

```bash
awp-sim manifest --mode streaming > conformance/manifest-streaming.json
awp-sim manifest --mode lockstep  > conformance/manifest-lockstep.json
awp-conformance agent --manifest conformance/manifest-streaming.json --frames conformance/frames.json \
  --mode streaming --out conformance/report-streaming -- node dist/demo.js --url '{url}' --token '{token}'
awp-conformance agent --manifest conformance/manifest-lockstep.json --frames conformance/frames.json \
  --mode lockstep --out conformance/report-lockstep -- node dist/demo.js --url '{url}' --token '{token}'
```

`frames.json` gives the harness one sample payload per channel.

## Claims (AWP-CNF-005)

- **Core Agent (streaming): AWP-conformant against 0.1-draft.9 (awp-conformance 0.1.0a5)**,
  [`report-streaming/report.json`](report-streaming/report.json)
- **Core Agent (lockstep): AWP-conformant against 0.1-draft.9 (awp-conformance 0.1.0a5)**,
  [`report-lockstep/report.json`](report-lockstep/report.json)

Each claim rests on its report having no failure and nothing untested in scope, plus the evidence below
for every row the report marks `manual`.

| Mode | pass | fail | warn | untested | manual | n/a | untestable |
|---|---|---|---|---|---|---|---|
| streaming | 45 | 0 | 0 | 0 | 2 | 187 | 3 |
| lockstep | 38 | 0 | 0 | 0 | 4 | 192 | 3 |

## Evidence for the `manual` rows

The tests named below are in `test/` and run with `npm test`. Each shows the property its row asks for.

### AWP-DAT-008: frame test vectors (both modes)

- [`dat-008-frame-vectors.json`](dat-008-frame-vectors.json), produced by `node scripts/frame-vectors.mjs`,
  runs this SDK's decoder over `schemas/test-vectors/frames.json`, vendored from the ref in
  `../schemas/source.json`. Result: all 20 vectors pass.
  - Every valid vector decodes to its listed fields and round-trips through the encoder.
  - Every `expect_error` vector is rejected with its listed error. That covers `AWP_MALFORMED` for
    `ext_len_exceeds_frame`, `wrong_registered_len`, `wrong_registered_len_ts_send`, `duplicate_type`,
    `payload_len_mismatch`, `trailing_bytes`, `bad_magic`, `unknown_version` and
    `resync_without_keyframe`, and `AWP_INTEGER_RANGE` for `seq_over_2p53`.
- `test/frames.test.ts` asserts the same thing per vector:
  - "vector <name> decodes to its listed fields" and "… round-trips through the encoder and the inline form";
  - "vector <name> re-encodes byte for byte";
  - "vector <name> is rejected with <error>";
  - "the vendored vector file covers every case AWP-DAT-008 lists".

### AWP-DAT-001: loss by `seq` gap, agent side (lockstep)

In streaming the suite tests this from `obs.report` (the `frame-gaps` episode passes). In lockstep there is
no receiver report, so the tests are the evidence:

- `test/channels.test.ts`, "seq gaps count as loss (AWP-DAT-001)": after seqs 1, 2, 5 the receiver reports
  a gap of 2 and `lost = 2`.
- `test/channels.test.ts`, "channels are independent: interleaving across channels is not loss
  (AWP-OBS-003)": interleaved frames on two channels count no loss.
- `test/client.test.ts`, "frames: gaps, resync, reserved bits, ungranted and unconsumed channels
  (AWP-DAT-001/005/009, AWP-MOD-002)": through the client, a gap on a channel counts as lost frames.
- `test/client.test.ts`, "malformed inline frames are dropped, not processed (AWP-DAT-010)": frames dropped
  as malformed leave a `seq` gap, which the next valid frame reports as loss.

The accounting is per channel and independent of the time model (`src/channels.ts`). Lockstep frames go
through the same `ChannelTracker`.

### AWP-DAT-009: a gap before a resync frame is not loss; delta state is discarded (lockstep)

- `test/channels.test.ts`, "the gap before a resync frame is not loss and delta state is discarded
  (AWP-DAT-009)":
  - after two delta frames, a resync keyframe at seq 40 reports a gap of 36 with `lost = 0` and
    `resyncSkipped = 36`;
  - the count of delta frames since the last keyframe returns to 0.
- `test/client.test.ts`, "lockstep resumption: resync keyframes with the current tick complete an advance
  whose frames were lost (AWP-TIM-009)":
  - in a lockstep session, the resync keyframes sent after `session.resume` skip the lost `seq` values
    without counting loss;
  - they complete the interrupted `world.tick`.
- `test/client.test.ts`, "ws stream binding: bearer session token, resync on move, late inline frames
  discarded, malformed frames close the stream (AWP-TRN-012/013)":
  - the resync keyframe that moves a channel to a stream connection, and the one that follows its
    re-establishment, count no loss;
  - late inline frames are discarded.

### AWP-VER-009: the draft revision is named (both modes)

- `SPEC_REVISION = "0.1-draft.9"` in `src/version.ts`, asserted by `test/schemas.test.ts`, "SPEC_REVISION
  names the targeted draft (AWP-VER-009)".
- The README names the revision, and so does each report (`"specification": "0.1-draft.9"`).
