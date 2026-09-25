# @hyperduality/awp — TypeScript agent SDK for the Agent World Protocol

An independent, clean-room implementation of the **agent side** of the
[Agent World Protocol](https://agentworldprotocol.com) (AWP), written from the specification text and
the canonical JSON Schemas alone — no code or documentation of other AWP implementations was used.

- **Specification revision:** `0.1-draft.9`; wire protocol version `"0.1"`. Both are exported as
  `SPEC_REVISION` and `PROTOCOL_VERSION`. The vendored artifacts come from the ref recorded in
  [`schemas/source.json`](schemas/source.json): the tag `spec-v0.1-draft.9`.
- **Target:** the Core Agent conformance class (AWP-CNF-002) in both time models, streaming and lockstep.
- Node ≥ 20, TypeScript, ESM; `ws` for WebSockets, `ajv` (JSON Schema 2020-12) for validation.

## What is implemented

| Area | Where |
|---|---|
| WebSocket control connection: `awp` subprotocol, `Authorization: Bearer` credential, no credential in URLs, `wss://` required off-loopback | `src/client.ts` |
| JSON-RPC 2.0: no batches sent, batches received answered `-32600`, unknown world requests `-32601`, unknown fields ignored, every outgoing message checked against the **sender form** of its schema (`x-awp-closed`, `x-awp-lint`), every incoming one against the **receiver form** | `src/rpc.ts`, `src/schemas.ts` |
| 64-bit integer bound (2^53 − 1) in JSON (exact, via source text) and in binary frames; a larger value ends the session: `session.close`, then close code 1002 with reason `AWP_INTEGER_RANGE`, never resumed | `src/ints.ts`, `src/client.ts` |
| `initialize` and manifest validation (canonical schema + AWP-MAN-001 cross references + params schemas compiled against the manifest `$defs`); no session is opened against an invalid manifest | `src/manifest.ts` |
| `session.open` / `session.ready`, grants enforced locally (never submit ungranted types, undeclared preemption, or schema-invalid params), `obs.subscribe` / `obs.unsubscribe`, `session.close` awaiting the post-`closed` result | `src/client.ts` |
| Binary frame codec (header, flags, extension TLVs, reserved bits and types) and the inline JSON form; malformed frames dropped, and a stream connection carrying one closed with 1002 `AWP_MALFORMED` and re-established (AWP-DAT-010); all vectors of `schemas/test-vectors/frames.json` | `src/frames.ts`, `src/stream.ts` |
| Per-channel `seq` accounting: loss by gaps, resync (gap not loss, delta state discarded), late frames from another connection discarded, channels independent | `src/channels.ts` |
| Action lifecycle from the vendored normative table, `status_seq` dedup, redelivered terminal statuses tolerated, unique `action_id`s, idempotent identical resubmission, non-retryable refusals never retried identically | `src/lifecycle.ts`, `src/client.ts` |
| Heartbeats (every heartbeat interval and twice per `watchdog_ms` in streaming; at least every 5 s without a session), pongs stamped on one process-wide agent clock, min-RTT-of-eight clock offset, pre-session pongs excluded, loss after three silent intervals only once a session exists, `obs.report` receiver reports | `src/clock.ts`, `src/client.ts` |
| Lockstep: `world.tick` only as an explicit call; an advance completes when the result **and** a frame for its tick on every per-tick channel are held; `AWP_TICK_MISMATCH` resync; a lockstep-only submit-and-advance helper | `src/client.ts` |
| Reconnection: reconnect, `initialize`, `session.resume` with `last_status_seq`, replay processed through `replay_to_status_seq` before any unacknowledged submission is re-sent identically, lockstep resync keyframes awaited, embodiment treated as in safe state until a new action executes; `AWP_SESSION_UNKNOWN` ends the session for good | `src/client.ts` |
| Beyond Core: the `ws` stream binding (resync on move, reconnect while the control connection lives), command frames (bound action, rate limit, latest-wins replacement, never inline while the stream that carried them is lost), `task.update`, `world.snapshot` / `restore` / `reset` | `src/stream.ts`, `src/client.ts` |

Not implemented: stream bindings other than `inline` and `ws`, the approver role, `session.transfer`,
multi-bind sessions, and the `awp.bearer.<token>` subprotocol fallback (Node can set headers).

## Install

```bash
npm install @hyperduality/awp@alpha
```

The package ships the SDK, the `awp-demo` agent, and the vendored schemas it validates against. Alpha
releases are published under the `alpha` dist-tag; each names the draft revision it targets.

## Build and test

```bash
npm install
npm run build          # tsc → dist/
npm run typecheck      # tsc --noEmit over src and test
npm test               # node:test on the compiled tests: unit tests and an in-process scripted world
npm run test:ts        # the same tests from source (Node ≥ 22.6, type stripping)
npm run check-schemas  # vendored files == spec repo at the ref recorded in schemas/source.json
npm run sync-schemas   # re-vendor from ../agent-world-protocol at tag spec-v0.1-draft.9
                       # (or -- --ref <git-ref>, --from <path>)
```

`schemas/` holds the canonical schemas (`schemas/v0.1`), the frame test vectors
(`schemas/test-vectors/frames.json`), and the lifecycle table (`schemas/action-lifecycle.json`,
converted from `spec/action-lifecycle.yaml`), all read from one git ref of the specification repository.
CI (`.github/workflows/ci.yml`) runs the typecheck and tests on Node 20, 22 and 24, the schema check, and
awp-conformance against `awp-demo` in both time models. Pushing a `v*` tag publishes that version to npm
(`.github/workflows/release.yml`).

## Using the SDK

```ts
import { AwpClient } from "@hyperduality/awp";

const client = new AwpClient({
  url: "ws://127.0.0.1:8710",
  token: process.env.AWP_TOKEN,
  agent: { name: "my-agent", version: "0.1.0", vendor: "me" },
  consumesModalities: ["proprio/json"],
});
const manifest = await client.initialize();          // throws ManifestInvalidError on a bad manifest
await client.openSession("streaming", { embodiment: "arm_01", subscribe: ["proprio"] });
const basis = await client.freshFrame("proprio");    // never act on a stale observation
const rec = await client.submit("move_to_pose",
  { pose: { frame: "base", p_m: [0.2, 0.1, 0.4], q: [0, 0, 0, 1] } },
  { preempt: "replace", basis, validForMs: 500 });
await rec.settled();                                  // terminal, refused, or lost with the session
await client.close();
```

In lockstep, advance explicitly: `while (!rec.terminal) await client.advance(1);`.

## Demo agent

```bash
npm run build
node dist/demo.js --url ws://127.0.0.1:8710 --token <token>      # or $AWP_URL / $AWP_TOKEN
```

It opens a session in the manifest's first time model, moves the arm through four targets inside the
declared envelope with `move_to_pose`, cancels the second while it executes, and closes. It reconnects
and resumes if the control connection drops; if the world no longer holds the session
(`AWP_SESSION_UNKNOWN`), it continues its remaining targets in a new session. It never submits on a stale
basis, and exits cleanly when the world falls silent (exit 4) or serves an invalid manifest (exit 3, no
session opened). Flags:
`--mode streaming|lockstep`, `--pace-ms N` (decision time per lockstep advance, default 100),
`-v`, `--trace` (every wire message on stderr).

Run against the reference world:

```bash
awp-sim serve --token t                 # streaming on ws://127.0.0.1:8710
awp-sim serve --mode lockstep --token t
node dist/demo.js --url ws://127.0.0.1:8710 --token t
```

`node scripts/fetch-manifest.mjs --url <ws-url> --token <t> --out manifest.json` fetches a world's
manifest over the protocol (the `initialize` result).

## Conformance

See [`conformance/README.md`](conformance/README.md) for the reports of `awp-conformance` in both time
models, the claim each supports (AWP-CNF-005), and the evidence for every `manual` row.

## License

Apache-2.0.
