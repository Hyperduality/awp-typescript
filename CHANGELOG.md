# Changelog

## 0.1.0-alpha.3

Targets specification revision `0.1-draft.9`.

- Multi-bind sessions: `openSession` takes `embodiments`, which must share a `multi_bind_group`, and each submission then names `embodimentId` (AWP-EMB-005). The client's bound embodiments are in `embodiments`; `embodiment` is now a getter that is undefined in observer and multi-bind sessions.
- Lockstep: the known tick follows the frames of an advance made by another session. `advance` refuses a second call while one is in flight, and refuses in an observer session. `Manifest.tickAuthority` names the world's tick authority; under `barrier`, `advance` waits for the other bound sessions.
- Every timeout rejects with `TimeoutError`, which `RequestTimeoutError` now extends. This covers `nextFrame`, `waitForFrame`, the per-tick frame waits of `initialObservations`, `advance` and `subscribe`, which rejected with a plain `Error` or `ProtocolError` before.
- `AwpError.errorName` is the JSON-RPC message for reserved codes, without the detail.
- `ChannelTracker.deltaState` is replaced by the count `deltaFrames`, so the tracker no longer keeps every delta frame.
- `replay_complete` is emitted once per resumption.
- New type exports: `LogLevel`, `ManifestCheck`, `ApplyOutcome`, `StateClass`, `Transition`, and `SESSIONLESS_PING_INTERVAL_MS`.
- The package no longer ships source-map references to missing files, or the frame test vectors.
- `awp-demo` reports the package version as its agent version.

## 0.1.0-alpha.2

Targets specification revision `0.1-draft.9`.

- The conformance reports come from awp-conformance 0.1.0a4. The API is unchanged.

## 0.1.0-alpha.1

First release on npm, targeting specification revision `0.1-draft.9`.

- A Core Agent in both time models: the control connection, sessions, the action lifecycle, frames inline and on the `ws` stream binding, heartbeats and clock synchronization, receiver reports, lockstep advances, and resumption with replay.
- `awp-demo`, a scripted agent for awp-sim's arm, with the claim "Core Agent: AWP-conformant against 0.1-draft.9" in each time model.
