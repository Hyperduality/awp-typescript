#!/usr/bin/env node
/**
 * Demo agent: opens a session in the manifest's first time model, moves the arm through a few targets
 * with `move_to_pose`, cancels one move while it executes, and closes. It resumes after a dropped
 * connection and exits cleanly when the world falls silent or serves an invalid manifest.
 *
 *   node dist/demo.js --url ws://127.0.0.1:8710 --token <token> [--mode streaming|lockstep] [--pace-ms N] [-v] [--trace]
 *
 * `--pace-ms` is the demo policy's decision time per lockstep advance (default 100 ms): the world is
 * paused between advances, so without it a lockstep run finishes in milliseconds.
 *
 * `$AWP_URL` and `$AWP_TOKEN` are read when the flags are absent.
 *
 * When the world no longer holds the session (AWP_SESSION_UNKNOWN, AWP-SES-008) the demo treats it as
 * closed and continues its remaining targets in a new session, up to three sessions.
 *
 * Exit codes: 0 done; 1 unexpected error; 2 could not connect; 3 invalid manifest or no common
 * protocol version (no session was opened); 4 the session ended early (world silent, window expired).
 */
import { createRequire } from "node:module";
import { parseArgs } from "node:util";
import { AwpClient, type LogLevel } from "./client.ts";
import { AwpError, ConnectionLostError, ErrorCode, ManifestInvalidError, RequestTimeoutError, SessionClosedError, UsageError } from "./errors.ts";
import type { ActionRecord } from "./lifecycle.ts";
import type { Manifest } from "./manifest.ts";
import type { PreemptionPolicy, TimeModel } from "./types.ts";
import { SPEC_REVISION } from "./version.ts";

const CONSUMES = ["proprio/json", "text/event+json"];
const { version } = createRequire(import.meta.url)("../package.json") as { version: string };

interface Args {
  url: string;
  token: string | undefined;
  mode: TimeModel | undefined;
  verbose: boolean;
  trace: boolean;
  paceMs: number;
}

function parse(): Args {
  const { values } = parseArgs({
    options: {
      url: { type: "string" },
      token: { type: "string" },
      mode: { type: "string" },
      verbose: { type: "boolean", short: "v", default: false },
      trace: { type: "boolean", default: false },
      "pace-ms": { type: "string", default: "100" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });
  if (values.help) {
    process.stdout.write("usage: awp-demo --url <ws-url> [--token <token>] [--mode streaming|lockstep] [--pace-ms N] [-v] [--trace]\n");
    process.exit(0);
  }
  const url = values.url ?? process.env.AWP_URL;
  if (!url) {
    process.stderr.write("awp-demo: --url or $AWP_URL is required\n");
    process.exit(2);
  }
  const mode = values.mode as TimeModel | undefined;
  if (mode !== undefined && mode !== "streaming" && mode !== "lockstep") {
    process.stderr.write("awp-demo: --mode must be streaming or lockstep\n");
    process.exit(2);
  }
  const paceMs = Number(values["pace-ms"]);
  if (!Number.isFinite(paceMs) || paceMs < 0) {
    process.stderr.write("awp-demo: --pace-ms must be a non-negative number\n");
    process.exit(2);
  }
  return { url, token: values.token ?? process.env.AWP_TOKEN, mode, verbose: values.verbose, trace: values.trace, paceMs };
}

const started = Date.now();
let verbose = false;
function log(level: LogLevel, msg: string): void {
  if (level === "debug" && !verbose) return;
  process.stderr.write(`[awp-demo +${((Date.now() - started) / 1000).toFixed(3)}s] ${level === "info" ? "" : `${level}: `}${msg}\n`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** Targets inside the embodiment's spatial envelope (or a unit box around the origin when none is declared). */
function targets(manifest: Manifest, embodiment: string): { frame: string; points: number[][] } {
  const env = manifest.raw.safety_policy.envelopes.find((e) => e.embodiment === embodiment && e.spatial);
  const lo = env?.spatial?.aabb_m[0] ?? [-0.5, -0.5, 0];
  const hi = env?.spatial?.aabb_m[1] ?? [0.5, 0.5, 1];
  const frame = env?.spatial?.frame ?? "base";
  const at = (fx: number, fy: number, fz: number) => [
    lo[0]! + (hi[0]! - lo[0]!) * fx,
    lo[1]! + (hi[1]! - lo[1]!) * fy,
    lo[2]! + (hi[2]! - lo[2]!) * fz,
  ].map((v) => Math.round(v * 1000) / 1000);
  return { frame, points: [at(0.7, 0.6, 0.55), at(0.25, 0.7, 0.65), at(0.4, 0.3, 0.45), at(0.5, 0.5, 0.5)] };
}

function choosePreempt(manifest: Manifest, type: string): PreemptionPolicy | undefined {
  const declared = manifest.preemptionPolicies(type);
  if (declared.length === 0) return undefined;
  return declared.includes("replace") ? "replace" : declared[0];
}

/** Where the demo stands across sessions, so a new session continues rather than starting over. */
interface Progress {
  next: number;
  cancelled: boolean;
}

/** The world answered that it no longer holds the session: a new one may be opened (AWP-SES-008). */
const LOST_SESSION = new Set(["AWP_SESSION_UNKNOWN", "AWP_SESSION_EXPIRED"]);
const MAX_SESSIONS = 3;
/** Application deadline for initialize and session.open; the SDK itself detects no loss before session.ready (AWP-SES-012). */
const NEGOTIATION_DEADLINE_MS = 60000;

async function main(): Promise<number> {
  const args = parse();
  verbose = args.verbose;
  log("info", `AWP TypeScript demo agent (spec ${SPEC_REVISION}) → ${args.url}`);
  const progress: Progress = { next: 0, cancelled: false };
  for (let n = 1; ; n++) {
    const outcome = await runOnce(args, progress);
    if (outcome !== "session_lost") return outcome;
    if (n >= MAX_SESSIONS) {
      log("error", `session lost ${n} times; giving up`);
      return 4;
    }
    // AWP-SES-008: the old session is closed and nothing of it survives; continue in a new one.
    log("info", "the world no longer holds the session; opening a new session");
  }
}

async function runOnce(args: Args, progress: Progress): Promise<number | "session_lost"> {
  const client = new AwpClient({
    url: args.url,
    ...(args.token !== undefined ? { token: args.token } : {}),
    agent: { name: "awp-typescript-demo", version, vendor: "hyperduality" },
    consumesModalities: CONSUMES,
    logger: log,
    ...(args.trace ? { trace: (dir: "out" | "in", m: unknown) => log("debug", `${dir === "out" ? "→" : "←"} ${JSON.stringify(m)}`) } : {}),
  });
  client.on("suspended", (reason: string) => log("info", `connection lost (${reason}); reconnecting`));
  client.on("resumed", () => log("info", `resumed; embodiment treated as in safe state until a new action executes`));
  client.on("event", (e: { event: string }) => log("info", `world.event ${e.event}`));

  // 1. Connect and negotiate.
  let manifest: Manifest;
  try {
    await client.connect();
  } catch (err) {
    log("error", `could not connect: ${(err as Error).message}`);
    return 2;
  }
  try {
    manifest = await client.initialize(NEGOTIATION_DEADLINE_MS);
  } catch (err) {
    if (err instanceof ManifestInvalidError) {
      log("error", `refusing to open a session: ${err.message}`);
      for (const p of err.problems.slice(0, 20)) log("error", `  manifest: ${p}`);
      client.disconnect();
      return 3;
    }
    if (err instanceof AwpError && err.code === ErrorCode.AWP_VERSION_UNSUPPORTED) {
      log("error", `no common protocol version: ${err.message}`);
      client.disconnect();
      return 3;
    }
    log("error", `initialize failed: ${(err as Error).message}`);
    client.disconnect();
    return err instanceof RequestTimeoutError || err instanceof ConnectionLostError ? 4 : 1;
  }
  const w = manifest.raw.world;
  log("info", `world ${w.name} ${w.version} (${w.vendor}); time models ${manifest.timeModels.join(", ")}`);

  const mode: TimeModel = args.mode ?? manifest.timeModels[0]!;
  if (!manifest.timeModels.includes(mode)) {
    log("error", `the world does not offer ${mode}`);
    client.disconnect();
    return 3;
  }
  const emb = manifest.embodiments.find((e) => e.action_types.includes("move_to_pose")) ?? manifest.embodiments[0]!;
  const subscribe = emb.channels.filter((c) => {
    const decl = manifest.channel(c);
    return decl !== undefined && CONSUMES.includes(decl.modality) && manifest.raw.observation_channels.some((o) => o.id === c);
  });

  // 2. Open the session.
  try {
    await client.openSession(mode, { embodiment: emb.id, subscribe, timeoutMs: NEGOTIATION_DEADLINE_MS });
  } catch (err) {
    log("error", `session.open failed: ${(err as Error).message}`);
    client.disconnect();
    return err instanceof AwpError ? 1 : 4;
  }
  const ready = client.ready!;
  log("info", `session ${ready.session_id} open (${mode}) on ${emb.id}; granted actions [${ready.granted.action_types.join(", ")}], channels [${ready.granted.channels.map((c) => `${c.channel}#${c.channel_id}`).join(", ")}]`);

  const ended = (): number | "session_lost" => {
    const lost = LOST_SESSION.has(client.closeReason ?? "");
    log(lost ? "info" : "error", `session ended early: ${client.closeReason}`);
    client.disconnect();
    return lost ? "session_lost" : 4;
  };
  try {
    await runSession(client, manifest, emb.id, mode, args.paceMs, progress);
  } catch (err) {
    if (err instanceof SessionClosedError || client.state === "closed") return ended();
    log("error", `unexpected error: ${(err as Error).stack ?? String(err)}`);
    try {
      await client.close();
    } catch {
      client.disconnect();
    }
    return 1;
  }

  // 5. Close.
  if (client.state === "closed") return ended();
  await client.close();
  log("info", "session closed");
  return 0;
}

async function runSession(
  client: AwpClient,
  manifest: Manifest,
  embodiment: string,
  mode: TimeModel,
  paceMs: number,
  progress: Progress,
): Promise<void> {
  const ensureOpen = () => {
    if (client.state === "closed") throw new SessionClosedError(client.closeReason ?? "closed");
  };
  /** One lockstep decision: think for paceMs, then advance (the only way time moves, AWP-AGT-009). */
  const step = async () => {
    if (paceMs > 0) await sleep(paceMs);
    ensureOpen();
    await client.advance(1);
  };

  // 3. Observe before acting.
  if (mode === "lockstep") {
    await client.initialObservations();
    log("info", `initial observations for tick ${client.tick}`);
  } else {
    try {
      await client.waitForFrame(undefined, client.heartbeatIntervalMs * 3);
    } catch {
      log("warn", "no observation yet; continuing");
    }
    if (!(await client.clockReady())) log("warn", "no clock-offset estimate yet");
    const best = client.estimator.best;
    if (best) log("info", `clock offset ${best.offset_ns} ns ± ${Math.round(best.rtt_ns / 2)} ns`);
  }
  ensureOpen();

  const granted = client.ready!.granted.action_types;
  if (!granted.includes("move_to_pose")) {
    // AWP-AGT-003: never submit an ungranted type. Observe briefly, then close.
    log("info", "move_to_pose is not granted in this session; observing only");
    // In lockstep nothing happens until the agent advances, and it has nothing to wait for.
    if (mode === "streaming") await sleep(500);
    return;
  }

  const { frame, points } = targets(manifest, embodiment);
  const decl = manifest.actionSchema("move_to_pose")!;
  const settleMs = (decl.max_duration_ms ?? 10000) + (decl.max_abort_ms ?? 0) + 5000;
  const preempt = choosePreempt(manifest, "move_to_pose");
  const cancelIndex = 1;

  /** The newest observation the move relies on; in streaming, never a stale one (AWP-SAF-010). */
  const basisChannel = [...client.channels.values()].find((c) => c.decl?.modality === "proprio/json")?.grant.channel;
  const basisFrame = async () => {
    if (basisChannel) return mode === "streaming" ? client.freshFrame(basisChannel) : client.latest(basisChannel);
    for (const c of client.channels.values()) if (c.tracker.latest) return c.tracker.latest;
    return undefined;
  };

  /** Submits one move; retries retryable refusals, never retries a non-retryable one identically. */
  const submitMove = async (p: number[]): Promise<ActionRecord | undefined> => {
    for (let attempt = 1; attempt <= 8; attempt++) {
      ensureOpen();
      const basis = await basisFrame();
      try {
        const rec = await client.submit(
          "move_to_pose",
          { pose: { frame, p_m: p, q: [0, 0, 0, 1] } },
          {
            ...(preempt ? { preempt } : {}),
            ...(basis ? { basis } : {}),
            ...(mode === "streaming" ? { validForMs: 1000 } : {}),
          },
        );
        log("info", `submitted ${rec.action_id} → [${p.join(", ")}]: ${rec.state}`);
        return rec;
      } catch (err) {
        if (err instanceof AwpError) {
          if (!err.retryable) {
            log("warn", `move to [${p.join(", ")}] refused (${err.errorName}, not retryable); skipping this target`);
            return undefined;
          }
          const wait = err.retryAfterMs ?? 250;
          log("info", `move refused (${err.errorName}, retryable); retrying in ${wait} ms`);
          if (mode === "lockstep" && err.code === ErrorCode.AWP_ENVELOPE_EXCEEDED) {
            await step(); // lockstep rate limits clear by advancing (AWP-ENV-004)
          } else {
            await sleep(wait);
          }
          continue;
        }
        if (err instanceof UsageError) {
          log("warn", `not submitting: ${err.message}`);
          return undefined;
        }
        throw err;
      }
    }
    log("warn", `giving up on [${p.join(", ")}] after repeated retryable refusals`);
    return undefined;
  };

  /** Waits for a terminal state: advancing in lockstep, waiting in streaming. */
  const finish = async (rec: ActionRecord): Promise<void> => {
    if (mode === "lockstep") {
      for (let i = 0; i < 5000 && !rec.terminal && !rec.lost; i++) await step();
    } else {
      const timeout = sleep(settleMs).then(() => "timeout" as const);
      const r = await Promise.race([rec.settled(), timeout]);
      if (r === "timeout" && !rec.terminal) {
        log("warn", `${rec.action_id} not terminal after ${settleMs} ms; cancelling`);
        await client.cancel(rec.action_id).catch(() => undefined);
        await Promise.race([rec.settled(), sleep(settleMs)]);
      }
    }
    ensureOpen();
    log("info", `${rec.action_id} ${rec.terminal ? rec.state : "unfinished"}${rec.reason ? ` (${rec.reason})` : ""}`);
  };

  for (let i = progress.next; i < points.length; i++) {
    ensureOpen();
    const rec = await submitMove(points[i]!);
    if (!rec) {
      progress.next = i + 1;
      continue;
    }
    if (i === cancelIndex && !progress.cancelled) {
      // Let it start, then cancel while it executes (AWP-LIF-005).
      if (mode === "lockstep") {
        if (!rec.terminal) await step();
      } else {
        await Promise.race([rec.until((r) => r.state === "executing" || r.terminal || r.lost), sleep(settleMs)]);
        await sleep(150);
      }
      ensureOpen();
      if (!rec.terminal) {
        await client.cancel(rec.action_id);
        log("info", `cancel ${rec.action_id}: ${rec.state}`);
      }
      progress.cancelled = true;
    }
    await finish(rec);
    progress.next = i + 1;
  }
}

main().then(
  (code) => {
    process.exitCode = code;
    setTimeout(() => process.exit(code), 200).unref();
  },
  (err) => {
    log("error", `fatal: ${(err as Error).stack ?? String(err)}`);
    process.exit(1);
  },
);
