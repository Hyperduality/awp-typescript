/**
 * AwpClient: the agent side of one AWP session.
 *
 * Connection → `initialize` (manifest validated, AWP-AGT-002) → `session.open` → heartbeats and clock
 * synchronization → submissions, cancellations, subscriptions, lockstep advances → `session.close`.
 * A dropped control connection is resumed with `session.resume` and `last_status_seq` (AWP-CTL-005,
 * AWP-AGT-007); the embodiment is treated as in safe state until an action admitted after the
 * resumption executes.
 */
import { EventEmitter } from "node:events";
import { randomBytes } from "node:crypto";
import WebSocket from "ws";
import { ChannelTracker, latencyStats, type ReceivedFrame } from "./channels.ts";
import { OffsetEstimator, agentClock, clockSample, type Clock, type ClockSample } from "./clock.ts";
import {
  AwpError,
  ConnectionLostError,
  ErrorCode,
  JsonRpcCode,
  ManifestInvalidError,
  ProtocolError,
  SessionClosedError,
  UsageError,
} from "./errors.ts";
import { decodeInlineFrame, encodeBinaryFrame, encodeInlineFrame, type Frame, type FrameFields } from "./frames.ts";
import { ActionRecord, type StatusUpdate } from "./lifecycle.ts";
import { Manifest, checkManifest } from "./manifest.ts";
import { RpcConnection, type IncomingNotification, type IncomingRequest } from "./rpc.ts";
import { PROTOCOL_ERROR_CLOSE, StreamConnection } from "./stream.ts";
import { INCOMING_PARAMS_SCHEMA, RESULT_SCHEMA, validate } from "./schemas.ts";
import type {
  ActionCancelResult,
  ActionStatus,
  ActionSubmitParams,
  ActionSubmitResult,
  AdminOperation,
  AgentInfo,
  AgentManifest,
  ChannelDecl,
  ChannelGrant,
  ObsReport,
  PingParams,
  PongResult,
  PreemptionPolicy,
  SessionOpenParams,
  SessionReady,
  SessionStateNotification,
  SessionTelemetry,
  SubscribeRequest,
  TimeModel,
  WorldEvent,
  WorldManifest,
} from "./types.ts";
import { AWP_SUBPROTOCOL, SUPPORTED_PROTOCOL_VERSIONS } from "./version.ts";

/** A sessionless connection pings at least this often (AWP-SES-012); it detects no loss by silence. */
export const SESSIONLESS_PING_INTERVAL_MS = 5000;

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface ClientOptions {
  /** World endpoint, e.g. ws://127.0.0.1:8710. Never carries a credential (AWP-SEC-006). */
  url: string;
  /** Agent bearer credential, presented as `Authorization: Bearer …` (AWP-SEC-005). */
  token?: string;
  agent: AgentInfo;
  /** Modalities this agent decodes (AWP-AGM-001); channels of other modalities are ignored (AWP-MOD-002). */
  consumesModalities: string[];
  timeModels?: TimeModel[];
  maxObsRateHz?: number;
  clock?: Clock;
  /**
   * `auto` (default): the SDK sends `ping` on a timer. `manual`: the application calls `heartbeat()`
   * from the loop that produces intents, so a stalled policy stops the heartbeat (AWP-SAF-005).
   */
  heartbeat?: "auto" | "manual";
  /** Reconnect and resume after a dropped control connection (default true). */
  reconnect?: boolean;
  /** Send `obs.report` receiver reports in streaming sessions (default true, AWP-OBS-007). */
  obsReport?: boolean;
  obsReportIntervalMs?: number;
  /** Validate outgoing messages against the sender form of the canonical schemas (default true). */
  validateOutgoing?: boolean;
  /**
   * Stream bindings this agent may use besides inline, in preference order (default `["ws"]`). The
   * agent connects to the first endpoint in `stream_endpoints` whose binding it supports; `[]` keeps
   * every channel inline.
   */
  streamBindings?: "ws"[];
  /** Allow a plaintext ws:// URL to a non-loopback host (AWP-SEC-001 forbids it; default false). */
  allowInsecure?: boolean;
  logger?: (level: LogLevel, message: string) => void;
  trace?: (direction: "out" | "in", message: unknown) => void;
}

export type ClientState =
  | "idle"
  | "connecting"
  | "negotiating"
  | "ready"
  | "active"
  | "suspended"
  | "closing"
  | "closed";

export interface GrantedChannel {
  grant: ChannelGrant;
  decl: ChannelDecl | undefined;
  tracker: ChannelTracker;
  /** Per-tick lockstep channel (manifest `rate_hz: null`). */
  perTick: boolean;
  /** This agent decodes the channel's modality. */
  consumed: boolean;
}

export interface SubmitOptions {
  actionId?: string;
  embodimentId?: string;
  preempt?: PreemptionPolicy;
  deadlineMs?: number;
  /** The frame the intent relies on; its `ts_mono_ns` is copied unchanged into `basis_ts_mono_ns` (AWP-ACT-007). */
  basis?: ReceivedFrame;
  /** Streaming only: validity window; `valid_until_ns` is expressed through the clock offset (AWP-CLK-009). */
  validForMs?: number;
}

export interface OpenOptions {
  embodiment?: string;
  subscribe?: (string | SubscribeRequest)[];
  actionTypes?: string[];
  admin?: AdminOperation[];
  seed?: number;
  task?: SessionOpenParams["task"];
  /** Bound the wait for session.ready (default: none — no loss by silence before it, AWP-SES-012). */
  timeoutMs?: number;
}

interface PingRecord {
  origin: number;
  epoch: number;
  connection: number;
}

function isLoopback(host: string): boolean {
  const h = host.replace(/^\[|\]$/g, "");
  return h === "localhost" || h === "::1" || /^127\./.test(h);
}

function canonical(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  if (v !== null && typeof v === "object") {
    const keys = Object.keys(v as object).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${canonical((v as Record<string, unknown>)[k])}`).join(",")}}`;
  }
  return JSON.stringify(v);
}

/** Per-sender-unique request ids are handled by RpcConnection; action ids are generated here (AWP-AGT-004). */
export function newActionId(): string {
  return `a-${randomBytes(8).toString("hex")}`;
}

export class AwpClient extends EventEmitter {
  readonly options: ClientOptions;
  readonly clock: Clock;
  readonly estimator = new OffsetEstimator();

  state: ClientState = "idle";
  manifest: Manifest | undefined;
  ready: SessionReady | undefined;
  mode: TimeModel | undefined;
  embodiment: string | undefined;

  /** Current lockstep tick as the agent knows it. */
  tick: number | undefined;
  /** Highest status_seq processed with every lower one also processed (sent as `last_status_seq`). */
  lastStatusSeq = 0;
  /** `replay_to_status_seq` of the latest resumption. */
  replayTo: number | undefined;
  /** The world reported safe state (safe_state_entered / resume result). */
  worldSafeState = false;
  /** AWP-AGT-007: after a resumption, the embodiment is treated as in safe state until a new action executes. */
  assumedSafeState = false;
  eStopEngaged = false;
  telemetry: SessionTelemetry | undefined;
  /** Resumptions completed in this session. */
  resumes = 0;
  /** Why the session ended, once closed. */
  closeReason: string | undefined;

  readonly records = new Map<string, ActionRecord>();
  readonly channels = new Map<number, GrantedChannel>();

  private conn: RpcConnection | undefined;
  private connectionCounter = 0;
  private sessionEpoch = 0;
  private processedAbove = new Set<number>();
  private admittedIds = new Set<string>();
  private refused = new Map<string, AwpError>();
  private pings = new Map<number, PingRecord>();
  private heartbeatTimer: NodeJS.Timeout | undefined;
  private livenessTimer: NodeJS.Timeout | undefined;
  private reportTimer: NodeJS.Timeout | undefined;
  private lastReportAt = 0;
  private decisionLatencies: number[] = [];
  private tickWaiters: { tick: number; channels: Set<number>; resolve: () => void }[] = [];
  private ticksSeen = new Map<number, Set<number>>();
  private resumeWaiters: { resolve: () => void; reject: (e: Error) => void }[] = [];
  private closedWaiters: (() => void)[] = [];
  private submittedAfterResume = new Set<string>();
  private closingIntentionally = false;
  private reconnecting = false;
  private lostAt = 0;
  private lastPingSentAt = 0;
  private originalManifest: string | undefined;
  private streams: StreamConnection[] = [];
  /**
   * Endpoints whose established stream connection was lost while the control connection remained: their
   * channels are delivered in neither direction, not inline, until re-established (AWP-TRN-010).
   */
  private lostStreams: StreamConnection["endpoint"][] = [];
  /** Id of the control connection that carries the session. */
  private sessionConnection: number | undefined;
  /** Lockstep: resolves once the resync keyframes after a resumption arrived (AWP-TIM-009). */
  private resumeObservation: Promise<void> | undefined;

  constructor(options: ClientOptions) {
    super();
    this.options = options;
    this.clock = options.clock ?? agentClock;
    const u = new URL(options.url);
    if (u.protocol !== "ws:" && u.protocol !== "wss:") throw new UsageError(`unsupported URL scheme ${u.protocol}`);
    if (u.protocol === "ws:" && !isLoopback(u.hostname) && !options.allowInsecure) {
      throw new UsageError("a connection that leaves the machine must use wss:// (AWP-SEC-001)");
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Logging and events

  private log(level: LogLevel, message: string): void {
    this.options.logger?.(level, message);
  }

  private warn(message: string): void {
    this.log("warn", message);
    this.emit("warning", message);
  }

  /** The negotiated heartbeat interval; before any session, the sessionless ping interval. */
  get heartbeatIntervalMs(): number {
    return this.ready?.heartbeat_interval_ms ?? SESSIONLESS_PING_INTERVAL_MS;
  }

  get reconnectWindowMs(): number {
    return this.ready?.reconnect_window_ms ?? 0;
  }

  /** Whether the current control connection carries the session (after session.ready or the resume result). */
  private get connectionHasSession(): boolean {
    return this.conn !== undefined && this.conn.connectionId === this.sessionConnection && this.state !== "closed";
  }

  /** Whether the agent must regard the embodiment as in safe state (AWP-AGT-007, AWP-SAF-008). */
  get inSafeState(): boolean {
    return this.assumedSafeState || this.worldSafeState;
  }

  get replayComplete(): boolean {
    return this.replayTo === undefined || this.lastStatusSeq >= this.replayTo;
  }

  get sessionOpen(): boolean {
    return this.ready !== undefined && this.state !== "closed";
  }

  /** Granted channel by name. */
  channel(name: string): GrantedChannel | undefined {
    for (const c of this.channels.values()) if (c.grant.channel === name) return c;
    return undefined;
  }

  /** Latest accepted frame on a channel, by name. */
  latest(name: string): ReceivedFrame | undefined {
    return this.channel(name)?.tracker.latest;
  }

  /**
   * Staleness of a frame now: session-clock now minus its capture time (streaming, through the clock
   * offset). Undefined without an offset estimate or in lockstep, where the session clock is simulated.
   */
  stalenessNs(rf: ReceivedFrame): number | undefined {
    if (this.mode !== "streaming") return undefined;
    const now = this.sessionNow();
    return now === undefined ? undefined : now - rf.frame.ts_mono_ns;
  }

  /** The channel's `stale_after_ms`, defaulting to twice its period (AWP-SAF-009). */
  staleAfterMs(name: string): number | undefined {
    const decl = this.channel(name)?.decl;
    if (!decl) return undefined;
    if (decl.stale_after_ms !== undefined) return decl.stale_after_ms;
    const rate = this.channel(name)?.grant.rate_hz ?? decl.rate_hz;
    return rate ? (2 * 1000) / rate : undefined;
  }

  /**
   * A basis frame that is not stale (AWP-SAF-010: agents SHOULD NOT submit on a stale basis): the
   * latest frame on the channel if fresh, else the next one to arrive within `timeoutMs`.
   */
  async freshFrame(name: string, timeoutMs = 1000): Promise<ReceivedFrame | undefined> {
    const limit = this.staleAfterMs(name);
    const fresh = (rf: ReceivedFrame | undefined) => {
      if (!rf) return false;
      const st = this.stalenessNs(rf);
      return limit === undefined || st === undefined || st <= limit * 1e6;
    };
    const cur = this.latest(name);
    if (fresh(cur)) return cur;
    const until = Date.now() + timeoutMs;
    while (Date.now() < until && this.state !== "closed") {
      try {
        const rf = await this.nextFrame(name, until - Date.now());
        if (fresh(rf)) return rf;
      } catch {
        break;
      }
    }
    return this.latest(name);
  }

  /** The next frame to arrive on a channel. */
  nextFrame(name: string, timeoutMs: number): Promise<ReceivedFrame> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.off("frame", onFrame);
        reject(new Error(`no frame on ${name} within ${timeoutMs} ms`));
      }, Math.max(1, timeoutMs));
      const onFrame = (n: string, _f: Frame, rf: ReceivedFrame) => {
        if (n !== name) return;
        clearTimeout(timer);
        this.off("frame", onFrame);
        resolve(rf);
      };
      this.on("frame", onFrame);
    });
  }

  // ---------------------------------------------------------------------------------------------
  // Connection

  private openSocket(timeoutMs: number): Promise<WebSocket> {
    return new Promise((resolve, reject) => {
      const headers: Record<string, string> = {};
      if (this.options.token) headers.Authorization = `Bearer ${this.options.token}`;
      const ws = new WebSocket(this.options.url, [AWP_SUBPROTOCOL], {
        headers,
        handshakeTimeout: timeoutMs,
        perMessageDeflate: false,
      });
      let settled = false;
      const fail = (err: Error) => {
        if (settled) return;
        settled = true;
        try {
          ws.terminate();
        } catch {
          /* ignore */
        }
        reject(err);
      };
      ws.once("open", () => {
        if (settled) return;
        settled = true;
        if (ws.protocol !== AWP_SUBPROTOCOL) this.warn(`world selected subprotocol ${JSON.stringify(ws.protocol)}, expected "awp"`);
        resolve(ws);
      });
      ws.once("unexpected-response", (_req, res) => fail(new ConnectionLostError(`HTTP ${res.statusCode} on WebSocket upgrade`)));
      ws.once("error", (err) => fail(new ConnectionLostError(`connect failed: ${err.message}`)));
      ws.once("close", () => fail(new ConnectionLostError("closed during handshake")));
    });
  }

  private attach(ws: WebSocket): RpcConnection {
    const id = ++this.connectionCounter;
    const conn = new RpcConnection(
      ws,
      id,
      this.clock,
      {
        request: (req) => this.onRequest(req),
        notification: (n) => this.onNotification(n, id),
        integerRange: (detail) => this.onIntegerRange(detail),
        protocolWarning: (d) => this.warn(d),
        binary: () => this.warn("binary message on the control connection ignored"),
      },
      {
        validateOutgoing: this.options.validateOutgoing ?? true,
        ...(this.options.trace ? { trace: this.options.trace } : {}),
      },
    );
    ws.on("close", (code, reason) => {
      if (this.conn === conn) this.onConnectionLost(`connection closed (${code}${reason.length ? ` ${reason.toString()}` : ""})`);
    });
    this.conn = conn;
    return conn;
  }

  /** Opens the control connection and starts pre-session heartbeats (AWP-SES-012). */
  async connect(): Promise<void> {
    if (this.conn && !this.conn.closed) return;
    this.state = "connecting";
    const ws = await this.openSocket(this.heartbeatIntervalMs * 2);
    this.attach(ws);
    this.startTimers();
  }

  private requireConn(): RpcConnection {
    if (!this.conn || this.conn.closed) throw new ConnectionLostError("not connected");
    return this.conn;
  }

  private async call<T>(
    method: string,
    params: unknown,
    timeoutMs?: number,
    onResult?: (result: T, info: { receivedAt: number; sentAt: number }) => void,
  ): Promise<{ result: T; receivedAt: number; sentAt: number }> {
    const conn = this.requireConn();
    const schema = RESULT_SCHEMA[method];
    const { result, info } = await conn.request(method, params, timeoutMs, (r, i) => {
      if (schema && method !== "initialize" && method !== "world.manifest") {
        const problems = validate(schema, r, "receiver");
        if (problems.length > 0) this.warn(`${method} result does not match ${schema}: ${problems.join("; ")}`);
      }
      onResult?.(r as T, i);
    });
    return { result: result as T, receivedAt: info.receivedAt, sentAt: info.sentAt };
  }

  // ---------------------------------------------------------------------------------------------
  // initialize

  agentManifest(): AgentManifest {
    const m: AgentManifest = {
      protocol_versions: [...SUPPORTED_PROTOCOL_VERSIONS],
      agent: this.options.agent,
      consumes_modalities: this.options.consumesModalities,
    };
    if (this.options.timeModels) m.time_models = this.options.timeModels;
    if (this.options.maxObsRateHz !== undefined) m.max_obs_rate_hz = this.options.maxObsRateHz;
    return m;
  }

  /**
   * Sends `initialize` and validates the manifest. Throws ManifestInvalidError when the manifest does
   * not validate; the client then refuses `openSession` (AWP-AGT-002).
   */
  async initialize(timeoutMs?: number): Promise<Manifest> {
    if (!this.conn) await this.connect();
    this.state = "negotiating";
    this.manifest = undefined;
    // No silence-based loss detection before session.ready (AWP-SES-012): wait unless the caller bounds it.
    const { result } = await this.call<WorldManifest>("initialize", this.agentManifest(), timeoutMs);
    const check = checkManifest(result, SUPPORTED_PROTOCOL_VERSIONS);
    if (!check.valid) throw new ManifestInvalidError(check.problems);
    this.manifest = new Manifest(result);
    return this.manifest;
  }

  /** Re-fetches the manifest (`world.manifest`); stable per connection (AWP-MAN-003). */
  async fetchManifest(): Promise<Manifest> {
    const { result } = await this.call<WorldManifest>("world.manifest", {});
    const check = checkManifest(result, SUPPORTED_PROTOCOL_VERSIONS);
    if (!check.valid) throw new ManifestInvalidError(check.problems);
    return new Manifest(result);
  }

  // ---------------------------------------------------------------------------------------------
  // session.open / session.ready

  /** Opens a session. The time model must be one the manifest offers (AWP-TIM-001, AWP-NEG-002). */
  async openSession(mode: TimeModel, opts: OpenOptions = {}): Promise<SessionReady> {
    const manifest = this.manifest;
    if (!manifest) throw new UsageError("initialize must succeed with a valid manifest before session.open (AWP-AGT-002)");
    if (this.ready && this.state !== "closed") throw new UsageError("this connection already carries a session (AWP-CTL-007)");
    if (!manifest.timeModels.includes(mode)) throw new UsageError(`the world does not offer ${mode} (offers ${manifest.timeModels.join(", ")})`);
    const params: SessionOpenParams = { mode };
    if (opts.embodiment !== undefined) {
      const e = manifest.embodiment(opts.embodiment);
      if (!e) throw new UsageError(`embodiment ${opts.embodiment} is not declared`);
      params.embodiment = opts.embodiment;
    }
    if (opts.subscribe) {
      params.subscribe = opts.subscribe.map((s) => (typeof s === "string" ? { channel: s } : s));
      for (const s of params.subscribe) {
        const decl = manifest.channel(s.channel);
        if (!decl) throw new UsageError(`channel ${s.channel} is not declared in the manifest`);
        if (!this.options.consumesModalities.includes(decl.modality)) {
          throw new UsageError(`channel ${s.channel} carries ${decl.modality}, which this agent does not consume (AWP-MOD-002)`);
        }
      }
    }
    if (opts.actionTypes) params.action_types = opts.actionTypes;
    if (opts.admin) params.admin = opts.admin;
    if (opts.seed !== undefined) params.seed = opts.seed;
    if (opts.task) {
      if (manifest.raw.capabilities?.task !== true) throw new UsageError("task requires the task capability (AWP-TSK-001)");
      params.task = opts.task;
    }
    if (opts.seed !== undefined && manifest.raw.capabilities?.seed !== true) throw new UsageError("seed requires the seed capability");
    const { result } = await this.call<SessionReady>("session.open", params, opts.timeoutMs, (r) => {
      this.mode = mode;
      this.embodiment = opts.embodiment;
      this.originalManifest = canonical(manifest.raw);
      this.lastStatusSeq = 0;
      this.processedAbove.clear();
      this.sessionConnection = this.conn?.connectionId;
      this.applyReady(r, false);
      this.state = "ready";
    });
    if (mode === "streaming") this.syncBurst();
    return result;
  }

  private applyReady(ready: SessionReady, resumed: boolean): void {
    const problems = validate("session-ready", ready, "receiver");
    if (problems.length > 0) this.warn(`session.ready does not match its schema: ${problems.join("; ")}`);
    this.ready = ready;
    this.sessionEpoch++;
    if (ready.tick !== undefined) this.tick = ready.tick;
    this.rebuildChannels(ready.granted.channels, resumed);
    if (!ready.stream_endpoints.some((e) => e.binding === "inline")) {
      this.warn("session.ready offers no inline endpoint (AWP-TRN-004); frames are expected inline");
    }
    this.restartTimers();
    this.connectStreams();
  }

  // ---------------------------------------------------------------------------------------------
  // Stream connections

  /**
   * Connects to the first offered endpoint whose binding this agent supports (stream negotiation);
   * channels stay inline until it is up (AWP-TRN-012). Re-run from the re-issued endpoints after
   * every resumption (AWP-TRN-008).
   */
  private connectStreams(): void {
    this.closeStreams();
    // After session.open or session.resume every channel is inline until a stream is established (AWP-TRN-008).
    this.lostStreams = [];
    const supported = this.options.streamBindings ?? ["ws"];
    const ready = this.ready;
    if (!ready) return;
    for (const ep of ready.stream_endpoints) {
      if (ep.binding === "inline") return;
      if ((supported as string[]).includes(ep.binding)) {
        void this.openStream(ep, 0);
        return;
      }
    }
  }

  private async openStream(ep: StreamConnection["endpoint"], attempt: number): Promise<void> {
    const ready = this.ready;
    if (!ready || this.state === "closed" || this.state === "closing" || this.state === "suspended") return;
    const stream = new StreamConnection(ep, ++this.connectionCounter, this.clock, {
      frame: (f, at, id) => this.processFrame(f, at, id),
      closed: (s, code, reason, byUs) => this.onStreamClosed(s, code, reason, byUs, attempt),
      integerRange: (d) => this.onIntegerRange(d),
      warning: (d) => this.warn(d),
    });
    this.streams.push(stream);
    try {
      await stream.open(ready.session_token, this.heartbeatIntervalMs * 2);
      this.lostStreams = this.lostStreams.filter((e) => e !== ep);
      this.log("debug", `stream connection up: ${ep.binding} ${ep.url} (channels ${ep.channels?.join(",") ?? "all"})`);
      this.emit("stream_open", ep);
    } catch (err) {
      this.warn(`stream connection to ${ep.url} failed: ${(err as Error).message}; channels stay inline`);
    }
  }

  private onStreamClosed(s: StreamConnection, code: number, reason: string, byUs: boolean, attempt: number): void {
    this.streams = this.streams.filter((x) => x !== s);
    if (byUs || this.state === "closed" || this.state === "closing" || this.state === "suspended") return;
    if (!this.conn || this.conn.closed) return;
    // AWP-TRN-010: the agent MAY reconnect to the same endpoint while the control connection remains;
    // reliable channels restart with a resync keyframe and the outage arrives as channel_degraded.
    if (s.established && !this.lostStreams.includes(s.endpoint)) this.lostStreams.push(s.endpoint);
    this.emit("stream_lost", s.endpoint, code, reason);
    if (!s.established && attempt >= 3) {
      this.warn(`stream connection to ${s.endpoint.url} keeps failing; its channels are not delivered`);
      return;
    }
    const delay = Math.min(2000, 100 * 2 ** attempt);
    const t = setTimeout(() => void this.openStream(s.endpoint, s.established ? 0 : attempt + 1), delay);
    t.unref();
  }

  private closeStreams(): void {
    const streams = this.streams;
    this.streams = [];
    for (const s of streams) s.close(1000, "");
  }

  private rebuildChannels(grants: ChannelGrant[], keepTrackers: boolean): void {
    const old = new Map([...this.channels.values()].map((c) => [c.grant.channel, c]));
    this.channels.clear();
    for (const g of grants) {
      const decl = this.manifest?.channel(g.channel);
      const prev = old.get(g.channel);
      // A channel keeps its receive state across resumption and re-rating when its channel_id is unchanged.
      const tracker =
        keepTrackers && prev && prev.grant.channel_id === g.channel_id
          ? prev.tracker
          : new ChannelTracker(g.channel_id, decl?.loss_class ?? "reliable");
      this.channels.set(g.channel_id, {
        grant: g,
        decl,
        tracker,
        perTick: decl?.rate_hz === null || g.rate_hz === null,
        consumed: decl ? this.options.consumesModalities.includes(decl.modality) : false,
      });
    }
  }

  /**
   * Lockstep: resolves once a frame carrying the current tick arrived on every subscribed per-tick
   * channel (AWP-TIM-009) — the agent observes before it acts.
   */
  async initialObservations(timeoutMs = this.heartbeatIntervalMs * 3): Promise<void> {
    if (this.mode !== "lockstep" || this.tick === undefined) return;
    await this.waitForTickFrames(this.tick, timeoutMs);
  }

  /** Streaming: resolves once a frame has arrived on the named channel (or any channel). */
  waitForFrame(channel?: string, timeoutMs = this.heartbeatIntervalMs * 3): Promise<ReceivedFrame> {
    const have = channel ? this.latest(channel) : [...this.channels.values()].find((c) => c.tracker.latest)?.tracker.latest;
    if (have) return Promise.resolve(have);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.off("frame", onFrame);
        reject(new Error(`no frame${channel ? ` on ${channel}` : ""} within ${timeoutMs} ms`));
      }, timeoutMs);
      const onFrame = (name: string, _f: Frame, rf: ReceivedFrame) => {
        if (channel && name !== channel) return;
        clearTimeout(timer);
        this.off("frame", onFrame);
        resolve(rf);
      };
      this.on("frame", onFrame);
    });
  }

  // ---------------------------------------------------------------------------------------------
  // Heartbeats, clock synchronization, liveness, receiver reports

  /**
   * Ping period: at least every heartbeat interval (AWP-SAF-001) and twice per watchdog_ms in streaming
   * (AWP-SAF-005); on a connection without a session, at least every 5 s (AWP-SES-012).
   */
  get pingPeriodMs(): number {
    let period = this.heartbeatIntervalMs;
    if (!this.connectionHasSession) period = Math.min(period, SESSIONLESS_PING_INTERVAL_MS);
    const wd = this.manifest?.watchdogMs;
    if (this.mode === "streaming" && wd !== undefined) period = Math.min(period, wd / 2);
    return Math.max(50, Math.floor(period * 0.8));
  }

  private startTimers(): void {
    this.stopTimers();
    if ((this.options.heartbeat ?? "auto") === "auto") {
      this.heartbeatTimer = setInterval(() => {
        this.heartbeat().catch(() => undefined);
      }, this.pingPeriodMs);
      this.heartbeatTimer.unref();
    }
    this.livenessTimer = setInterval(() => this.checkLiveness(), Math.max(50, Math.min(250, this.heartbeatIntervalMs / 4)));
    this.livenessTimer.unref();
    if (this.mode === "streaming" && this.ready && (this.options.obsReport ?? true)) {
      this.lastReportAt = this.clock.now();
      const every = Math.min(this.options.obsReportIntervalMs ?? 1000, 5000);
      this.reportTimer = setInterval(() => this.sendReport(), every);
      this.reportTimer.unref();
    }
  }

  private restartTimers(): void {
    if (this.conn && !this.conn.closed) this.startTimers();
  }

  private stopTimers(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.livenessTimer) clearInterval(this.livenessTimer);
    if (this.reportTimer) clearInterval(this.reportTimer);
    this.heartbeatTimer = this.livenessTimer = this.reportTimer = undefined;
  }

  /**
   * Sends one `ping` carrying the agent clock and, in a session, `last_status_seq` (AWP-CTL-010).
   * Resolves with the clock sample; pre-session exchanges never enter the estimate (AWP-SES-012).
   */
  async heartbeat(): Promise<ClockSample | undefined> {
    const conn = this.conn;
    if (!conn || conn.closed || this.state === "closed") return undefined;
    const origin = this.clock.now();
    const params: PingParams = { origin_ns: origin };
    const inSession = this.ready !== undefined && (this.state === "ready" || this.state === "active" || this.state === "closing");
    if (inSession) params.last_status_seq = this.lastStatusSeq;
    const rec: PingRecord = { origin, epoch: inSession ? this.sessionEpoch : -1, connection: conn.connectionId };
    this.lastPingSentAt = origin;
    const { result, receivedAt } = await this.call<PongResult>("ping", params);
    if (result.origin_ns !== origin) this.warn(`pong echoed origin_ns ${result.origin_ns}, sent ${origin}`);
    const sample = clockSample(origin, result.receive_ns, result.transmit_ns, receivedAt);
    const sameSession = rec.epoch === this.sessionEpoch && rec.epoch >= 0 && rec.connection === this.conn?.connectionId;
    // Lockstep: the session clock is simulated time; no offset relates it to the agent clock (AWP-TIM-013).
    if (sameSession && this.mode === "streaming") {
      this.estimator.add(sample);
      this.emit("clock", sample);
    }
    return sample;
  }

  /** CLK-008: SHOULD complete four exchanges within the first second after session.ready. */
  private syncBurst(): void {
    for (let i = 0; i < 4; i++) {
      const t = setTimeout(() => this.heartbeat().catch(() => undefined), i * 150);
      t.unref();
    }
  }

  /** Resolves once a clock-offset estimate exists (required before any AWP-CLK-009 value). */
  async clockReady(timeoutMs = 2000): Promise<boolean> {
    const until = this.clock.now() + timeoutMs * 1e6;
    while (this.estimator.offsetNs === undefined) {
      if (this.clock.now() > until || !this.conn || this.conn.closed) return false;
      await this.heartbeat().catch(() => undefined);
      if (this.estimator.offsetNs === undefined) await new Promise((r) => setTimeout(r, 50));
    }
    return true;
  }

  /** Session-clock "now" estimated through the offset (streaming only). */
  sessionNow(): number | undefined {
    const o = this.estimator.offsetNs;
    return o === undefined ? undefined : this.clock.now() + o;
  }

  private checkLiveness(): void {
    const conn = this.conn;
    if (!conn || conn.closed) return;
    // Neither side detects loss by silence before session.ready (AWP-SES-012).
    if (!this.connectionHasSession) return;
    const silentMs = (this.clock.now() - conn.lastReceivedAt) / 1e6;
    // AWP-SAF-002: three consecutive intervals without a pong or any other message → lost.
    if (silentMs > 3 * this.heartbeatIntervalMs) {
      this.warn(`no message from the world for ${Math.round(silentMs)} ms; treating the control connection as lost (AWP-SAF-002)`);
      conn.terminate();
      this.onConnectionLost("heartbeat loss");
    }
  }

  private sendReport(): void {
    const conn = this.conn;
    const best = this.estimator.best;
    if (!conn || conn.closed || !best || this.mode !== "streaming" || !this.ready) return;
    if (this.state !== "active" && this.state !== "ready") return;
    const now = this.clock.now();
    const report: ObsReport = {
      window_ms: Math.max(1, Math.round((now - this.lastReportAt) / 1e6)),
      sync: { offset_ns: best.offset_ns, rtt_ns: Math.max(0, best.rtt_ns), samples: this.estimator.samples },
      channels: {},
    };
    for (const c of this.channels.values()) {
      const w = c.tracker.takeWindow();
      if (w) report.channels[String(c.grant.channel_id)] = w;
    }
    const dl = latencyStats(this.decisionLatencies);
    if (dl) report.decision_latency_ns = dl;
    this.decisionLatencies = [];
    this.lastReportAt = now;
    try {
      conn.notify("obs.report", report);
    } catch (err) {
      this.log("debug", `obs.report not sent: ${(err as Error).message}`);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Incoming world → agent traffic

  private onRequest(req: IncomingRequest): { result: unknown } | undefined {
    if (req.method === "ping") {
      const p = req.params as Partial<PingParams> | undefined;
      if (!p || typeof p.origin_ns !== "number" || !Number.isInteger(p.origin_ns)) {
        throw new AwpError({ code: JsonRpcCode.INVALID_PARAMS, message: "Invalid params", data: { retryable: false, detail: "ping requires origin_ns" } });
      }
      // AWP-CLK-007: stamped on the agent clock, as close to the transport as the runtime permits.
      const pong: PongResult = { origin_ns: p.origin_ns, receive_ns: req.receivedAt, transmit_ns: this.clock.now() };
      return { result: pong };
    }
    // Any other world → agent request is unknown to a Core agent: -32601 (AWP-CTL-002).
    return undefined;
  }

  private checkIncoming(method: string, params: unknown): boolean {
    const schema = INCOMING_PARAMS_SCHEMA[method];
    if (!schema) return true;
    const problems = validate(schema, params, "receiver");
    if (problems.length > 0) {
      this.warn(`${method} does not match ${schema}: ${problems.join("; ")}`);
      return false;
    }
    return true;
  }

  private onNotification(n: IncomingNotification, connection: number): void {
    switch (n.method) {
      case "obs.frame":
        this.onFrame(n.params, n.receivedAt, connection);
        return;
      case "action.status":
        if (this.checkIncoming(n.method, n.params)) this.onActionStatus(n.params as ActionStatus);
        return;
      case "world.event":
        if (this.checkIncoming(n.method, n.params)) this.onWorldEvent(n.params as WorldEvent);
        return;
      case "session.state":
        if (this.checkIncoming(n.method, n.params)) this.onSessionState(n.params as SessionStateNotification);
        return;
      case "session.telemetry":
        if (this.checkIncoming(n.method, n.params)) {
          this.telemetry = n.params as SessionTelemetry;
          this.emit("telemetry", this.telemetry);
        }
        return;
      default:
        // Unknown notifications are ignored (AWP-VER-003); safety.approval_requested is for approvers.
        this.emit("notification", n.method, n.params);
    }
  }

  /**
   * Records a `status_seq` as processed. Returns false for a redelivery (AWP-LIF-009, AWP-CTL-008).
   * `lastStatusSeq` only advances over a gapless prefix, so a resumption replays anything missed.
   */
  private processSeq(seq: number): boolean {
    if (seq <= this.lastStatusSeq || this.processedAbove.has(seq)) return false;
    if (seq === this.lastStatusSeq + 1) {
      this.lastStatusSeq = seq;
      while (this.processedAbove.delete(this.lastStatusSeq + 1)) this.lastStatusSeq++;
    } else {
      this.processedAbove.add(seq);
    }
    if (this.replayTo !== undefined && this.lastStatusSeq >= this.replayTo) this.emit("replay_complete");
    return true;
  }

  private applyToRecord(actionId: string, u: StatusUpdate): void {
    const rec = this.records.get(actionId);
    if (!rec) {
      this.warn(`status for unknown action ${actionId}`);
      return;
    }
    const out = rec.apply(u);
    if (out.kind === "applied") {
      if (out.note) this.warn(`${actionId}: ${out.note}`);
      this.admittedIds.add(actionId);
      if (u.state === "executing" && this.submittedAfterResume.has(actionId)) {
        // A new action executes: control regained (AWP-AGT-007, AWP-SAF-008).
        this.assumedSafeState = false;
      }
      if (u.state === "executing" && this.worldSafeState && this.submittedAfterResume.has(actionId)) {
        this.worldSafeState = false;
      }
      this.emit("status", rec, u);
      if (rec.terminal) this.emit("terminal", rec);
    } else if (out.kind === "after_terminal") {
      this.warn(`${actionId}: ${out.note}`);
    }
  }

  private onActionStatus(s: ActionStatus): void {
    if (!this.processSeq(s.status_seq)) return;
    const u: StatusUpdate = { state: s.state, status_seq: s.status_seq, ts_mono_ns: s.ts_mono_ns, source: "status", raw: s };
    if (s.reason !== undefined) u.reason = s.reason;
    if (s.progress !== undefined) u.progress = s.progress;
    if (s.detail !== undefined) u.detail = s.detail;
    this.applyToRecord(s.action_id, u);
  }

  private onWorldEvent(e: WorldEvent): void {
    if (!this.processSeq(e.status_seq)) return;
    switch (e.event) {
      case "safe_state_entered":
        this.worldSafeState = true;
        this.assumedSafeState = true;
        // Anything admitted from here on counts as a new action for regaining control.
        this.submittedAfterResume.clear();
        break;
      case "safe_state_exited":
        this.worldSafeState = false;
        this.assumedSafeState = false;
        break;
      case "e_stop_engaged":
        this.eStopEngaged = true;
        break;
      case "e_stop_released":
        this.eStopEngaged = false;
        break;
      default:
        break;
    }
    this.emit("event", e);
  }

  private onSessionState(s: SessionStateNotification): void {
    if (!this.processSeq(s.status_seq)) return;
    this.emit("session_state", s);
    if (s.state === "active" && this.state === "ready") this.state = "active";
    if (s.state === "closed") {
      const ours = this.state === "closing";
      this.finish(s.reason ?? "closed by the world");
      // During our own session.close the result follows this notification (AWP-SES-011).
      if (!ours) {
        this.closingIntentionally = true;
        this.conn?.close(1000, "");
      }
    }
  }

  private onFrame(params: unknown, receivedAt: number, connection: number): void {
    let frame: Frame;
    try {
      frame = decodeInlineFrame(params);
    } catch (err) {
      if (err instanceof ProtocolError && err.code === ErrorCode.AWP_INTEGER_RANGE) {
        this.onIntegerRange(err.message);
        return;
      }
      this.warn(`dropped malformed obs.frame: ${(err as Error).message}`);
      return;
    }
    this.processFrame(frame, receivedAt, connection);
  }

  private processFrame(frame: Frame, receivedAt: number, connection: number): void {
    const ch = this.channels.get(frame.channel_id);
    if (!ch) {
      this.warn(`obs.frame on channel ${frame.channel_id}, which is not granted`);
      return;
    }
    if (!ch.consumed) return; // AWP-MOD-002: ignore modalities this agent did not declare
    const rf: ReceivedFrame = { frame, receivedAt, connection };
    const verdict = ch.tracker.observe(rf, this.mode === "streaming" ? this.estimator.offsetNs : undefined);
    if (verdict.kind === "discard_late") return;
    if (verdict.kind === "discard_violation") {
      this.warn(verdict.detail);
      return;
    }
    if (verdict.gap > 0 && !verdict.resync && ch.tracker.lossClass === "reliable") {
      this.emit("loss", ch.grant.channel, verdict.gap);
    }
    if (verdict.resync) this.emit("resync", ch.grant.channel, frame);
    if (this.state === "ready") this.state = "active";
    if (frame.tick !== undefined) {
      let seen = this.ticksSeen.get(frame.channel_id);
      if (!seen) this.ticksSeen.set(frame.channel_id, (seen = new Set()));
      seen.add(frame.tick);
      if (seen.size > 64) seen.delete(seen.values().next().value as number);
      this.flushTickWaiters();
    }
    this.emit("frame", ch.grant.channel, frame, rf);
  }

  /**
   * AWP-CTL-009: on a value above 2^53 − 1 (JSON or binary frame) the agent ends the session — it sends
   * `session.close`, then closes the connection with close code 1002 and reason AWP_INTEGER_RANGE — and
   * never resumes it.
   */
  private onIntegerRange(detail: string): void {
    if (this.state === "closed") return;
    this.warn(`AWP_INTEGER_RANGE: ${detail}; ending the session`);
    this.closingIntentionally = true;
    const conn = this.conn;
    if (conn && !conn.closed && this.connectionHasSession) {
      conn.request("session.close", {}).catch(() => undefined);
    }
    this.finish("AWP_INTEGER_RANGE");
    conn?.close(PROTOCOL_ERROR_CLOSE, "AWP_INTEGER_RANGE");
  }

  // ---------------------------------------------------------------------------------------------
  // Connection loss, reconnection, resumption

  private onConnectionLost(reason: string): void {
    if (this.state === "closed") return;
    this.stopTimers();
    this.closeStreams();
    if (this.closingIntentionally || this.state === "closing") {
      this.finish(this.state === "closing" ? "connection lost while closing" : reason);
      return;
    }
    if (!this.ready || this.options.reconnect === false) {
      this.finish(`control connection lost before a session was established: ${reason}`);
      return;
    }
    if (this.reconnecting) return;
    this.state = "suspended";
    this.lostAt = this.clock.now();
    this.emit("suspended", reason);
    this.log("debug", `control connection lost (${reason}); resuming within ${this.reconnectWindowMs} ms`);
    this.reconnecting = true;
    void this.reconnectLoop().finally(() => {
      this.reconnecting = false;
    });
  }

  private async reconnectLoop(): Promise<void> {
    let delay = 100;
    const windowEnd = this.lostAt + this.reconnectWindowMs * 1e6;
    let attempt = 0;
    while (this.state === "suspended") {
      const remainingMs = (windowEnd - this.clock.now()) / 1e6;
      if (remainingMs <= 0) {
        this.finish("reconnect window expired");
        return;
      }
      attempt++;
      try {
        const ws = await this.openSocket(Math.min(5000, Math.max(250, remainingMs)));
        const conn = this.attach(ws);
        this.startTimers();
        // Sessionless until the resume result: bounded by the reconnect window, not by silence (AWP-SES-012).
        const left = () => Math.max(1, (windowEnd - this.clock.now()) / 1e6);
        const { result: m } = await this.call<WorldManifest>("initialize", this.agentManifest(), left());
        const check = checkManifest(m, SUPPORTED_PROTOCOL_VERSIONS);
        if (!check.valid) throw new ManifestInvalidError(check.problems);
        if (canonical(m) !== this.originalManifest) {
          this.warn("the manifest changed across the reconnection; the world has restarted (AWP-MAN-003)");
        }
        this.manifest = new Manifest(m);
        const { result } = await this.call<SessionReady>(
          "session.resume",
          { session_token: this.ready!.session_token, last_status_seq: this.lastStatusSeq },
          left(),
          (r) => {
            if (this.conn !== conn) return;
            this.replayTo = r.replay_to_status_seq;
            this.sessionConnection = conn.connectionId;
            if (this.mode === "lockstep") {
              // After the replay the world sends a resync keyframe with the current tick on every per-tick
              // channel; it also completes an advance whose frames were lost (AWP-TIM-009, AWP-TIM-003).
              for (const seen of this.ticksSeen.values()) seen.clear();
            }
            this.applyReady(r, true);
            if (this.mode === "lockstep" && this.tick !== undefined) {
              this.resumeObservation = this.waitForTickFrames(this.tick, this.reconnectWindowMs || 30000).catch((e: Error) =>
                this.warn(e.message),
              );
            }
            this.resumes++;
            // AWP-AGT-007: the embodiment is in safe state until a new action executes.
            this.assumedSafeState = true;
            this.worldSafeState = r.safe_state === true;
            this.submittedAfterResume.clear();
            this.state = "active";
          },
        );
        if (this.conn !== conn) continue;
        this.log("debug", `session resumed (attempt ${attempt}); replay through status_seq ${result.replay_to_status_seq ?? "?"}`);
        this.emit("resumed", result);
        const waiters = this.resumeWaiters;
        this.resumeWaiters = [];
        for (const w of waiters) w.resolve();
        return;
      } catch (err) {
        if (err instanceof AwpError && (err.code === ErrorCode.AWP_SESSION_UNKNOWN || err.code === ErrorCode.AWP_SESSION_EXPIRED)) {
          // AWP-SES-008: treat the session as Closed; nothing survives.
          this.conn?.close(1000, "");
          this.finish(err.errorName);
          return;
        }
        if (err instanceof ManifestInvalidError) {
          this.conn?.close(1000, "");
          this.finish("invalid manifest on reconnection");
          return;
        }
        this.log("debug", `reconnect attempt ${attempt} failed: ${(err as Error).message}`);
        if (this.conn && !this.conn.closed) this.conn.terminate();
        await new Promise((r) => setTimeout(r, delay));
        delay = Math.min(delay * 2, 2000);
      }
    }
  }

  /** Resolves when the session is usable again after a suspension; rejects if it closes instead. */
  private waitResumed(): Promise<void> {
    if (this.state !== "suspended") {
      return this.state === "closed" ? Promise.reject(new SessionClosedError(this.closeReason ?? "closed")) : Promise.resolve();
    }
    return new Promise((resolve, reject) => this.resumeWaiters.push({ resolve, reject }));
  }

  /** Resolves when the replay announced by the latest resumption has been processed (AWP-CTL-008). */
  waitReplay(timeoutMs = this.heartbeatIntervalMs * 3): Promise<void> {
    if (this.replayComplete) return Promise.resolve();
    return new Promise((resolve) => {
      const t = setTimeout(done, timeoutMs);
      const self = this;
      function done() {
        clearTimeout(t);
        self.off("replay_complete", done);
        self.off("closed", done);
        resolve();
      }
      this.on("replay_complete", done);
      this.on("closed", done);
    });
  }

  private finish(reason: string): void {
    if (this.state === "closed") return;
    this.state = "closed";
    this.closeReason = reason;
    this.stopTimers();
    this.closeStreams();
    for (const r of this.records.values()) r.markLost();
    for (const w of this.resumeWaiters) w.reject(new SessionClosedError(reason));
    this.resumeWaiters = [];
    for (const w of this.tickWaiters) w.resolve();
    this.tickWaiters = [];
    const cw = this.closedWaiters;
    this.closedWaiters = [];
    for (const w of cw) w();
    this.emit("closed", reason);
  }

  /** Resolves once the session (or pre-session connection) is closed. */
  whenClosed(): Promise<string> {
    if (this.state === "closed") return Promise.resolve(this.closeReason ?? "closed");
    return new Promise((resolve) => this.closedWaiters.push(() => resolve(this.closeReason ?? "closed")));
  }

  /**
   * Runs `fn` against the current connection; if the connection drops before a response, waits for
   * the resumption and runs it again (callers make `fn` safe to repeat).
   */
  private async resilient<T>(fn: () => Promise<T>): Promise<T> {
    let lost = false;
    for (;;) {
      if (this.state === "suspended" || lost) {
        await this.waitResumed();
        // Process the replay before resubmitting anything whose admission was not seen (AWP-AGT-007),
        // and in lockstep observe the resync keyframes before acting (AWP-TIM-009).
        await this.waitReplay();
        if (this.resumeObservation) await this.resumeObservation;
      }
      if (this.state === "closed") throw new SessionClosedError(this.closeReason ?? "closed");
      try {
        return await fn();
      } catch (err) {
        if (err instanceof ConnectionLostError && this.ready && !this.isClosed() && this.options.reconnect !== false) {
          // The loss may not have been noticed yet; give the close/liveness path a moment.
          lost = true;
          await new Promise((r) => setTimeout(r, 25));
          continue;
        }
        throw err;
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Actions

  private isClosed(): boolean {
    return this.state === "closed";
  }

  private requireSession(): SessionReady {
    if (!this.ready || this.state === "closed") throw new UsageError("no open session");
    if (this.state === "closing") throw new UsageError("the session is closing");
    return this.ready;
  }

  /**
   * AWP-ERR-001: a submission retries a refused one when its `type`, `params`, and `embodiment_id` are
   * the same, whatever its `action_id`.
   */
  private refusalKey(method: string, params: Record<string, unknown>): string {
    return `${method}:${canonical({ type: params.type, params: params.params, embodiment_id: params.embodiment_id ?? null })}`;
  }

  /**
   * Submits an action and resolves with its record once admitted (AWP-LIF-002). Only granted types on
   * the bound embodiment are submitted (AWP-AGT-003); a fresh unique `action_id` is generated unless
   * given (AWP-AGT-004). If the connection drops before the result arrives, the identical submission
   * is retried after resumption — idempotent by AWP-ACT-001 — unless the replay already reported it.
   * A JSON-RPC error marks the record refused and is thrown as AwpError; non-retryable refusals are
   * remembered and an identical submission is refused locally (AWP-ERR-001).
   */
  async submit(type: string, params: Record<string, unknown>, opts: SubmitOptions = {}): Promise<ActionRecord> {
    const ready = this.requireSession();
    const manifest = this.manifest!;
    if (this.embodiment === undefined) throw new UsageError("observer session: no embodiment is bound, submissions are forbidden (AWP-EMB-004)");
    if (!ready.granted.action_types.includes(type)) {
      throw new UsageError(`action type ${type} is not granted in this session (AWP-AGT-003, AWP-PRM-001)`);
    }
    const emb = manifest.embodiment(this.embodiment);
    if (emb && !emb.action_types.includes(type)) throw new UsageError(`embodiment ${this.embodiment} does not offer ${type}`);
    if (opts.embodimentId !== undefined && opts.embodimentId !== this.embodiment) {
      throw new UsageError(`embodiment ${opts.embodimentId} is not bound to this session (AWP-AGT-003)`);
    }
    const policies = manifest.preemptionPolicies(type);
    if (opts.preempt !== undefined && !policies.includes(opts.preempt)) {
      throw new UsageError(`preempt ${opts.preempt} is not declared for ${type} (declared: ${policies.join(", ")}, AWP-ACT-005)`);
    }
    const problems = manifest.checkParams(type, params);
    if (problems.length > 0) throw new UsageError(`params for ${type} fail its params_schema: ${problems.join("; ")}`);

    const actionId = opts.actionId ?? newActionId();
    const existing = this.records.get(actionId);
    if (existing && (existing.admitted || this.admittedIds.has(actionId))) {
      throw new UsageError(`action_id ${actionId} was already admitted and must not be reused (AWP-ACT-010)`);
    }
    const sub: ActionSubmitParams = { action_id: actionId, type, params };
    if (opts.embodimentId !== undefined) sub.embodiment_id = opts.embodimentId;
    if (opts.preempt !== undefined) sub.preempt = opts.preempt;
    if (opts.deadlineMs !== undefined) sub.deadline_ms = opts.deadlineMs;
    if (opts.basis) sub.basis_ts_mono_ns = opts.basis.frame.ts_mono_ns; // copied unchanged (AWP-CLK-009)
    if (opts.validForMs !== undefined && this.mode === "streaming") {
      if (!(await this.clockReady())) throw new UsageError("no clock-offset estimate yet; valid_until_ns cannot be expressed (AWP-CLK-008)");
      sub.valid_until_ns = this.estimator.toSession(this.clock.now()) + Math.round(opts.validForMs * 1e6);
    }
    const key = this.refusalKey("action.submit", sub as unknown as Record<string, unknown>);
    const prior = this.refused.get(key);
    if (prior) {
      throw new UsageError(`an identical submission was refused with non-retryable ${prior.errorName}; not retrying (AWP-ERR-001)`);
    }

    const rec = new ActionRecord(sub);
    return this.sendSubmission(rec, key, opts.basis);
  }

  /**
   * Re-sends a record's exact submission — identical content (AWP-ACT-001, AWP-ACT-009) and the same
   * `action_id` — after a retryable refusal (AWP-ACT-010). Honour `retry_after_ms` before calling.
   */
  async resubmit(rec: ActionRecord): Promise<ActionRecord> {
    this.requireSession();
    if (!rec.refused) throw new UsageError(`action ${rec.action_id} was not refused; an admitted action_id is never reused (AWP-ACT-010)`);
    const key = this.refusalKey("action.submit", rec.submission as unknown as Record<string, unknown>);
    const prior = this.refused.get(key);
    if (prior) throw new UsageError(`identical submission refused with non-retryable ${prior.errorName}; not retrying (AWP-ERR-001)`);
    return this.sendSubmission(new ActionRecord(rec.submission), key, undefined);
  }

  private sendSubmission(rec: ActionRecord, key: string, basis: ReceivedFrame | undefined): Promise<ActionRecord> {
    const sub = rec.submission;
    const actionId = sub.action_id;
    this.records.set(actionId, rec);
    return this.resilient(async () => {
      if (rec.admitted) return rec; // the replay reported it while we were suspended
      this.submittedAfterResume.add(actionId);
      try {
        await this.call<ActionSubmitResult>("action.submit", sub, undefined, (result, info) => {
          rec.transmittedAt = info.sentAt;
          if (basis) this.decisionLatencies.push(info.sentAt - basis.receivedAt);
          rec.received_ts_mono_ns = result.received_ts_mono_ns;
          this.admittedIds.add(actionId);
          if (this.processSeq(result.status_seq) || !rec.admitted) {
            const u: StatusUpdate = { state: result.state, status_seq: result.status_seq, ts_mono_ns: result.ts_mono_ns, source: "submit" };
            if (result.reason !== undefined) u.reason = result.reason;
            this.applyToRecord(actionId, u);
          }
        });
        return rec;
      } catch (err) {
        if (err instanceof AwpError) {
          // No action exists (AWP-ACT-010); the same action_id may be submitted again.
          rec.markRefused();
          this.submittedAfterResume.delete(actionId);
          if (!err.retryable) this.refused.set(key, err);
        }
        throw err;
      }
    });
  }

  /** Requests cancellation (AWP-LIF-005); the result's transition is applied to the record. */
  async cancel(actionId: string): Promise<ActionRecord> {
    this.requireSession();
    const rec = this.records.get(actionId);
    if (!rec) throw new UsageError(`unknown action ${actionId}`);
    return this.resilient(async () => {
      if (rec.terminal) return rec;
      await this.call<ActionCancelResult>("action.cancel", { action_id: actionId }, undefined, (result) => {
        this.processSeq(result.status_seq);
        this.applyToRecord(actionId, { state: result.state, status_seq: result.status_seq, source: "cancel" });
      });
      return rec;
    });
  }

  /** Pulls an action's current state (`action.status` request). */
  async pullStatus(actionId: string): Promise<ActionRecord> {
    this.requireSession();
    const rec = this.records.get(actionId);
    if (!rec) throw new UsageError(`unknown action ${actionId}`);
    await this.call<ActionStatus>("action.status", { action_id: actionId }, undefined, (result) => {
      this.processSeq(result.status_seq);
      const u: StatusUpdate = { state: result.state, status_seq: result.status_seq, ts_mono_ns: result.ts_mono_ns, source: "pull" };
      if (result.reason !== undefined) u.reason = result.reason;
      this.applyToRecord(actionId, u);
    });
    return rec;
  }

  // ---------------------------------------------------------------------------------------------
  // Subscriptions

  private applyGrantList(granted: ChannelGrant[]): void {
    this.ready!.granted.channels = granted;
    this.rebuildChannels(granted, true);
  }

  /** `obs.subscribe`: declared, consumed channels only (AWP-AGT-003, AWP-MOD-002). */
  async subscribe(channels: (string | SubscribeRequest)[]): Promise<ChannelGrant[]> {
    this.requireSession();
    const reqs = channels.map((c) => (typeof c === "string" ? { channel: c } : c));
    for (const r of reqs) {
      const decl = this.manifest!.channel(r.channel);
      if (!decl) throw new UsageError(`channel ${r.channel} is not declared in the manifest`);
      if (!this.options.consumesModalities.includes(decl.modality)) throw new UsageError(`channel ${r.channel}: modality ${decl.modality} not consumed`);
      const emb = this.embodiment ? this.manifest!.embodiment(this.embodiment) : undefined;
      if (emb && !emb.channels.includes(r.channel) && !this.channel(r.channel)) {
        throw new UsageError(`channel ${r.channel} is not offered by embodiment ${emb.id} (AWP-AGT-003)`);
      }
    }
    const before = new Set(this.channels.keys());
    const { result } = await this.call<{ granted: ChannelGrant[] }>("obs.subscribe", { channels: reqs }, undefined, (r) => this.applyGrantList(r.granted));
    if (this.mode === "lockstep" && this.tick !== undefined) {
      const fresh = [...this.channels.values()].filter((c) => !before.has(c.grant.channel_id) && c.perTick && c.consumed);
      if (fresh.length > 0) await this.waitForTickFrames(this.tick, this.heartbeatIntervalMs * 3, new Set(fresh.map((c) => c.grant.channel_id)));
    }
    return result.granted;
  }

  async unsubscribe(channels: string[]): Promise<ChannelGrant[]> {
    this.requireSession();
    const { result } = await this.call<{ granted: ChannelGrant[] }>("obs.unsubscribe", { channels }, undefined, (r) => {
      // A channel removed and later re-subscribed starts fresh accounting.
      this.applyGrantList(r.granted);
    });
    return result.granted;
  }

  // ---------------------------------------------------------------------------------------------
  // Lockstep

  private flushTickWaiters(): void {
    const keep: typeof this.tickWaiters = [];
    for (const w of this.tickWaiters) {
      const done = [...w.channels].every((id) => this.ticksSeen.get(id)?.has(w.tick) || !this.channels.has(id));
      if (done) w.resolve();
      else keep.push(w);
    }
    this.tickWaiters = keep;
  }

  private perTickChannelIds(): Set<number> {
    return new Set([...this.channels.values()].filter((c) => c.perTick && c.consumed).map((c) => c.grant.channel_id));
  }

  private waitForTickFrames(tick: number, timeoutMs: number, channels = this.perTickChannelIds()): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.tickWaiters = this.tickWaiters.filter((w) => w !== waiter);
        const missing = [...channels].filter((id) => !this.ticksSeen.get(id)?.has(tick));
        reject(new ProtocolError("AWP_MALFORMED", `no frame for tick ${tick} on channel(s) ${missing.join(", ")} (AWP-TIM-003)`));
      }, timeoutMs);
      const waiter = {
        tick,
        channels,
        resolve: () => {
          clearTimeout(timer);
          resolve();
        },
      };
      this.tickWaiters.push(waiter);
      this.flushTickWaiters();
    });
  }

  /**
   * `world.tick`: the only way this SDK advances time (AWP-AGT-009). Resolves when the advance is
   * complete per AWP-TIM-003 — the result is held and every subscribed per-tick channel delivered a
   * frame whose `tick` equals the result's. On AWP_TICK_MISMATCH the known tick is updated from
   * `data.tick` and the error is rethrown (nothing advanced, AWP-TIM-011).
   */
  async advance(count = 1): Promise<number> {
    this.requireSession();
    if (this.mode !== "lockstep") throw new UsageError("world.tick is only valid in lockstep sessions");
    if (!Number.isInteger(count) || count < 1) throw new UsageError("count must be an integer ≥ 1");
    const expected = this.tick;
    if (expected === undefined) throw new UsageError("current tick unknown");
    for (const s of this.ticksSeen.values()) for (const t of [...s]) if (t > expected) s.delete(t);
    const resumesBefore = this.resumes;
    const params: { expected_tick: number; count?: number } = { expected_tick: expected };
    if (count !== 1) params.count = count;
    let result: { tick: number };
    try {
      result = await this.resilient(async () => {
        if (this.tick !== undefined && this.tick !== expected) return { tick: this.tick }; // advanced before the loss
        return (
          await this.call<{ tick: number }>("world.tick", params, undefined, (r) => {
            this.tick = r.tick;
          })
        ).result;
      });
    } catch (err) {
      if (err instanceof AwpError && err.code === ErrorCode.AWP_TICK_MISMATCH && typeof err.data?.tick === "number") {
        this.tick = err.data.tick;
      }
      throw err;
    }
    this.tick = result.tick;
    // Across a reconnection the resync keyframes carrying the current tick complete the advance (AWP-TIM-009).
    if (this.resumes !== resumesBefore && this.resumeObservation) await this.resumeObservation;
    await this.waitForTickFrames(result.tick, this.heartbeatIntervalMs * 3);
    return result.tick;
  }

  /**
   * Lockstep-only convenience: submit, then advance until the action is terminal or `maxTicks` pass.
   * A distinct entry point that fails in a streaming session (AWP-AGT-009).
   */
  async submitAndAdvance(type: string, params: Record<string, unknown>, opts: SubmitOptions & { maxTicks?: number } = {}): Promise<ActionRecord> {
    if (this.mode !== "lockstep") throw new UsageError("submitAndAdvance is lockstep-only (AWP-AGT-009)");
    const rec = await this.submit(type, params, opts);
    const max = opts.maxTicks ?? 1000;
    for (let i = 0; i < max && !rec.terminal && !rec.lost; i++) await this.advance(1);
    return rec;
  }

  // ---------------------------------------------------------------------------------------------
  // Task and administrative operations (beyond Core; gated on capabilities and grants)

  private requireCapability(key: string, method: string): void {
    if (this.manifest?.raw.capabilities?.[key] !== true) {
      // Absent the key, the feature's messages are unknown to the world (AWP-VER-007).
      throw new UsageError(`${method} requires the ${key} capability, which the world does not advertise`);
    }
  }

  private requireAdmin(op: AdminOperation): void {
    if (!(this.ready?.granted.admin ?? []).includes(op)) {
      throw new UsageError(`${op} is not in granted.admin (default-deny, AWP-PRM-001, AWP-PRM-005)`);
    }
  }

  /** `task.update`: replaces the task whole (AWP-TSK-003); requires `capabilities.task`. */
  async updateTask(task: NonNullable<SessionOpenParams["task"]>): Promise<void> {
    this.requireSession();
    this.requireCapability("task", "task.update");
    await this.call("task.update", { task });
  }

  /** `world.snapshot` (admin `snapshot`, `capabilities.snapshot`). The token is a credential. */
  async snapshot(): Promise<string> {
    this.requireSession();
    this.requireCapability("snapshot", "world.snapshot");
    this.requireAdmin("snapshot");
    const { result } = await this.call<{ snapshot_token: string }>("world.snapshot", {});
    return result.snapshot_token;
  }

  private async resetLike(method: "world.reset" | "world.restore", params: Record<string, unknown>): Promise<number | undefined> {
    // The fresh frames may precede or follow the result (AWP-PRM-006 does not order them), so forget
    // earlier ticks before sending: the restored tick may be one already seen.
    for (const seen of this.ticksSeen.values()) seen.clear();
    const { result } = await this.call<{ tick?: number }>(method, params, undefined, (r) => {
      if (r.tick !== undefined) this.tick = r.tick;
    });
    // In lockstep a fresh frame carrying the restored tick follows on every per-tick channel (AWP-PRM-006).
    if (this.mode === "lockstep" && result.tick !== undefined) {
      await this.waitForTickFrames(result.tick, this.heartbeatIntervalMs * 3).catch((e: Error) => this.warn(e.message));
    }
    return result.tick;
  }

  /** `world.restore` (admin `restore`, `capabilities.snapshot`). */
  async restore(snapshotToken: string): Promise<number | undefined> {
    this.requireSession();
    this.requireCapability("snapshot", "world.restore");
    this.requireAdmin("restore");
    return this.resetLike("world.restore", { snapshot_token: snapshotToken });
  }

  /** `world.reset` (admin `reset`); `seed` requires `capabilities.seed`. */
  async reset(opts: { initialState?: string; seed?: number } = {}): Promise<number | undefined> {
    this.requireSession();
    this.requireAdmin("reset");
    const params: Record<string, unknown> = {};
    if (opts.initialState !== undefined) params.initial_state = opts.initialState;
    if (opts.seed !== undefined) {
      this.requireCapability("seed", "world.reset with seed");
      params.seed = opts.seed;
    }
    return this.resetLike("world.reset", params);
  }

  // ---------------------------------------------------------------------------------------------
  // Command frames (inline binding)

  private cmdSeq = new Map<number, number>();
  private cmdLastSent = new Map<number, number>();
  private cmdPending = new Map<number, { payload: Uint8Array; fields: Partial<FrameFields>; timer: NodeJS.Timeout }>();

  /**
   * Sends a command frame inline (`cmd.frame`, AWP-DAT-004) on a command channel whose bound
   * `duration: "streaming"` action is executing (AWP-CMD-003). `seq` increases per channel and
   * `ts_mono_ns` is the issue time mapped through the clock offset (AWP-CMD-007, AWP-CLK-009).
   *
   * The negotiated rate is never exceeded (AWP-DAT-003). On a `latest-wins` channel at most one
   * undelivered frame is held and a newer one replaces it (AWP-DAT-002, AWP-TRN-009): a frame produced
   * before the previous one could be sent supersedes it instead of queueing.
   *
   * While the stream connection that carried the channel is lost, the frame is not sent at all — never
   * inline (AWP-TRN-010) — and the result is `"dropped"` (a held frame dropped later is announced with a
   * `command_dropped` event).
   */
  async sendCommandFrame(channel: string, payload: Uint8Array, fields: Partial<FrameFields> = {}): Promise<"sent" | "held" | "replaced" | "dropped"> {
    this.requireSession();
    if (this.mode !== "streaming") throw new UsageError("command channels are streaming-only (AWP-CMD-001)");
    const ch = this.channel(channel);
    const decl = this.manifest?.raw.command_channels?.find((c) => c.id === channel);
    if (!ch || !decl) throw new UsageError(`command channel ${channel} is not granted`);
    const anchors = this.manifest!.raw.action_schemas.filter((a) => a.command_channel === channel).map((a) => a.type);
    const bound = [...this.records.values()].filter((r) => anchors.includes(r.submission.type) && r.state === "executing");
    if (bound.length !== 1) throw new UsageError(`no single executing ${anchors.join("/")} action is bound to ${channel} (AWP-CMD-003)`);
    if (!(await this.clockReady())) throw new UsageError("no clock-offset estimate (AWP-CLK-008)");
    const id = ch.grant.channel_id;
    if (this.channelDown(id)) {
      this.emit("command_dropped", channel, "stream_lost");
      return "dropped";
    }
    const rate = ch.grant.rate_hz ?? decl.rate_hz;
    const minGapNs = rate ? 1e9 / rate : 0;
    const buffered = () => this.commandTransport(id)?.bufferedAmount ?? this.requireConn().ws.bufferedAmount;
    const due = (this.cmdLastSent.get(id) ?? -Infinity) + minGapNs;
    const blocked = this.clock.now() < due || buffered() > 0;
    if (!blocked && !this.cmdPending.has(id)) {
      this.emitCommand(id, payload, fields);
      return "sent";
    }
    if (decl.loss_class === "latest-wins") {
      const prior = this.cmdPending.get(id);
      if (prior) clearTimeout(prior.timer);
      const timer = setTimeout(() => this.flushCommand(id), Math.max(1, Math.ceil((due - this.clock.now()) / 1e6)));
      timer.unref();
      this.cmdPending.set(id, { payload, fields, timer });
      return prior ? "replaced" : "held";
    }
    // reliable: wait for the rate slot and for the transport to drain, preserving seq order.
    while (this.clock.now() < due || buffered() > 0 || this.cmdPending.has(id)) {
      await new Promise((r) => setTimeout(r, Math.max(1, Math.ceil((due - this.clock.now()) / 1e6))));
    }
    return this.emitCommand(id, payload, fields) ? "sent" : "dropped";
  }

  private flushCommand(id: number): void {
    const p = this.cmdPending.get(id);
    if (!p) return;
    const conn = this.conn;
    if (!conn || conn.closed) {
      this.cmdPending.delete(id);
      return;
    }
    if ((this.commandTransport(id)?.bufferedAmount ?? conn.ws.bufferedAmount) > 0) {
      p.timer = setTimeout(() => this.flushCommand(id), 2);
      p.timer.unref();
      return;
    }
    this.cmdPending.delete(id);
    this.emitCommand(id, p.payload, p.fields);
  }

  /** Sends a command frame on the channel's current path; false if the channel is down (AWP-TRN-010). */
  private emitCommand(id: number, payload: Uint8Array, fields: Partial<FrameFields>): boolean {
    if (this.channelDown(id)) {
      const name = this.channels.get(id)?.grant.channel ?? String(id);
      this.emit("command_dropped", name, "stream_lost");
      return false;
    }
    const seq = (this.cmdSeq.get(id) ?? 0) + 1;
    this.cmdSeq.set(id, seq);
    const f: FrameFields = { ...fields, channel_id: id, seq, ts_mono_ns: this.estimator.toSession(this.clock.now()), payload };
    delete f.ts_send_ns; // command frames never carry ts_send_ns (AWP-OBS-006)
    delete f.ts_sim_ns; // not meaningful agent→world
    delete f.tick;
    const stream = this.commandTransport(id);
    // Once a stream connection carrying the channel is up, the channel travels only there (AWP-TRN-012).
    if (stream) stream.send(encodeBinaryFrame(f));
    else this.requireConn().notify("cmd.frame", encodeInlineFrame(f));
    this.cmdLastSent.set(id, this.clock.now());
    return true;
  }

  /** A lost stream connection carried the channel and none carries it now (AWP-TRN-010). */
  channelDown(id: number): boolean {
    if (this.commandTransport(id)) return false;
    return this.lostStreams.some((ep) => ep.channels === undefined || ep.channels.includes(id));
  }

  /** The established stream connection carrying a channel, if any; otherwise the channel is inline. */
  private commandTransport(id: number): StreamConnection | undefined {
    return this.streams.find((s) => s.established && s.isOpen && s.carries(id));
  }

  // ---------------------------------------------------------------------------------------------
  // Closing

  /**
   * `session.close`. The world answers only after every action is terminal and `session.state: closed`
   * was sent (AWP-SES-011), so the wait allows the longest declared `max_abort_ms`.
   */
  async close(): Promise<void> {
    if (this.state === "closed") {
      this.conn?.close(1000, "");
      return;
    }
    const conn = this.conn;
    if (!this.ready || !conn || conn.closed) {
      this.closingIntentionally = true;
      conn?.close(1000, "");
      this.finish("closed by the agent");
      return;
    }
    if (this.state === "suspended") {
      try {
        await this.waitResumed();
      } catch {
        return;
      }
    }
    this.state = "closing";
    const budget = Math.max(5000, (this.manifest?.longestMaxAbortMs ?? 0) * 2 + 3 * this.heartbeatIntervalMs);
    try {
      await this.call("session.close", {}, budget);
    } catch (err) {
      this.warn(`session.close: ${(err as Error).message}`);
    }
    this.closingIntentionally = true;
    this.finish("closed by the agent");
    this.conn?.close(1000, "");
  }

  /** Drops the control connection without closing the session (tests, diagnostics). */
  dropConnection(): void {
    this.conn?.terminate();
  }

  /** Closes the connection without a session (e.g. after an invalid manifest). */
  disconnect(): void {
    this.closingIntentionally = true;
    this.conn?.close(1000, "");
    this.finish(this.closeReason ?? "disconnected");
  }
}
