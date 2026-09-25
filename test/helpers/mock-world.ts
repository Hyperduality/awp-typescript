/** A scriptable in-process world for client tests: each test drives the protocol by hand. */
import { WebSocketServer, type WebSocket } from "ws";
import type { AddressInfo } from "node:net";

export type Msg = Record<string, any>;

export class MockConn {
  readonly ws: WebSocket;
  readonly received: Msg[] = [];
  readonly headers: Record<string, string | string[] | undefined>;
  readonly protocol: string;
  private waiters: { pred: (m: Msg) => boolean; resolve: (m: Msg) => void }[] = [];
  private cursor = 0;
  closed: { code: number; reason: string } | undefined;
  private closeWaiters: (() => void)[] = [];
  /** Answer the agent's pings automatically (default true). */
  autoPong = true;
  sessionClock = () => 1_000_000_000;

  constructor(ws: WebSocket, headers: Record<string, string | string[] | undefined>) {
    this.ws = ws;
    this.headers = headers;
    this.protocol = ws.protocol;
    ws.on("message", (data, isBinary) => {
      if (isBinary) return;
      const m = JSON.parse(data.toString()) as Msg;
      this.received.push(m);
      if (this.autoPong && m.method === "ping" && m.id !== undefined) {
        const t = this.sessionClock();
        this.send({ jsonrpc: "2.0", id: m.id, result: { origin_ns: m.params.origin_ns, receive_ns: t, transmit_ns: t } });
      }
      this.flush();
    });
    ws.on("close", (code, reason) => {
      this.closed = { code, reason: reason.toString() };
      for (const w of this.closeWaiters) w();
    });
  }

  private flush(): void {
    const keep: typeof this.waiters = [];
    for (const w of this.waiters) {
      const idx = this.received.findIndex((m, i) => i >= this.cursor && w.pred(m));
      if (idx >= 0) {
        this.cursor = idx + 1;
        w.resolve(this.received[idx]!);
      } else keep.push(w);
    }
    this.waiters = keep;
  }

  /** Next message from the agent (after the previous match) satisfying pred. */
  next(pred: ((m: Msg) => boolean) | string, timeoutMs = 3000): Promise<Msg> {
    const p = typeof pred === "string" ? (m: Msg) => m.method === pred : (pred as (m: Msg) => boolean);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timed out waiting for agent message`)), timeoutMs);
      this.waiters.push({
        pred: p,
        resolve: (m) => {
          clearTimeout(timer);
          resolve(m);
        },
      });
      this.flush();
    });
  }

  /** Messages of a method received so far. */
  all(method: string): Msg[] {
    return this.received.filter((m) => m.method === method);
  }

  send(m: Msg): void {
    if (this.ws.readyState === this.ws.OPEN) this.ws.send(JSON.stringify(m));
  }

  sendRaw(text: string): void {
    this.ws.send(text);
  }

  result(id: unknown, result: unknown): void {
    this.send({ jsonrpc: "2.0", id, result });
  }

  error(id: unknown, code: number, message: string, retryable = false, extra: Msg = {}): void {
    this.send({ jsonrpc: "2.0", id, error: { code, message, data: { retryable, ...extra } } });
  }

  notify(method: string, params: unknown): void {
    this.send({ jsonrpc: "2.0", method, params });
  }

  whenClosed(timeoutMs = 3000): Promise<{ code: number; reason: string }> {
    if (this.closed) return Promise.resolve(this.closed);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("connection not closed")), timeoutMs);
      this.closeWaiters.push(() => {
        clearTimeout(t);
        resolve(this.closed!);
      });
    });
  }

  drop(): void {
    this.ws.terminate();
  }
}

export class MockStream {
  readonly ws: WebSocket;
  readonly headers: Record<string, string | string[] | undefined>;
  readonly url: string;
  closed: { code: number; reason: string } | undefined;
  /** Binary messages the agent sent on this stream connection (command frames). */
  readonly received: Buffer[] = [];
  private closeWaiters: (() => void)[] = [];
  constructor(ws: WebSocket, headers: Record<string, string | string[] | undefined>, url: string) {
    this.ws = ws;
    this.headers = headers;
    this.url = url;
    ws.on("message", (data, isBinary) => {
      if (isBinary) this.received.push(Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer));
    });
    ws.on("close", (code, reason) => {
      this.closed = { code, reason: reason.toString() };
      for (const w of this.closeWaiters) w();
    });
  }
  sendFrame(bytes: Uint8Array): void {
    this.ws.send(bytes, { binary: true });
  }
  whenClosed(timeoutMs = 3000): Promise<{ code: number; reason: string }> {
    if (this.closed) return Promise.resolve(this.closed);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("stream not closed")), timeoutMs);
      this.closeWaiters.push(() => {
        clearTimeout(t);
        resolve(this.closed!);
      });
    });
  }
}

export class MockWorld {
  readonly server: WebSocketServer;
  readonly connections: MockConn[] = [];
  readonly streams: MockStream[] = [];
  private waiters: ((c: MockConn) => void)[] = [];
  private streamWaiters: ((s: MockStream) => void)[] = [];

  private constructor(server: WebSocketServer) {
    this.server = server;
    server.on("connection", (ws, req) => {
      if (req.url?.startsWith("/stream")) {
        const s = new MockStream(ws, req.headers, req.url);
        this.streams.push(s);
        const w = this.streamWaiters.shift();
        if (w) w(s);
        return;
      }
      const c = new MockConn(ws, req.headers);
      this.connections.push(c);
      const w = this.waiters.shift();
      if (w) w(c);
    });
  }

  acceptStream(timeoutMs = 3000): Promise<MockStream> {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no stream connection")), timeoutMs);
      this.streamWaiters.push((s) => {
        clearTimeout(t);
        resolve(s);
      });
    });
  }

  static async start(): Promise<MockWorld> {
    const server = new WebSocketServer({ port: 0, host: "127.0.0.1", handleProtocols: (protocols) => (protocols.has("awp") ? "awp" : false) });
    await new Promise<void>((r) => server.once("listening", () => r()));
    return new MockWorld(server);
  }

  get url(): string {
    const a = this.server.address() as AddressInfo;
    return `ws://127.0.0.1:${a.port}`;
  }

  /** The next (or an already pending) connection. */
  accept(timeoutMs = 3000): Promise<MockConn> {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("no connection")), timeoutMs);
      this.waiters.push((c) => {
        clearTimeout(t);
        resolve(c);
      });
    });
  }

  async stop(): Promise<void> {
    for (const c of this.connections) c.ws.terminate();
    for (const s of this.streams) s.ws.terminate();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
}

export function streamingManifest(): Msg {
  return {
    protocol_version: "0.1",
    world: { name: "mock", version: "0", vendor: "test" },
    time_models: ["streaming"],
    capabilities: {},
    embodiments: [{ id: "arm_01", kind: "manipulator", action_types: ["move_to_pose", "stop"], channels: ["proprio", "arm_state"] }],
    observation_channels: [
      { id: "proprio", modality: "proprio/json", rate_hz: 100, loss_class: "latest-wins", schema: {} },
      { id: "arm_state", modality: "text/event+json", rate_hz: 10, loss_class: "reliable", schema: {} },
    ],
    action_schemas: [
      {
        type: "move_to_pose",
        params_schema: { $ref: "#/$defs/p" },
        duration: "extended",
        preemption: ["replace", "queue"],
        concurrency_group: "arm",
        max_abort_ms: 500,
      },
      { type: "stop", params_schema: { type: "object", additionalProperties: false }, duration: "instant", preemption: "replace" },
    ],
    safety_policy: { envelopes: [], safe_state: { behavior: "safe_stop", watchdog_ms: 2000 } },
    $defs: {
      p: {
        type: "object",
        properties: { pose: { $ref: "https://agentworldprotocol.com/schemas/v0.1/common.schema.json#/$defs/pose" } },
        required: ["pose"],
        additionalProperties: false,
      },
    },
  };
}

export function lockstepManifest(): Msg {
  const m = streamingManifest();
  m.time_models = ["lockstep"];
  m.tick_policy = "on_tick";
  m.tick_authority = "any_session";
  for (const c of m.observation_channels) c.rate_hz = null;
  delete m.safety_policy.safe_state;
  return m;
}

export function sessionReady(extra: Msg = {}): Msg {
  return {
    session_id: "sess_1",
    session_token: "st_abcdefghijklmnop",
    reconnect_window_ms: 5000,
    heartbeat_interval_ms: 1000,
    granted: {
      channels: [
        { channel: "proprio", rate_hz: 100, channel_id: 1 },
        { channel: "arm_state", rate_hz: 10, channel_id: 2 },
      ],
      action_types: ["move_to_pose", "stop"],
      admin: [],
      envelopes: [],
    },
    stream_endpoints: [{ binding: "inline" }],
    clock_anchor: "2026-09-25T00:00:00Z",
    ...extra,
  };
}

export const pose = (x: number) => ({ pose: { frame: "base", p_m: [x, 0, 0.4], q: [0, 0, 0, 1] } });

export function b64(v: unknown): string {
  return Buffer.from(JSON.stringify(v)).toString("base64");
}
