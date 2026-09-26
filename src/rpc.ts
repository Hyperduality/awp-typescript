/**
 * JSON-RPC 2.0 over one AWP control connection (spec/transport/control-channel).
 *
 * - No batches are ever sent (AWP-CTL-006); a batch received is answered with -32600.
 * - Unknown methods in requests are answered with -32601 (AWP-CTL-002); unknown notifications are ignored.
 * - Unknown fields are ignored everywhere (AWP-VER-003).
 * - Every incoming text is scanned for integers beyond 2^53 − 1 (AWP-CTL-009).
 * - Every outgoing message is checked against the sender form of its canonical schema.
 */
import type WebSocket from "ws";
import { AwpError, ConnectionLostError, JsonRpcCode, ProtocolError, RequestTimeoutError, type WireError } from "./errors.ts";
import { assertSendable, parseJsonChecked } from "./ints.ts";
import { OUTGOING_PARAMS_SCHEMA, validate } from "./schemas.ts";
import type { Clock } from "./clock.ts";

export type RequestId = number | string;

export interface IncomingRequest {
  id: RequestId;
  method: string;
  params: unknown;
  /** Agent clock (ns) when the message arrived. */
  receivedAt: number;
}

export interface IncomingNotification {
  method: string;
  params: unknown;
  receivedAt: number;
}

export interface ResponseInfo {
  /** Agent clock (ns) when the response arrived. */
  receivedAt: number;
  /** Agent clock (ns) when the request was transmitted. */
  sentAt: number;
}

export interface RpcHandlers {
  /** Returns the result, or throws a WireError-shaped object / AwpError. Undefined → -32601. */
  request(req: IncomingRequest): { result: unknown } | undefined;
  notification(n: IncomingNotification): void;
  /** A 64-bit field exceeded 2^53 − 1 (AWP-CTL-009). */
  integerRange(detail: string): void;
  /** A message was not valid JSON-RPC; diagnostics only. */
  protocolWarning(detail: string): void;
  /** A binary WebSocket message arrived on the control connection. */
  binary?(data: Buffer, receivedAt: number): void;
}

interface Pending {
  method: string;
  sentAt: number;
  onResult: ((result: unknown, info: ResponseInfo) => void) | undefined;
  resolve: (v: { result: unknown; info: ResponseInfo }) => void;
  reject: (e: Error) => void;
  timer: NodeJS.Timeout | undefined;
}

export interface RpcOptions {
  /** Validate outgoing params against the sender form of the canonical schemas (default true). */
  validateOutgoing?: boolean;
  /** Called with every message sent or received (for tracing). */
  trace?: (direction: "out" | "in", message: unknown) => void;
}

export class RpcConnection {
  readonly ws: WebSocket;
  readonly connectionId: number;
  private nextId = 1;
  private readonly pending = new Map<RequestId, Pending>();
  private readonly clock: Clock;
  private readonly handlers: RpcHandlers;
  private readonly options: RpcOptions;
  /** Agent clock (ns) of the last message of any kind received from the world. */
  lastReceivedAt: number;
  closed = false;

  constructor(ws: WebSocket, connectionId: number, clock: Clock, handlers: RpcHandlers, options: RpcOptions = {}) {
    this.ws = ws;
    this.connectionId = connectionId;
    this.clock = clock;
    this.handlers = handlers;
    this.options = options;
    this.lastReceivedAt = clock.now();
    ws.on("message", (data: Buffer | ArrayBuffer | Buffer[], isBinary: boolean) => {
      const receivedAt = this.clock.now();
      this.lastReceivedAt = receivedAt;
      const buf = Array.isArray(data) ? Buffer.concat(data) : Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (isBinary) {
        if (this.handlers.binary) this.handlers.binary(buf, receivedAt);
        else this.handlers.protocolWarning("binary message on the control connection");
        return;
      }
      this.onText(buf.toString("utf8"), receivedAt);
    });
    ws.on("close", () => this.failAll(new ConnectionLostError()));
    ws.on("error", () => this.failAll(new ConnectionLostError()));
  }

  private failAll(err: Error): void {
    this.closed = true;
    for (const [id, p] of this.pending) {
      if (p.timer) clearTimeout(p.timer);
      this.pending.delete(id);
      p.reject(err);
    }
  }

  private sendRaw(msg: Record<string, unknown>): void {
    if (this.closed || this.ws.readyState !== this.ws.OPEN) throw new ConnectionLostError("control connection is not open");
    assertSendable(msg);
    this.options.trace?.("out", msg);
    this.ws.send(JSON.stringify(msg));
  }

  private checkOutgoing(method: string, params: unknown): void {
    if (this.options.validateOutgoing === false) return;
    const schema = OUTGOING_PARAMS_SCHEMA[method];
    if (!schema) return;
    const problems = validate(schema, params, "sender");
    if (problems.length > 0) throw new ProtocolError("AWP_MALFORMED", `outgoing ${method} params: ${problems.join("; ")}`);
  }

  /**
   * Sends a request; resolves with the result or rejects with AwpError / ConnectionLostError.
   *
   * `onResult` runs synchronously when the response is read, before any later message on the
   * connection is dispatched — state that later notifications depend on (grants, channel ids, the
   * admission of an action) must be applied there, not in a promise continuation.
   */
  request(
    method: string,
    params: unknown,
    timeoutMs?: number,
    onResult?: (result: unknown, info: ResponseInfo) => void,
  ): Promise<{ result: unknown; info: ResponseInfo }> {
    this.checkOutgoing(method, params);
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const sentAt = this.clock.now();
      const p: Pending = { method, sentAt, onResult, resolve, reject, timer: undefined };
      if (timeoutMs !== undefined) {
        p.timer = setTimeout(() => {
          this.pending.delete(id);
          reject(new RequestTimeoutError(method, timeoutMs));
        }, timeoutMs);
      }
      this.pending.set(id, p);
      try {
        const msg: Record<string, unknown> = { jsonrpc: "2.0", id, method };
        if (params !== undefined) msg.params = params;
        this.sendRaw(msg);
        p.sentAt = this.clock.now();
      } catch (err) {
        this.pending.delete(id);
        if (p.timer) clearTimeout(p.timer);
        reject(err as Error);
      }
    });
  }

  /** Sends a notification (no id). */
  notify(method: string, params: unknown): void {
    this.checkOutgoing(method, params);
    const msg: Record<string, unknown> = { jsonrpc: "2.0", method };
    if (params !== undefined) msg.params = params;
    this.sendRaw(msg);
  }

  private respond(id: RequestId | null, body: { result: unknown } | { error: WireError }): void {
    try {
      this.sendRaw({ jsonrpc: "2.0", id, ...body });
    } catch {
      // Connection gone; nothing to answer on.
    }
  }

  private onText(text: string, receivedAt: number): void {
    let parsed: ReturnType<typeof parseJsonChecked>;
    try {
      parsed = parseJsonChecked(text);
    } catch {
      this.handlers.protocolWarning("unparseable JSON on the control connection");
      this.respond(null, { error: { code: JsonRpcCode.PARSE_ERROR, message: "Parse error" } });
      return;
    }
    const msg = parsed.value;
    this.options.trace?.("in", msg);
    if (Array.isArray(msg)) {
      // Batches are prohibited (AWP-CTL-006).
      this.handlers.protocolWarning("batch received on the control connection (AWP-CTL-006)");
      this.respond(null, { error: { code: JsonRpcCode.INVALID_REQUEST, message: "Invalid Request", data: { retryable: false, detail: "batch requests are not permitted (AWP-CTL-006)" } } });
      return;
    }
    if (msg === null || typeof msg !== "object") {
      this.handlers.protocolWarning("non-object JSON-RPC message");
      return;
    }
    const m = msg as Record<string, unknown>;
    if (parsed.outOfRange.length > 0) {
      const detail = `integer beyond 2^53 − 1 in field(s) ${parsed.outOfRange.join(", ")}`;
      if (typeof m.method === "string" && m.id !== undefined && m.id !== null) {
        this.respond(m.id as RequestId, {
          error: { code: 2006, message: "AWP_INTEGER_RANGE", data: { retryable: false, detail } },
        });
      }
      this.handlers.integerRange(detail);
      return;
    }
    const hasId = m.id !== undefined && m.id !== null;
    if (typeof m.method === "string") {
      if (hasId) this.onRequest({ id: m.id as RequestId, method: m.method, params: m.params, receivedAt });
      else this.handlers.notification({ method: m.method, params: m.params, receivedAt });
      return;
    }
    if (hasId && ("result" in m || "error" in m)) {
      const p = this.pending.get(m.id as RequestId);
      if (!p) {
        this.handlers.protocolWarning(`response to unknown id ${String(m.id)}`);
        return;
      }
      this.pending.delete(m.id as RequestId);
      if (p.timer) clearTimeout(p.timer);
      if ("error" in m && m.error !== null && typeof m.error === "object") {
        p.reject(new AwpError(m.error as WireError, p.method));
      } else {
        const info = { receivedAt, sentAt: p.sentAt };
        try {
          p.onResult?.(m.result, info);
        } catch (err) {
          p.reject(err as Error);
          return;
        }
        p.resolve({ result: m.result, info });
      }
      return;
    }
    if ("error" in m) {
      this.handlers.protocolWarning(`error without a request id: ${JSON.stringify(m.error)}`);
      return;
    }
    this.handlers.protocolWarning("message is neither a request, a notification, nor a response");
  }

  private onRequest(req: IncomingRequest): void {
    let handled: { result: unknown } | undefined;
    try {
      handled = this.handlers.request(req);
    } catch (err) {
      const wire: WireError =
        err instanceof AwpError
          ? { code: err.code, message: err.errorName, data: { retryable: err.retryable, ...(err.detail ? { detail: err.detail } : {}) } }
          : err instanceof ProtocolError
            ? { code: err.code, message: err.errorName, data: { retryable: false, detail: err.message } }
            : { code: JsonRpcCode.INTERNAL_ERROR, message: "Internal error", data: { retryable: false, detail: String(err) } };
      this.respond(req.id, { error: wire });
      return;
    }
    if (handled === undefined) {
      this.respond(req.id, { error: { code: JsonRpcCode.METHOD_NOT_FOUND, message: "Method not found" } });
      return;
    }
    this.respond(req.id, handled);
  }

  close(code = 1000, reason = ""): void {
    if (this.ws.readyState === this.ws.OPEN || this.ws.readyState === this.ws.CONNECTING) {
      try {
        this.ws.close(code, reason);
      } catch {
        this.ws.terminate();
      }
    }
    this.failAll(new ConnectionLostError("control connection closed"));
  }

  terminate(): void {
    this.ws.terminate();
    this.failAll(new ConnectionLostError("control connection terminated"));
  }
}
