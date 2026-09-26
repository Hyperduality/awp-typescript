/**
 * The `ws` stream binding (spec/transport/overview, api-reference/data/stream-negotiation): one binary
 * WebSocket message per frame in the binary envelope, authenticated with the session token as a bearer
 * credential and the `awp` subprotocol, never with a credential in the URL (AWP-SEC-004..006, AWP-TRN-013).
 */
import WebSocket from "ws";
import { ErrorCode, ProtocolError } from "./errors.ts";
import { decodeBinaryFrame, type Frame } from "./frames.ts";
import type { Clock } from "./clock.ts";
import type { StreamEndpoint } from "./types.ts";
import { AWP_SUBPROTOCOL } from "./version.ts";

export const DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;
/** WebSocket close code used with an AWP reason (the specification names the reason, not the code). */
export const PROTOCOL_ERROR_CLOSE = 1002;

export interface StreamHandlers {
  frame(frame: Frame, receivedAt: number, connection: number): void;
  /** The stream closed; `byUs` when this side closed it intentionally (not for a protocol violation). */
  closed(stream: StreamConnection, code: number, reason: string, byUs: boolean): void;
  integerRange(detail: string): void;
  warning(detail: string): void;
}

export class StreamConnection {
  readonly endpoint: StreamEndpoint;
  readonly connectionId: number;
  private ws: WebSocket | undefined;
  /** Set once the WebSocket opened; a channel moves to this connection from then on (AWP-TRN-012). */
  established = false;
  private closedByUs = false;
  private readonly clock: Clock;
  private readonly handlers: StreamHandlers;

  constructor(endpoint: StreamEndpoint, connectionId: number, clock: Clock, handlers: StreamHandlers) {
    this.endpoint = endpoint;
    this.connectionId = connectionId;
    this.clock = clock;
    this.handlers = handlers;
  }

  /** Connects with the session token (AWP-SEC-005); resolves once the WebSocket is open. */
  open(sessionToken: string, timeoutMs: number): Promise<void> {
    const url = this.endpoint.url;
    if (!url) return Promise.reject(new Error("ws stream endpoint without url"));
    const u = new URL(url);
    if (u.username || u.password) {
      // Credentials never appear in URLs (AWP-SEC-006).
      return Promise.reject(new Error("stream endpoint URL carries userinfo credentials (AWP-SEC-006)"));
    }
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url, [AWP_SUBPROTOCOL], {
        headers: { Authorization: `Bearer ${sessionToken}` },
        handshakeTimeout: timeoutMs,
        perMessageDeflate: false,
        maxPayload: this.endpoint.max_frame_bytes ?? DEFAULT_MAX_FRAME_BYTES,
      });
      this.ws = ws;
      let opened = false;
      ws.once("open", () => {
        opened = true;
        this.established = true;
        resolve();
      });
      ws.once("unexpected-response", (_req, res) => reject(new Error(`stream endpoint answered HTTP ${res.statusCode}`)));
      ws.on("error", (err) => {
        if (!opened) reject(err);
      });
      ws.on("message", (data: Buffer | ArrayBuffer | Buffer[], isBinary: boolean) => {
        const receivedAt = this.clock.now();
        if (!isBinary) {
          this.fail("AWP_MALFORMED", "text message on a stream connection (AWP-TRN-013)");
          return;
        }
        const buf = Array.isArray(data) ? Buffer.concat(data) : Buffer.isBuffer(data) ? data : Buffer.from(data);
        let frame: Frame;
        try {
          frame = decodeBinaryFrame(new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength));
        } catch (err) {
          if (err instanceof ProtocolError && err.code === ErrorCode.AWP_INTEGER_RANGE) {
            this.fail("AWP_INTEGER_RANGE", err.message);
            this.handlers.integerRange(err.message);
          } else {
            this.fail("AWP_MALFORMED", (err as Error).message);
          }
          return;
        }
        this.handlers.frame(frame, receivedAt, this.connectionId);
      });
      ws.on("close", (code, reason) => {
        if (!opened) reject(new Error(`stream connection closed during handshake (${code})`));
        this.handlers.closed(this, code, reason.toString(), this.closedByUs);
      });
    });
  }

  /** Closes the stream connection because a message was not a valid frame (AWP-TRN-013, AWP-DAT-006). */
  private fail(reason: "AWP_MALFORMED" | "AWP_INTEGER_RANGE", detail: string): void {
    this.handlers.warning(`closing stream connection ${this.endpoint.url}: ${reason}: ${detail}`);
    // Not an intentional close: the owner may reconnect, and the world restarts with a resync keyframe.
    this.shut(PROTOCOL_ERROR_CLOSE, reason);
  }

  /** Intentional close; the owner does not reconnect. */
  close(code = 1000, reason = ""): void {
    this.closedByUs = true;
    this.shut(code, reason);
  }

  private shut(code: number, reason: string): void {
    const ws = this.ws;
    if (!ws) return;
    if (ws.readyState === ws.OPEN || ws.readyState === ws.CONNECTING) {
      try {
        ws.close(code, reason);
      } catch {
        ws.terminate();
      }
    }
  }

  get isOpen(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  /** Whether this endpoint carries a channel (an endpoint without `channels` carries every channel). */
  carries(channelId: number): boolean {
    return this.endpoint.channels === undefined || this.endpoint.channels.includes(channelId);
  }

  get bufferedAmount(): number {
    return this.ws?.bufferedAmount ?? 0;
  }

  /** Sends one binary frame (agent→world command frames, AWP-TRN-013). */
  send(frame: Uint8Array): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) throw new Error("stream connection is not open");
    const limit = this.endpoint.max_frame_bytes ?? DEFAULT_MAX_FRAME_BYTES;
    if (frame.length > limit) throw new Error(`frame of ${frame.length} bytes exceeds max_frame_bytes ${limit} (AWP-TRN-011)`);
    this.ws.send(frame, { binary: true });
  }
}
