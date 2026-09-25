/** AWP error registry (api-reference/error-codes) and the SDK's error types. */

export const ErrorCode = {
  AWP_VERSION_UNSUPPORTED: 1001,
  AWP_MALFORMED: 1002,
  AWP_EMBODIMENT_UNAVAILABLE: 2001,
  AWP_TIME_MODEL_UNSUPPORTED: 2002,
  AWP_SESSION_EXPIRED: 2003,
  AWP_SESSION_EXISTS: 2004,
  AWP_SESSION_UNKNOWN: 2005,
  AWP_INTEGER_RANGE: 2006,
  AWP_CHANNEL_UNKNOWN: 2007,
  AWP_PARAMS_INVALID: 3001,
  AWP_BUSY: 3002,
  AWP_QUEUE_FULL: 3003,
  AWP_ACTION_ID_CONFLICT: 3004,
  AWP_ESTOP_ACTIVE: 3005,
  AWP_TICK_NOT_AUTHORIZED: 3006,
  AWP_STALE_INTENT: 3007,
  AWP_ACTION_UNKNOWN: 3008,
  AWP_TICK_MISMATCH: 3009,
  AWP_FORBIDDEN: 4001,
  AWP_ENVELOPE_EXCEEDED: 4002,
  AWP_APPROVAL_DENIED: 4003,
  AWP_APPROVAL_TIMEOUT: 4004,
} as const;

export type ErrorName = keyof typeof ErrorCode;

/** JSON-RPC 2.0 reserved codes. */
export const JsonRpcCode = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL_ERROR: -32603,
} as const;

const NAME_BY_CODE = new Map<number, ErrorName>(
  Object.entries(ErrorCode).map(([name, code]) => [code, name as ErrorName]),
);

export function errorName(code: number): ErrorName | undefined {
  return NAME_BY_CODE.get(code);
}

/** The JSON-RPC error object as it travels on the wire (schemas/v0.1/error.schema.json). */
export interface WireError {
  code: number;
  message: string;
  data?: { retryable?: boolean; retry_after_ms?: number; detail?: string; [k: string]: unknown };
}

/** An error answered by the world (a JSON-RPC error object). */
export class AwpError extends Error {
  readonly code: number;
  readonly data: WireError["data"];
  readonly method: string | undefined;

  constructor(error: WireError, method?: string) {
    const name = errorName(error.code) ?? error.message;
    super(`${name}${error.data?.detail ? `: ${error.data.detail}` : ""}`);
    this.name = "AwpError";
    this.code = error.code;
    this.data = error.data;
    this.method = method;
  }

  /** Registry name, e.g. `AWP_BUSY`, or the JSON-RPC message for reserved codes. */
  get errorName(): string {
    return errorName(this.code) ?? this.message;
  }

  /**
   * Whether the request may be retried with identical parameters (AWP-ERR-001). JSON-RPC reserved
   * errors are non-retryable unless `data` says otherwise.
   */
  get retryable(): boolean {
    return this.data?.retryable === true;
  }

  get retryAfterMs(): number | undefined {
    return typeof this.data?.retry_after_ms === "number" ? this.data.retry_after_ms : undefined;
  }

  get detail(): string | undefined {
    return typeof this.data?.detail === "string" ? this.data.detail : undefined;
  }
}

/** A protocol violation detected locally, carrying the AWP code it maps to (e.g. AWP_MALFORMED). */
export class ProtocolError extends Error {
  readonly code: number;
  constructor(code: ErrorName, message: string) {
    super(`${code}: ${message}`);
    this.name = "ProtocolError";
    this.code = ErrorCode[code];
  }
  get errorName(): string {
    return errorName(this.code) ?? "AWP_MALFORMED";
  }
}

/** The control connection dropped before a response arrived. */
export class ConnectionLostError extends Error {
  constructor(message = "control connection lost") {
    super(message);
    this.name = "ConnectionLostError";
  }
}

/** The world's manifest failed validation; the SDK refuses to open a session against it (AWP-AGT-002). */
export class ManifestInvalidError extends Error {
  readonly problems: string[];
  constructor(problems: string[]) {
    super(`world manifest is invalid: ${problems.slice(0, 5).join("; ")}${problems.length > 5 ? "; …" : ""}`);
    this.name = "ManifestInvalidError";
    this.problems = problems;
  }
}

/** A local usage error: the SDK refuses to send something the specification forbids the agent to send. */
export class UsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UsageError";
  }
}

/** The session is gone (closed, expired, or unknown to the world, AWP-SES-008). */
export class SessionClosedError extends Error {
  readonly reason: string;
  constructor(reason: string) {
    super(`session closed: ${reason}`);
    this.name = "SessionClosedError";
    this.reason = reason;
  }
}

/** No response arrived within the caller's deadline (the connection may still be open). */
export class RequestTimeoutError extends Error {
  readonly method: string;
  constructor(method: string, timeoutMs: number) {
    super(`${method} got no response within ${timeoutMs} ms`);
    this.name = "RequestTimeoutError";
    this.method = method;
  }
}
