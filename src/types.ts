/** Wire types for AWP 0.1 (schemas/v0.1). Unknown fields may be present on anything the world sends. */

export type TimeModel = "lockstep" | "streaming";
export type LossClass = "reliable" | "latest-wins";
export type PreemptionPolicy = "queue" | "replace" | "blend" | "reject";
export type AdminOperation = "snapshot" | "restore" | "reset" | "tick";

export type ActionState =
  | "pending_approval"
  | "queued"
  | "accepted"
  | "executing"
  | "cancelling"
  | "rejected"
  | "completed"
  | "failed"
  | "preempted"
  | "cancelled";

export type SessionStateName = "ready" | "active" | "suspended" | "closed";

export interface AgentInfo {
  name: string;
  version: string;
  vendor: string;
}

export interface AgentManifest {
  protocol_versions: string[];
  agent: AgentInfo;
  consumes_modalities: string[];
  time_models?: TimeModel[];
  max_obs_rate_hz?: number;
  extensions?: Record<string, unknown>;
}

export interface ChannelDecl {
  id: string;
  modality: string;
  rate_hz: number | null;
  loss_class: LossClass;
  keyframe_interval?: number;
  stale_after_ms?: number;
  schema: Record<string, unknown>;
  [k: string]: unknown;
}

export interface EmbodimentDecl {
  id: string;
  kind: string;
  shared_control?: boolean;
  multi_bind_group?: string;
  action_types: string[];
  channels: string[];
  [k: string]: unknown;
}

export interface ActionSchemaDecl {
  type: string;
  params_schema: Record<string, unknown>;
  duration: "instant" | "extended" | "streaming";
  preemption: PreemptionPolicy | PreemptionPolicy[];
  concurrency_group?: string;
  max_queue?: number;
  requires_approval?: boolean;
  command_channel?: string;
  watchdog_ms?: number;
  max_abort_ms?: number;
  max_duration_ms?: number;
  description?: string;
  [k: string]: unknown;
}

export interface Envelope {
  embodiment: string;
  spatial?: { frame: string; aabb_m: [number[], number[]] };
  max_velocity_mps?: number;
  max_force_n?: number;
  max_action_rate_hz?: number;
  enforcement?: "command_check" | "measured" | "both";
  on_violation: "clamp" | "reject";
  [k: string]: unknown;
}

export interface SafetyPolicy {
  envelopes: Envelope[];
  safe_state?: { behavior: string; watchdog_ms: number };
  max_basis_age_ms?: number;
  approval_timeout_ms?: number;
  [k: string]: unknown;
}

export interface WorldManifest {
  protocol_version: string;
  world: { name: string; version: string; vendor: string };
  time_models: TimeModel[];
  tick_policy?: "on_tick";
  tick_authority?: "any_session" | "barrier";
  capabilities?: Record<string, unknown>;
  initial_states?: string[];
  embodiments: EmbodimentDecl[];
  observation_channels: ChannelDecl[];
  command_channels?: ChannelDecl[];
  action_schemas: ActionSchemaDecl[];
  safety_policy: SafetyPolicy;
  $defs?: Record<string, unknown>;
  extensions?: Record<string, unknown>;
  [k: string]: unknown;
}

export interface ChannelGrant {
  channel: string;
  rate_hz: number | null;
  channel_id: number;
}

export interface StreamEndpoint {
  binding: "inline" | "ws" | "grpc" | "webrtc" | "webtransport" | "shm";
  url?: string;
  signaling?: string;
  channels?: number[];
  max_frame_bytes?: number;
  [k: string]: unknown;
}

export interface FrameTree {
  frames: { id: string; parent: string | null; dynamic?: boolean; transform?: { p_m: number[]; q: number[] } }[];
}

export interface SessionReady {
  session_id: string;
  session_token: string;
  reconnect_window_ms: number;
  heartbeat_interval_ms: number;
  replay_to_status_seq?: number;
  granted: {
    channels: ChannelGrant[];
    action_types: string[];
    admin?: AdminOperation[];
    envelopes: Envelope[];
    expires_at?: number;
  };
  stream_endpoints: StreamEndpoint[];
  frame_tree?: FrameTree;
  clock_anchor?: string;
  tick?: number;
  safe_state?: boolean;
  [k: string]: unknown;
}

export interface SubscribeRequest {
  channel: string;
  rate_hz?: number;
}

export interface SessionOpenParams {
  mode: TimeModel;
  embodiment?: string;
  embodiments?: string[];
  subscribe?: SubscribeRequest[];
  action_types?: string[];
  admin?: AdminOperation[];
  seed?: number;
  takeover?: boolean;
  transfer_token?: string;
  task?: { content: { type: string; text?: string; [k: string]: unknown }[] };
}

export interface ActionSubmitParams {
  action_id: string;
  type: string;
  params: Record<string, unknown>;
  embodiment_id?: string;
  preempt?: PreemptionPolicy;
  deadline_ms?: number;
  basis_ts_mono_ns?: number;
  valid_until_ns?: number;
}

export interface ActionSubmitResult {
  action_id: string;
  state: ActionState;
  status_seq: number;
  received_ts_mono_ns: number;
  ts_mono_ns: number;
  reason?: string;
  [k: string]: unknown;
}

export interface ActionCancelResult {
  action_id: string;
  state: ActionState;
  status_seq: number;
  [k: string]: unknown;
}

export interface ActionStatus {
  action_id: string;
  state: ActionState;
  status_seq: number;
  ts_mono_ns: number;
  tick?: number;
  progress?: number;
  reason?: string;
  detail?: string;
  clamped?: boolean;
  blended?: boolean;
  aborted_at_progress?: number;
  stream?: { frames_applied: number; last_seq: number; clamped_count: number };
  [k: string]: unknown;
}

export interface WorldEvent {
  event: string;
  status_seq: number;
  ts_mono_ns: number;
  tick?: number;
  detail?: Record<string, unknown>;
  [k: string]: unknown;
}

export interface SessionStateNotification {
  state: SessionStateName;
  status_seq: number;
  ts_mono_ns: number;
  reason?: string;
  [k: string]: unknown;
}

export interface LatencyStats {
  count: number;
  p50: number;
  p95: number;
  max?: number;
}

export interface SessionTelemetry {
  window_ms: number;
  observation_latency_ns?: LatencyStats;
  admission_latency_ns?: LatencyStats;
  observation_to_action_ns?: LatencyStats;
  command_latency_ns?: LatencyStats;
  channels?: Record<string, LatencyStats>;
  [k: string]: unknown;
}

export interface PingParams {
  origin_ns: number;
  last_status_seq?: number;
}

export interface PongResult {
  origin_ns: number;
  receive_ns: number;
  transmit_ns: number;
}

export interface ObsReport {
  window_ms: number;
  sync: { offset_ns: number; rtt_ns: number; samples?: number };
  channels: Record<string, { frames: number; gaps: number; jitter_ns?: number; staleness_ns?: LatencyStats }>;
  decision_latency_ns?: LatencyStats;
}
