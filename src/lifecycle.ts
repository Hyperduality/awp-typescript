/**
 * The action lifecycle (spec/loop/action-lifecycle, spec/action-lifecycle.yaml vendored as
 * schemas/action-lifecycle.json) and the agent-side record of each action.
 *
 * The record applies transitions in `status_seq` order, deduplicates redelivered notifications
 * (AWP-LIF-009), checks every transition against the normative table (AWP-LIF-001), and never
 * treats a second terminal status as a second terminal transition.
 */
import { readVendored } from "./schemas.ts";
import type { ActionState, ActionStatus, ActionSubmitParams } from "./types.ts";

export type StateClass = "transient" | "pre-execution" | "execution" | "terminal";

interface Transition {
  from: string;
  to: string;
  label: string;
  reasons?: string[];
  wire?: string;
}

interface LifecycleTable {
  states: Record<string, StateClass>;
  transitions: Transition[];
}

const TABLE = readVendored("action-lifecycle.json") as LifecycleTable;
const EDGES = new Map<string, Transition>(TABLE.transitions.map((t) => [`${t.from}>${t.to}`, t]));

export const STATE_CLASS: Readonly<Record<string, StateClass>> = TABLE.states;

export function stateClass(state: string): StateClass | undefined {
  return TABLE.states[state];
}

export function isTerminal(state: string): boolean {
  return TABLE.states[state] === "terminal";
}

export function isPreExecution(state: string): boolean {
  return TABLE.states[state] === "pre-execution";
}

/** The table's transition from → to, if listed. `submitted` is the implicit start. */
export function transition(from: string, to: string): Transition | undefined {
  return EDGES.get(`${from}>${to}`);
}

/**
 * Whether `from → to` is a legal path through the table (possibly through intermediate states the
 * agent never saw, e.g. after a gap). Returns the shortest path length or undefined.
 */
export function reachable(from: string, to: string): number | undefined {
  if (from === to) return 0;
  const queue: [string, number][] = [[from, 0]];
  const seen = new Set([from]);
  while (queue.length > 0) {
    const [s, d] = queue.shift()!;
    for (const t of TABLE.transitions) {
      if (t.from !== s || seen.has(t.to)) continue;
      if (t.to === to) return d + 1;
      seen.add(t.to);
      queue.push([t.to, d + 1]);
    }
  }
  return undefined;
}

/** Checks a reason against the registry constraints of the transition (vendor reasons always pass). */
export function reasonAllowed(from: string, to: string, reason: string | undefined): boolean {
  const t = transition(from, to);
  if (!t) return false;
  if (reason === undefined || reason.startsWith("x-")) return true;
  if (!t.reasons) return true;
  return t.reasons.includes(reason);
}

export interface StatusUpdate {
  state: ActionState;
  status_seq: number;
  ts_mono_ns?: number;
  reason?: string;
  progress?: number;
  detail?: string;
  source: "submit" | "cancel" | "status" | "pull";
  raw?: ActionStatus | Record<string, unknown>;
}

export type ApplyOutcome =
  | { kind: "applied"; from: ActionState | "submitted"; to: ActionState; legal: boolean; note?: string }
  | { kind: "duplicate" }
  | { kind: "stale" }
  | { kind: "after_terminal"; note: string };

/** Everything the agent knows about one action it submitted. */
export class ActionRecord {
  readonly action_id: string;
  /** The exact submission, reused unchanged for idempotent resubmission (AWP-ACT-001, AWP-ACT-009). */
  readonly submission: ActionSubmitParams;
  state: ActionState | "submitted" = "submitted";
  statusSeq = 0;
  reason: string | undefined;
  detail: string | undefined;
  progress: number | undefined;
  received_ts_mono_ns: number | undefined;
  /** Whether the world has acknowledged the action (admission result or a status for it). */
  admitted = false;
  /** Whether the submission was refused with a JSON-RPC error (no action exists, AWP-ACT-010). */
  refused = false;
  /** Whether the session ended before a terminal status was seen. */
  lost = false;
  /** Every applied update, in `status_seq` order. */
  readonly history: StatusUpdate[] = [];
  /** Transitions that were not legal per the table (world-side violations, kept for diagnostics). */
  readonly violations: string[] = [];
  /** Agent clock (ns) when the submission was transmitted, for decision latency (AWP-OBS-007). */
  transmittedAt: number | undefined;
  private readonly seen = new Set<number>();
  private waiters: { predicate: (r: ActionRecord) => boolean; resolve: (r: ActionRecord) => void }[] = [];

  constructor(submission: ActionSubmitParams) {
    this.action_id = submission.action_id;
    this.submission = submission;
  }

  get terminal(): boolean {
    return isTerminal(this.state);
  }

  /**
   * Applies a status. Updates are deduplicated on `status_seq`; an update older than the newest one
   * applied is recorded as stale (the world may deliver a result and later notifications out of the
   * order they were assigned in only across connections); a status after the terminal one is ignored.
   */
  apply(u: StatusUpdate): ApplyOutcome {
    if (this.seen.has(u.status_seq)) return { kind: "duplicate" };
    this.seen.add(u.status_seq);
    this.admitted = true;
    if (u.status_seq < this.statusSeq) return { kind: "stale" };
    if (this.terminal) {
      const note = `status ${u.state} (status_seq ${u.status_seq}) after terminal ${this.state}; not a second terminal transition (AWP-LIF-009)`;
      this.violations.push(note);
      return { kind: "after_terminal", note };
    }
    const from = this.state;
    let legal = true;
    let note: string | undefined;
    if (from === u.state) {
      // Progress updates repeat the state (e.g. executing with a new progress value).
      legal = true;
    } else if (!transition(from, u.state)) {
      const hops = reachable(from, u.state);
      legal = hops !== undefined && u.source !== "status";
      note = hops === undefined ? `illegal transition ${from} → ${u.state} (AWP-LIF-001)` : `transition ${from} → ${u.state} skips intermediate states`;
      if (u.source === "status" && hops !== undefined) {
        // Notifications must report every transition after admission; a skip means a missed status.
        legal = false;
      }
      this.violations.push(note);
    } else if (!reasonAllowed(from, u.state, u.reason)) {
      note = `reason ${u.reason} not permitted on ${from} → ${u.state}`;
      this.violations.push(note);
    }
    this.state = u.state;
    this.statusSeq = u.status_seq;
    if (u.reason !== undefined) this.reason = u.reason;
    if (u.detail !== undefined) this.detail = u.detail;
    if (u.progress !== undefined) this.progress = u.progress;
    this.history.push(u);
    this.flush();
    return note === undefined ? { kind: "applied", from, to: u.state, legal } : { kind: "applied", from, to: u.state, legal, note };
  }

  markRefused(): void {
    this.refused = true;
    this.flush();
  }

  /**
   * The session ended without this action reaching a reported terminal state (e.g. AWP_SESSION_UNKNOWN,
   * AWP-SES-008): nothing about it may be assumed to survive.
   */
  markLost(): void {
    if (this.terminal) return;
    this.lost = true;
    this.flush();
  }

  /** Resolves once the predicate holds (checked on every applied update). */
  until(predicate: (r: ActionRecord) => boolean): Promise<ActionRecord> {
    if (predicate(this)) return Promise.resolve(this);
    return new Promise((resolve) => this.waiters.push({ predicate, resolve }));
  }

  /** Resolves once the action is terminal, was refused at admission, or was lost with its session. */
  settled(): Promise<ActionRecord> {
    return this.until((r) => r.terminal || r.refused || r.lost);
  }

  private flush(): void {
    const keep: typeof this.waiters = [];
    for (const w of this.waiters) {
      if (w.predicate(this)) w.resolve(this);
      else keep.push(w);
    }
    this.waiters = keep;
  }
}
