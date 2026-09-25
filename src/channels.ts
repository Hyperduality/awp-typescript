/**
 * Per-channel receive accounting: `seq` gaps and loss (AWP-DAT-001), resync (AWP-DAT-009), late frames
 * from another connection (AWP-TRN-012), and receiver-report statistics (AWP-OBS-007).
 *
 * Channels are independent: ordering holds within a channel and never across channels (AWP-TRN-007,
 * AWP-OBS-003), so each channel keeps its own state and interleaving across channels is irrelevant.
 */
import type { Frame } from "./frames.ts";
import type { LossClass, LatencyStats } from "./types.ts";

export type FrameVerdict =
  /** Process the frame. */
  | { kind: "accept"; gap: number; resync: boolean }
  /** Late frame from another connection; discard silently, not loss (AWP-TRN-012). */
  | { kind: "discard_late" }
  /** Non-increasing seq on the same connection: a sender violation; the frame is discarded. */
  | { kind: "discard_violation"; detail: string };

export interface ReceivedFrame {
  frame: Frame;
  /** Agent clock (ns) at receipt. */
  receivedAt: number;
  /** Connection the frame arrived on (the control connection for the inline binding). */
  connection: number;
}

/** Percentile summary in the `latency_stats` shape; undefined when there are no samples. */
export function latencyStats(samples: number[]): LatencyStats | undefined {
  if (samples.length === 0) return undefined;
  const s = samples.map((v) => Math.max(0, Math.round(v))).sort((a, b) => a - b);
  const pick = (q: number) => s[Math.min(s.length - 1, Math.max(0, Math.ceil(q * s.length) - 1))]!;
  return { count: s.length, p50: pick(0.5), p95: pick(0.95), max: s[s.length - 1]! };
}

export class ChannelTracker {
  readonly channelId: number;
  readonly lossClass: LossClass;
  lastSeq: number | undefined;
  private lastConnection: number | undefined;
  /** Frames received since subscription. */
  received = 0;
  /** Missing seq values not excused by a resync (AWP-DAT-001). */
  lost = 0;
  /** Missing seq values excused by a resync frame (AWP-DAT-009); never counted as loss. */
  resyncSkipped = 0;
  resyncs = 0;
  lateDiscarded = 0;
  violations = 0;
  /** Latest accepted frame. */
  latest: ReceivedFrame | undefined;
  /** Delta state accumulated since the last keyframe; cleared on resync (AWP-DAT-009). */
  deltaState: unknown[] = [];

  // Receiver-report window (AWP-OBS-007).
  private windowFrames = 0;
  private windowGaps = 0;
  private windowJitter: number[] = [];
  private windowStaleness: number[] = [];
  private prevTransit: { r: number; s: number } | undefined;

  constructor(channelId: number, lossClass: LossClass) {
    this.channelId = channelId;
    this.lossClass = lossClass;
  }

  /**
   * Classifies a frame. `offsetNs` (session − agent clock), when known, is used for staleness at receipt.
   */
  observe(rf: ReceivedFrame, offsetNs?: number): FrameVerdict {
    const { frame } = rf;
    let gap = 0;
    if (this.lastSeq !== undefined) {
      if (frame.seq <= this.lastSeq) {
        if (this.lastConnection !== undefined && rf.connection !== this.lastConnection) {
          this.lateDiscarded++;
          return { kind: "discard_late" };
        }
        this.violations++;
        return { kind: "discard_violation", detail: `seq ${frame.seq} after ${this.lastSeq} on channel ${this.channelId}` };
      }
      gap = frame.seq - this.lastSeq - 1;
    }
    if (frame.resync) {
      this.resyncs++;
      this.resyncSkipped += gap;
      this.deltaState = [];
      this.prevTransit = undefined;
    } else {
      this.lost += gap;
      this.windowGaps += gap;
    }
    if (frame.keyframe) this.deltaState = [];
    else this.deltaState.push(frame);
    this.lastSeq = frame.seq;
    this.lastConnection = rf.connection;
    this.received++;
    this.latest = rf;

    this.windowFrames++;
    if (frame.ts_send_ns !== undefined) {
      if (this.prevTransit) {
        const d = (rf.receivedAt - this.prevTransit.r) - (frame.ts_send_ns - this.prevTransit.s);
        this.windowJitter.push(Math.abs(d));
      }
      this.prevTransit = { r: rf.receivedAt, s: frame.ts_send_ns };
    }
    if (offsetNs !== undefined) this.windowStaleness.push(rf.receivedAt + offsetNs - frame.ts_mono_ns);
    return { kind: "accept", gap, resync: frame.resync };
  }

  /** Receiver-report entry for the window just ended, or undefined when no frame arrived; resets the window. */
  takeWindow(): { frames: number; gaps: number; jitter_ns?: number; staleness_ns?: LatencyStats } | undefined {
    if (this.windowFrames === 0 && this.windowGaps === 0) return undefined;
    const entry: { frames: number; gaps: number; jitter_ns?: number; staleness_ns?: LatencyStats } = {
      frames: this.windowFrames,
      gaps: this.windowGaps,
    };
    if (this.windowJitter.length > 0) {
      entry.jitter_ns = Math.round(this.windowJitter.reduce((a, b) => a + b, 0) / this.windowJitter.length);
    }
    const st = latencyStats(this.windowStaleness);
    if (st) entry.staleness_ns = st;
    this.windowFrames = 0;
    this.windowGaps = 0;
    this.windowJitter = [];
    this.windowStaleness = [];
    return entry;
  }
}
