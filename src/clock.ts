/**
 * The agent clock and the clock-offset estimate (spec/semantics/timestamps-and-clocks).
 *
 * The agent clock is a monotonic nanosecond counter starting at 0 when the SDK creates it (AWP-CLK-006).
 * Each ping exchange yields `rtt_ns = (destination − origin) − (transmit − receive)` and
 * `offset_ns = ((receive − origin) + (transmit − destination)) / 2`, so `session = agent + offset`.
 * The estimate is the smallest-RTT sample among the most recent eight exchanges; its error bound is
 * half that RTT (AWP-CLK-008).
 */

export interface Clock {
  /** Nanoseconds on the agent's monotonic clock (≥ 0, ≤ 2^53 − 1). */
  now(): number;
}

export class MonotonicClock implements Clock {
  private readonly origin = process.hrtime.bigint();
  now(): number {
    return Number(process.hrtime.bigint() - this.origin);
  }
}

/**
 * The process-wide agent clock. Every client created without an explicit clock shares it, so the agent
 * clock never goes backward when an agent opens a new session after losing one (AWP-CLK-006).
 */
export const agentClock: Clock = new MonotonicClock();

export interface ClockSample {
  origin_ns: number;
  receive_ns: number;
  transmit_ns: number;
  destination_ns: number;
  rtt_ns: number;
  offset_ns: number;
}

export function clockSample(origin_ns: number, receive_ns: number, transmit_ns: number, destination_ns: number): ClockSample {
  const rtt_ns = destination_ns - origin_ns - (transmit_ns - receive_ns);
  const offset_ns = Math.round((receive_ns - origin_ns + (transmit_ns - destination_ns)) / 2);
  return { origin_ns, receive_ns, transmit_ns, destination_ns, rtt_ns, offset_ns };
}

export const OFFSET_WINDOW = 8;

export class OffsetEstimator {
  private recent: ClockSample[] = [];
  /** Exchanges completed in this session (all of them, not only the retained window). */
  samples = 0;

  /** Records a session-clock exchange. Callers MUST NOT pass pre-session exchanges (AWP-SES-012). */
  add(s: ClockSample): void {
    if (s.rtt_ns < 0) return; // impossible sample (clock misuse); never let it win the minimum
    this.recent.push(s);
    if (this.recent.length > OFFSET_WINDOW) this.recent.shift();
    this.samples++;
  }

  reset(): void {
    this.recent = [];
    this.samples = 0;
  }

  /** The current estimate, or undefined before the first exchange. */
  get best(): ClockSample | undefined {
    let best: ClockSample | undefined;
    for (const s of this.recent) if (!best || s.rtt_ns < best.rtt_ns) best = s;
    return best;
  }

  get offsetNs(): number | undefined {
    return this.best?.offset_ns;
  }

  /** Error bound of the estimate: rtt / 2 of the selected sample. */
  get errorBoundNs(): number | undefined {
    const b = this.best;
    return b ? b.rtt_ns / 2 : undefined;
  }

  /** Maps an agent-clock instant to the session clock (AWP-CLK-009). */
  toSession(agentNs: number): number {
    const o = this.offsetNs;
    if (o === undefined) throw new Error("no clock-offset estimate yet (AWP-CLK-008)");
    return Math.round(agentNs + o);
  }

  toAgent(sessionNs: number): number {
    const o = this.offsetNs;
    if (o === undefined) throw new Error("no clock-offset estimate yet (AWP-CLK-008)");
    return Math.round(sessionNs - o);
  }
}
