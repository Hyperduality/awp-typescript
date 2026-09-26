/**
 * The world manifest (spec/session/world-manifest): validation against the canonical schema plus the
 * cross-reference rules the schema cannot express, and typed accessors.
 *
 * An agent MUST refuse to open a session against a manifest it cannot validate (AWP-AGT-002).
 */
import type { AnySchemaObject, ValidateFunction } from "ajv/dist/2020.js";
import { ManifestInvalidError } from "./errors.ts";
import { compileEmbedded, formatErrors, validate } from "./schemas.ts";
import type { ChannelDecl, ActionSchemaDecl, EmbodimentDecl, WorldManifest, TimeModel, PreemptionPolicy } from "./types.ts";

export interface ManifestCheck {
  valid: boolean;
  problems: string[];
}

/**
 * Checks a manifest received from `initialize` or `world.manifest`:
 * - the canonical schema in receiver form (unknown fields tolerated, AWP-VER-003), which also checks
 *   every `params_schema` against the JSON Schema 2020-12 meta-schema (AWP-MAN-002);
 * - `protocol_version` is one this agent offered (AWP-VER-002, AWP-VER-008);
 * - every channel and action type an embodiment references is declared (AWP-MAN-001);
 * - declared ids are unique, and every `params_schema` compiles with its local `$defs` resolved.
 */
export function checkManifest(manifest: unknown, offeredVersions: readonly string[]): ManifestCheck {
  const problems = validate("world-manifest", manifest, "receiver");
  if (problems.length > 0) return { valid: false, problems };
  const m = manifest as WorldManifest;
  if (!offeredVersions.includes(m.protocol_version)) {
    problems.push(`protocol_version ${JSON.stringify(m.protocol_version)} was not offered (offered ${offeredVersions.join(", ")})`);
  }
  const dup = (kind: string, ids: string[]) => {
    const seen = new Set<string>();
    for (const id of ids) {
      if (seen.has(id)) problems.push(`duplicate ${kind} ${JSON.stringify(id)}`);
      seen.add(id);
    }
    return seen;
  };
  const channels = dup("channel id", [...m.observation_channels, ...(m.command_channels ?? [])].map((c) => c.id));
  const actions = dup("action type", m.action_schemas.map((a) => a.type));
  dup("embodiment id", m.embodiments.map((e) => e.id));
  for (const e of m.embodiments) {
    for (const c of e.channels) if (!channels.has(c)) problems.push(`embodiment ${e.id} references undeclared channel ${c} (AWP-MAN-001)`);
    for (const a of e.action_types) if (!actions.has(a)) problems.push(`embodiment ${e.id} references undeclared action type ${a} (AWP-MAN-001)`);
  }
  const commandIds = new Set((m.command_channels ?? []).map((c) => c.id));
  for (const a of m.action_schemas) {
    if (a.command_channel !== undefined && !commandIds.has(a.command_channel)) {
      problems.push(`action type ${a.type} names undeclared command channel ${a.command_channel} (AWP-CMD-002)`);
    }
    try {
      compileParamsSchema(m, a);
    } catch (err) {
      problems.push(`params_schema of ${a.type} does not compile: ${(err as Error).message} (AWP-MAN-002)`);
    }
  }
  return { valid: problems.length === 0, problems };
}

/** Throws ManifestInvalidError unless the manifest is valid. */
export function assertManifest(manifest: unknown, offeredVersions: readonly string[]): WorldManifest {
  const check = checkManifest(manifest, offeredVersions);
  if (!check.valid) throw new ManifestInvalidError(check.problems);
  return manifest as WorldManifest;
}

let embeddedCounter = 0;

/**
 * Compiles a type's `params_schema` with local `#/$defs/...` references resolved against the manifest's
 * own `$defs` ("Local `$ref`s inside `params_schema` resolve against the manifest's own `$defs`").
 */
export function compileParamsSchema(m: WorldManifest, a: ActionSchemaDecl): ValidateFunction {
  const own = (a.params_schema ?? {}) as AnySchemaObject;
  const defs = { ...((m.$defs ?? {}) as Record<string, unknown>), ...((own.$defs ?? {}) as Record<string, unknown>) };
  const wrapped: AnySchemaObject = {
    ...own,
    $id: `urn:awp:manifest:${++embeddedCounter}:${a.type}`,
    $defs: defs,
  };
  return compileEmbedded(wrapped);
}

/** Typed, indexed view of a validated manifest. */
export class Manifest {
  readonly raw: WorldManifest;
  private readonly channels = new Map<string, ChannelDecl>();
  private readonly actions = new Map<string, ActionSchemaDecl>();
  private readonly paramsValidators = new Map<string, ValidateFunction>();

  constructor(raw: WorldManifest) {
    this.raw = raw;
    for (const c of raw.observation_channels) this.channels.set(c.id, c);
    for (const c of raw.command_channels ?? []) this.channels.set(c.id, c);
    for (const a of raw.action_schemas) this.actions.set(a.type, a);
  }

  get timeModels(): TimeModel[] {
    return this.raw.time_models;
  }

  get embodiments(): EmbodimentDecl[] {
    return this.raw.embodiments;
  }

  embodiment(id: string): EmbodimentDecl | undefined {
    return this.raw.embodiments.find((e) => e.id === id);
  }

  channel(id: string): ChannelDecl | undefined {
    return this.channels.get(id);
  }

  actionSchema(type: string): ActionSchemaDecl | undefined {
    return this.actions.get(type);
  }

  /** Declared preemption policies of a type (a string or a list in the manifest, AWP-PRE-001). */
  preemptionPolicies(type: string): PreemptionPolicy[] {
    const p = this.actions.get(type)?.preemption;
    if (p === undefined) return [];
    return Array.isArray(p) ? p : [p];
  }

  /** Validates params against the type's `params_schema`; returns the problems. */
  checkParams(type: string, params: unknown): string[] {
    const decl = this.actions.get(type);
    if (!decl) return [`unknown action type ${type}`];
    let fn = this.paramsValidators.get(type);
    if (!fn) {
      fn = compileParamsSchema(this.raw, decl);
      this.paramsValidators.set(type, fn);
    }
    return fn(params) ? [] : formatErrors(fn.errors);
  }

  /** Longest declared `max_abort_ms` (AWP-SES-011: agents SHOULD allow it when awaiting session.close). */
  get longestMaxAbortMs(): number {
    return Math.max(0, ...this.raw.action_schemas.map((a) => a.max_abort_ms ?? 0));
  }

  /** Who may advance a lockstep world (AWP-TIM-012); present whenever lockstep is offered. */
  get tickAuthority(): "any_session" | "barrier" | undefined {
    return this.raw.tick_authority;
  }

  get watchdogMs(): number | undefined {
    return this.raw.safety_policy.safe_state?.watchdog_ms;
  }
}
