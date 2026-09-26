/**
 * Canonical JSON Schemas (vendored from the specification repository under ./schemas) and validation.
 *
 * The canonical schemas are in receiver form; the sender form applies two annotations
 * (spec/versioning-policy, "Canonical schemas"):
 * - **receiver form** — used on everything the world sends. Unknown fields are accepted (AWP-VER-003) and
 *   both annotations are ignored.
 * - **sender form** — used on everything this SDK sends (AWP-VER-004). An object marked
 *   `x-awp-closed: true` admits only its declared properties and `x-<vendor>.` fields, and the
 *   constraints under `x-awp-lint` are added to the schema that holds them.
 */
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Ajv2020 } from "ajv/dist/2020.js";
import type { ErrorObject, ValidateFunction, AnySchemaObject } from "ajv/dist/2020.js";
import * as ajvFormats from "ajv-formats";

/** The package's `schemas/` directory: next to `src/` or `dist/`, found by walking up from this module. */
function findSchemaRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 6; i++) {
    const candidate = join(dir, "..", "schemas");
    if (existsSync(join(candidate, "v0.1"))) return candidate;
    dir = dirname(dir);
  }
  throw new Error("vendored schemas not found");
}

const SCHEMA_ROOT = findSchemaRoot();
export const SCHEMA_BASE = "https://agentworldprotocol.com/schemas/v0.1/";

export type SchemaForm = "receiver" | "sender";

function walk(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) out.push(...walk(p));
    else if (name.endsWith(".schema.json")) out.push(p);
  }
  return out.sort();
}

/** Reads a vendored artifact (relative to ./schemas). */
export function readVendored(rel: string): unknown {
  return JSON.parse(readFileSync(join(SCHEMA_ROOT, rel), "utf8"));
}

const VENDOR_PROPERTY = "^x-[a-z0-9]+\\.";

/** Derives the sender form of a schema (spec/versioning-policy, "Canonical schemas"). */
export function senderForm(node: unknown): unknown {
  if (Array.isArray(node)) return node.map(senderForm);
  if (node === null || typeof node !== "object") return node;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(node)) out[k] = senderForm(v);
  if (out["x-awp-closed"] === true) {
    const pattern = { ...((out["patternProperties"] as Record<string, unknown> | undefined) ?? {}) };
    if (!Object.keys(pattern).some((p) => p.startsWith("^x-"))) pattern[VENDOR_PROPERTY] = {};
    out["patternProperties"] = pattern;
    out["additionalProperties"] = false;
  }
  const lint = out["x-awp-lint"];
  if (lint !== null && typeof lint === "object") {
    out["allOf"] = [...((out["allOf"] as unknown[] | undefined) ?? []), lint];
  }
  return out;
}

const addFormats = ((ajvFormats as unknown as { default?: unknown }).default ?? ajvFormats) as (ajv: Ajv2020) => void;

function makeAjv(form: SchemaForm, schemas: AnySchemaObject[]): Ajv2020 {
  const ajv = new Ajv2020({ strict: false, allErrors: true, validateFormats: true });
  addFormats(ajv);
  ajv.addKeyword({ keyword: "x-awp-closed", schemaType: "boolean" });
  ajv.addKeyword({ keyword: "x-awp-lint" });
  for (const s of schemas) ajv.addSchema(form === "sender" ? (senderForm(s) as AnySchemaObject) : s);
  return ajv;
}

let loaded: { receiver: Ajv2020; sender: Ajv2020; names: string[] } | undefined;

function load() {
  if (loaded) return loaded;
  const files = walk(join(SCHEMA_ROOT, "v0.1"));
  const schemas = files.map((f) => JSON.parse(readFileSync(f, "utf8")) as AnySchemaObject);
  loaded = {
    receiver: makeAjv("receiver", schemas),
    sender: makeAjv("sender", schemas),
    names: schemas.map((s) => String(s.$id).slice(SCHEMA_BASE.length).replace(/\.schema\.json$/, "")),
  };
  return loaded;
}

/** Names of the vendored canonical schemas, e.g. `world-manifest`, `profiles/gui-actions`. */
export function schemaNames(): string[] {
  return load().names;
}

const cache = new Map<string, ValidateFunction>();

export function validator(name: string, form: SchemaForm): ValidateFunction {
  const key = `${form}:${name}`;
  let fn = cache.get(key);
  if (!fn) {
    const ajv = load()[form];
    fn = ajv.getSchema(`${SCHEMA_BASE}${name}.schema.json`);
    if (!fn) throw new Error(`unknown schema ${name}`);
    cache.set(key, fn);
  }
  return fn;
}

export function formatErrors(errors: ErrorObject[] | null | undefined): string[] {
  return (errors ?? []).map((e) => `${e.instancePath || "/"} ${e.message ?? e.keyword}${e.params && "additionalProperty" in e.params ? ` (${String(e.params.additionalProperty)})` : ""}`);
}

/** Validates `value` against a canonical schema; returns the problems (empty when valid). */
export function validate(name: string, value: unknown, form: SchemaForm): string[] {
  const fn = validator(name, form);
  return fn(value) ? [] : formatErrors(fn.errors);
}

/** Compiles a JSON Schema embedded in a manifest (e.g. a `params_schema`) against the canonical set. */
export function compileEmbedded(schema: AnySchemaObject): ValidateFunction {
  return load().receiver.compile(schema);
}

/**
 * Sender-form schema for each agent→world request or notification, keyed by method
 * (api-reference/control/*). Methods without a canonical params schema are absent.
 */
export const OUTGOING_PARAMS_SCHEMA: Readonly<Record<string, string>> = {
  initialize: "agent-manifest",
  "world.manifest": "empty-result",
  "session.open": "session-open",
  "session.resume": "session-resume",
  "session.close": "empty-result",
  "session.transfer": "session-transfer",
  ping: "ping",
  "action.submit": "action-submit",
  "action.cancel": "action-ref",
  "action.status": "action-ref",
  "obs.subscribe": "subscribe",
  "obs.unsubscribe": "unsubscribe",
  "obs.report": "obs-report",
  "cmd.frame": "frame-inline",
  "world.tick": "tick",
  "world.reset": "reset",
  "world.restore": "restore",
  "world.snapshot": "empty-result",
  "task.update": "task-update",
};

/** Receiver-form schema for each world→agent result, keyed by the request method. */
export const RESULT_SCHEMA: Readonly<Record<string, string>> = {
  initialize: "world-manifest",
  "world.manifest": "world-manifest",
  "session.open": "session-ready",
  "session.resume": "session-ready",
  "session.close": "empty-result",
  "session.transfer": "session-transfer-result",
  ping: "ping-result",
  "action.submit": "action-submit-result",
  "action.cancel": "action-cancel-result",
  "action.status": "action-status",
  "obs.subscribe": "subscribe-result",
  "obs.unsubscribe": "subscribe-result",
  "world.tick": "tick-result",
  "world.reset": "reset-result",
  "world.restore": "reset-result",
  "world.snapshot": "snapshot-result",
  "task.update": "empty-result",
};

/** Receiver-form schema for each world→agent notification checked against its schema, keyed by method. */
export const INCOMING_PARAMS_SCHEMA: Readonly<Record<string, string>> = {
  "action.status": "action-status",
  "world.event": "world-event",
  "session.state": "session-state",
  "session.telemetry": "session-telemetry",
};
