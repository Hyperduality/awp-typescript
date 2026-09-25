#!/usr/bin/env node
// Vendors the canonical AWP artifacts from the specification repository into ./schemas:
//   schemas/v0.1/**/*.schema.json      (canonical JSON Schemas)
//   schemas/test-vectors/frames.json   (frame test vectors, AWP-DAT-008)
//   schemas/action-lifecycle.json      (spec/action-lifecycle.yaml, the normative transition table)
//
// Usage:
//   node scripts/sync-schemas.mjs [--from <docs-repo>] [--ref <git-ref>]          copy
//   node scripts/sync-schemas.mjs --check [--from <docs-repo>] [--ref <git-ref>]  fail if the vendored copy differs
//
// The docs repo defaults to $AWP_SPEC_REPO or ../agent-world-protocol. Files are read from a git ref, so a
// checkout on another branch cannot leak into the vendored copy. Syncing defaults to the tag
// `spec-v<SPEC_REVISION>` (from src/version.ts); the ref and the commit it resolved to are recorded in
// schemas/source.json, and `--check` compares against that recorded ref unless --ref is given.
// `--ref WORKTREE` reads the working tree.
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync, mkdirSync, readdirSync, statSync, existsSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "..");
const args = process.argv.slice(2);
const check = args.includes("--check");
const fromIdx = args.indexOf("--from");
const specRepo = resolve(
  fromIdx >= 0 ? args[fromIdx + 1] : process.env.AWP_SPEC_REPO ?? join(root, "..", "agent-world-protocol"),
);

if (!existsSync(join(specRepo, "schemas"))) {
  console.error(`spec repository not found at ${specRepo} (use --from or $AWP_SPEC_REPO)`);
  process.exit(2);
}
const revision = /SPEC_REVISION = "([^"]+)"/.exec(readFileSync(join(root, "src", "version.ts"), "utf8"))?.[1];
const SOURCE = join(root, "schemas", "source.json");
const recorded = existsSync(SOURCE) ? JSON.parse(readFileSync(SOURCE, "utf8")) : undefined;
const refIdx = args.indexOf("--ref");
const ref = refIdx >= 0 ? args[refIdx + 1] : check && recorded ? recorded.ref : `spec-v${revision}`;
const fromWorktree = ref === "WORKTREE";
let commit = "WORKTREE";
if (!fromWorktree) {
  try {
    commit = execFileSync("git", ["-C", specRepo, "rev-parse", "--verify", `${ref}^{commit}`], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    console.error(`ref ${ref} not found in ${specRepo}${refIdx < 0 ? " (pass --ref <commit> until the tag exists)" : ""}`);
    process.exit(2);
  }
}

/** Lists files under a repo-relative directory at the ref. */
function listSpec(dir) {
  if (fromWorktree) return walk(join(specRepo, dir)).map((f) => relative(specRepo, f));
  const out = execFileSync("git", ["-C", specRepo, "ls-tree", "-r", "--name-only", ref, "--", dir], { encoding: "utf8" });
  return out.split("\n").filter((l) => l.endsWith(".json")).sort();
}

/** Reads a repo-relative file at the ref. */
function readSpec(path) {
  if (fromWorktree) return readFileSync(join(specRepo, path), "utf8");
  return execFileSync("git", ["-C", specRepo, "show", `${ref}:${path}`], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
}

function walk(dir) {
  const out = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) out.push(...walk(p));
    else if (name.endsWith(".json")) out.push(p);
  }
  return out.sort();
}

/** @type {Map<string, string>} destination (relative to ./schemas) -> content */
const wanted = new Map();
for (const file of listSpec("schemas/v0.1")) {
  wanted.set(relative("schemas", file), readSpec(file));
}
wanted.set(join("test-vectors", "frames.json"), readSpec("schemas/test-vectors/frames.json"));
const lifecycle = parseYaml(readSpec("spec/action-lifecycle.yaml"));
wanted.set(
  "action-lifecycle.json",
  JSON.stringify({ $comment: "Generated from spec/action-lifecycle.yaml by scripts/sync-schemas.mjs; do not edit.", ...lifecycle }, null, 2) + "\n",
);

const dest = join(root, "schemas");
let differences = 0;
const present = existsSync(dest) ? new Set(walk(dest).map((f) => relative(dest, f))) : new Set();
present.delete("source.json");
if (check && recorded && !fromWorktree && recorded.commit !== commit) {
  differences++;
  console.error(`schemas/source.json records ${recorded.ref} = ${recorded.commit}, but ${ref} is ${commit}`);
}
if (!check) {
  const source = JSON.stringify({ spec_revision: revision, ref, commit }, null, 2) + "\n";
  if (!recorded || JSON.stringify(recorded) + "\n" !== JSON.stringify(JSON.parse(source)) + "\n") {
    writeFileSync(SOURCE, source);
    console.log(`recorded ${ref} (${commit}) in schemas/source.json`);
  }
}
for (const [rel, content] of wanted) {
  const target = join(dest, rel);
  const current = existsSync(target) ? readFileSync(target, "utf8") : undefined;
  present.delete(rel);
  if (current === content) continue;
  differences++;
  if (check) {
    console.error(`${current === undefined ? "missing" : "differs"}: schemas/${rel}`);
  } else {
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, content);
    console.log(`updated schemas/${rel}`);
  }
}
for (const extra of present) {
  differences++;
  console.error(`not in the spec repository: schemas/${extra}`);
}
if (check) {
  if (differences > 0) {
    console.error(`${differences} vendored file(s) out of date with ${specRepo} at ${ref}; run npm run sync-schemas`);
    process.exit(1);
  }
  console.log(`vendored schemas match ${specRepo} at ${ref}`);
} else if (differences === 0) {
  console.log("vendored schemas already up to date");
}
