import { test } from "node:test";
import assert from "node:assert/strict";
import { ActionRecord, isTerminal, reachable, reasonAllowed, stateClass, transition } from "../src/lifecycle.ts";
import type { ActionState } from "../src/types.ts";

const rec = () => new ActionRecord({ action_id: "a-1", type: "move", params: {} });

test("the vendored table has the normative states and transitions", () => {
  for (const s of ["rejected", "completed", "failed", "preempted", "cancelled"]) assert.ok(isTerminal(s));
  assert.equal(stateClass("cancelling"), "execution");
  assert.ok(transition("executing", "cancelling"));
  assert.ok(!transition("queued", "executing"));
  assert.equal(reachable("queued", "executing"), 2);
  assert.ok(reasonAllowed("cancelling", "failed", "e_stop"));
  assert.ok(!reasonAllowed("cancelling", "failed", "superseded"));
  assert.ok(reasonAllowed("cancelling", "failed", "x-acme.gremlins"));
});

test("every lifecycle state is handled, including pending_approval, queued, cancelling, preempted (AWP-AGT-005)", () => {
  const paths: ActionState[][] = [
    ["pending_approval", "queued", "accepted", "executing", "completed"],
    ["queued", "cancelled"],
    ["accepted", "executing", "cancelling", "cancelled"],
    ["accepted", "executing", "preempted"],
    ["pending_approval", "rejected"],
    ["accepted", "executing", "cancelling", "failed"],
  ];
  for (const path of paths) {
    const r = rec();
    path.forEach((state, i) => {
      const out = r.apply({ state, status_seq: i + 1, source: i === 0 ? "submit" : "status", ...(isTerminal(state) && state !== "completed" && state !== "preempted" ? { reason: state === "rejected" ? "approval_denied" : state === "failed" ? "e_stop" : "cancelled_by_agent" } : {}) });
      assert.equal(out.kind, "applied", `${path.join("→")} at ${state}`);
      assert.equal((out as { legal: boolean }).legal, true, `${path.join("→")} at ${state}`);
    });
    assert.ok(r.terminal);
    assert.deepEqual(r.violations, []);
  }
});

test("redelivered statuses are deduplicated on status_seq (AWP-LIF-009)", () => {
  const r = rec();
  r.apply({ state: "accepted", status_seq: 3, source: "submit" });
  r.apply({ state: "executing", status_seq: 4, source: "status" });
  r.apply({ state: "completed", status_seq: 5, source: "status" });
  assert.deepEqual(r.apply({ state: "completed", status_seq: 5, source: "status" }), { kind: "duplicate" });
  assert.equal(r.history.length, 3);
});

test("a second terminal status is not a second terminal transition", () => {
  const r = rec();
  r.apply({ state: "accepted", status_seq: 1, source: "submit" });
  r.apply({ state: "cancelled", status_seq: 2, reason: "cancelled_by_agent", source: "status" });
  const out = r.apply({ state: "completed", status_seq: 3, source: "status" });
  assert.equal(out.kind, "after_terminal");
  assert.equal(r.state, "cancelled");
});

test("progress updates repeat the executing state; illegal transitions are flagged", () => {
  const r = rec();
  r.apply({ state: "accepted", status_seq: 1, source: "submit" });
  r.apply({ state: "executing", status_seq: 2, progress: 0.1, source: "status" });
  r.apply({ state: "executing", status_seq: 3, progress: 0.5, source: "status" });
  assert.equal(r.progress, 0.5);
  const bad = rec();
  bad.apply({ state: "queued", status_seq: 1, source: "submit" });
  const out = bad.apply({ state: "completed", status_seq: 2, source: "status" });
  assert.equal(out.kind, "applied");
  assert.equal((out as { legal: boolean }).legal, false);
  assert.equal(bad.violations.length, 1);
});

test("an older result arriving after newer notifications is stale, not applied", () => {
  const r = rec();
  r.apply({ state: "executing", status_seq: 5, source: "status" });
  assert.deepEqual(r.apply({ state: "accepted", status_seq: 4, source: "submit" }), { kind: "stale" });
  assert.equal(r.state, "executing");
});

test("settled() resolves on terminal, refusal, or loss", async () => {
  const a = rec();
  const p = a.settled();
  a.apply({ state: "rejected", status_seq: 1, reason: "deadline_exceeded", source: "status" });
  assert.equal((await p).state, "rejected");
  const b = rec();
  const q = b.settled();
  b.markLost();
  assert.equal((await q).lost, true);
});
