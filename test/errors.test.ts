import { test } from "node:test";
import assert from "node:assert/strict";
import { AwpError, RequestTimeoutError, TimeoutError } from "../src/errors.ts";

test("errorName is the registry name, or the JSON-RPC message for reserved codes", () => {
  assert.equal(new AwpError({ code: 3002, message: "AWP_BUSY", data: { retryable: true, detail: "arm busy" } }).errorName, "AWP_BUSY");
  const reserved = new AwpError({ code: -32602, message: "Invalid params", data: { detail: "ping requires origin_ns" } });
  assert.equal(reserved.errorName, "Invalid params");
  assert.equal(reserved.message, "Invalid params: ping requires origin_ns");
});

test("a request timeout is a TimeoutError", () => {
  const e = new RequestTimeoutError("world.tick", 250);
  assert.ok(e instanceof TimeoutError);
  assert.equal(e.timeoutMs, 250);
  assert.equal(e.method, "world.tick");
});
