#!/usr/bin/env node
// Fetches a world's manifest over the protocol (the `initialize` result) and writes it as JSON.
//   node scripts/fetch-manifest.mjs --url ws://127.0.0.1:8710 [--token T] [--out manifest.json]
import { writeFileSync } from "node:fs";
import { parseArgs } from "node:util";
import { AwpClient } from "../dist/index.js";

const { values } = parseArgs({ options: { url: { type: "string" }, token: { type: "string" }, out: { type: "string" } } });
const url = values.url ?? process.env.AWP_URL;
if (!url) {
  console.error("--url or $AWP_URL is required");
  process.exit(2);
}
const token = values.token ?? process.env.AWP_TOKEN;
const client = new AwpClient({
  url,
  ...(token ? { token } : {}),
  agent: { name: "awp-typescript-fetch-manifest", version: "0.1.0", vendor: "hyperduality" },
  consumesModalities: ["proprio/json", "text/event+json"],
});
const manifest = await client.initialize();
const text = JSON.stringify(manifest.raw, null, 2) + "\n";
if (values.out) writeFileSync(values.out, text);
else process.stdout.write(text);
client.disconnect();
