// #637 — the Worker contract, against recorded Open-Meteo answers.
//
// Run: node --test infra/openmeteo-worker/test/   (tests/test_openmeteo_worker.py
// runs it inside pytest). No network: globalThis.fetch is replaced per test,
// and the recorded bodies are the same fixtures the Python fetcher tests use.

import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import worker from "../src/worker.js";

const here = dirname(fileURLToPath(import.meta.url));
const fixtures = join(here, "..", "..", "..", "tests", "fixtures");
const BATCH = readFileSync(join(fixtures, "openmeteo_forecast_batch_2026-11-25.json"), "utf8");
const DAILY_429 = readFileSync(join(fixtures, "openmeteo_429_daily_2026-11-25.json"), "utf8");

// Generated per run: a sentinel, never a real secret.
const TOKEN = `tok-${randomUUID()}`;
const WRONG = `tok-${randomUUID()}`;
const ENV = { PROXY_TOKEN: TOKEN };
const BASE = "https://mirror-market-openmeteo.example.workers.dev";
const QUERY = "?latitude=42.0,-12.5&longitude=-93.5,-55.7&daily=temperature_2m_max&past_days=30&forecast_days=7&timezone=auto";

let upstream;  // what the Worker asked Open-Meteo for
let answer;    // what Open-Meteo answers
const realFetch = globalThis.fetch;

beforeEach(() => {
  upstream = [];
  answer = () => new Response(BATCH, {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "X-RateLimit-Cost": "24.0",
      "X-RateLimit-Remaining": "9976.0",
      "X-RateLimit-Window": "86400",
    },
  });
  globalThis.fetch = async (input, init) => {
    upstream.push({ url: String(input), init: init ?? {} });
    return answer();
  };
});

afterEach(() => {
  globalThis.fetch = realFetch;
});

function call(path, { token = TOKEN, method = "GET", env = ENV } = {}) {
  const headers = token === null ? {} : { "X-Proxy-Token": token };
  return worker.fetch(new Request(BASE + path, { method, headers }), env);
}

test("forecast is forwarded to api.open-meteo.com with the query unchanged", async () => {
  const resp = await call("/v1/forecast" + QUERY);
  assert.equal(resp.status, 200);
  assert.equal(upstream.length, 1);
  assert.equal(upstream[0].url, "https://api.open-meteo.com/v1/forecast" + QUERY);
  assert.equal(upstream[0].init.method, "GET");
});

test("the body comes back byte-for-byte", async () => {
  const resp = await call("/v1/forecast" + QUERY);
  assert.equal(await resp.text(), BATCH);
});

test("X-RateLimit-* headers come back unchanged", async () => {
  const resp = await call("/v1/forecast" + QUERY);
  assert.equal(resp.headers.get("X-RateLimit-Cost"), "24.0");
  assert.equal(resp.headers.get("X-RateLimit-Remaining"), "9976.0");
  assert.equal(resp.headers.get("X-RateLimit-Window"), "86400");
  assert.equal(resp.headers.get("Content-Type"), "application/json; charset=utf-8");
});

test("Open-Meteo's own 429 is relayed as-is, never marked as the Worker's refusal", async () => {
  answer = () => new Response(DAILY_429, { status: 429, headers: { "Retry-After": "33000" } });
  const resp = await call("/v1/forecast" + QUERY);
  assert.equal(resp.status, 429);
  assert.equal(await resp.text(), DAILY_429);
  assert.equal(resp.headers.get("Retry-After"), "33000");
  assert.equal(resp.headers.get("X-Mirror-Proxy-Error"), null);
});

test("an upstream 5xx is relayed with its status, unmarked", async () => {
  answer = () => new Response('{"error":true,"reason":"upstream"}', { status: 503 });
  const resp = await call("/v1/forecast" + QUERY);
  assert.equal(resp.status, 503);
  assert.equal(resp.headers.get("X-Mirror-Proxy-Error"), null);
});

test("the upstream fetch opts out of every cache", async () => {
  await call("/v1/forecast" + QUERY);
  assert.equal(upstream[0].init.cache, "no-store");
});

test("the token is never forwarded to Open-Meteo", async () => {
  await call("/v1/forecast" + QUERY);
  const sent = JSON.stringify(upstream[0]);
  assert.ok(!sent.includes(TOKEN));
});

for (const [label, token] of [["missing", null], ["wrong", WRONG], ["prefix of the right one", TOKEN.slice(0, -1)]]) {
  test(`a ${label} token is refused 401 before any upstream call`, async () => {
    const resp = await call("/v1/forecast" + QUERY, { token });
    assert.equal(resp.status, 401);
    assert.equal(resp.headers.get("X-Mirror-Proxy-Error"), "unauthorized");
    assert.equal(upstream.length, 0);
  });
}

for (const path of ["/v1/archive", "/", "/v1/forecast/../archive", "/v1/forecastx", "/V1/forecast", "/v1/forecast/"]) {
  test(`path ${path} is not on the allowlist`, async () => {
    const resp = await call(path + QUERY);
    assert.equal(resp.status, 404);
    assert.equal(resp.headers.get("X-Mirror-Proxy-Error"), "path_not_allowed");
    assert.equal(upstream.length, 0);
  });
}

test("only GET is forwarded", async () => {
  const resp = await call("/v1/forecast" + QUERY, { method: "POST" });
  assert.equal(resp.status, 405);
  assert.equal(resp.headers.get("X-Mirror-Proxy-Error"), "method_not_allowed");
  assert.equal(upstream.length, 0);
});

test("a Worker deployed without its secret refuses everything, it never runs open", async () => {
  for (const env of [{}, { PROXY_TOKEN: "" }]) {
    const resp = await call("/v1/forecast" + QUERY, { env });
    assert.equal(resp.status, 500);
    assert.equal(resp.headers.get("X-Mirror-Proxy-Error"), "misconfigured");
  }
  assert.equal(upstream.length, 0);
});

test("the Worker's own refusals say no-store and never echo the token", async () => {
  const resp = await call("/v1/forecast" + QUERY, { token: WRONG });
  assert.equal(resp.headers.get("Cache-Control"), "no-store");
  assert.ok(!(await resp.text()).includes(TOKEN));
});
