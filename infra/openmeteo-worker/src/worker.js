// Mirror Market — Open-Meteo pass-through Worker (#637).
//
// Why: Open-Meteo's free quota is shared per outgoing IP, and a GitHub
// runner's IP is shared with strangers; on 2026-11-25 one of them spent the
// day's quota before Layer 5's first request. Traffic from a Cloudflare
// Worker is counted by Worker identity instead (README.md → Sources).
//
// What it does, and nothing else:
//   - GET only, on an allowlist of exact paths;
//   - only with the shared secret in X-Proxy-Token (constant-time compare),
//     so it is not an open proxy;
//   - forwards the query string unchanged, never the token;
//   - returns Open-Meteo's status, body and headers (X-RateLimit-* included)
//     unchanged, with every cache bypassed — a stale forecast is worse than
//     a refused one.
// Its own refusals carry X-Mirror-Proxy-Error, so the fetcher can tell a
// misconfigured Worker from an Open-Meteo answer and hard-fail on it.

// Exact path → upstream origin. The forecast endpoint is the only Open-Meteo
// API Layer 5 calls (fetchers/weather.py, config.OPENMETEO_FORECAST_URL).
const ALLOWED = Object.freeze({
  "/v1/forecast": "https://api.open-meteo.com",
});

const TOKEN_HEADER = "X-Proxy-Token";
const ERROR_HEADER = "X-Mirror-Proxy-Error";

function refuse(status, code) {
  return new Response(JSON.stringify({ error: true, reason: code }), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      [ERROR_HEADER]: code,
    },
  });
}

// Constant-time over the expected length: a mismatch at the first byte takes
// as long as one at the last. Length is not secret (the token is fixed-size).
function sameSecret(given, expected) {
  const enc = new TextEncoder();
  const a = enc.encode(given);
  const b = enc.encode(expected);
  let diff = a.length ^ b.length;
  for (let i = 0; i < b.length; i++) {
    diff |= (a[i] ?? 0) ^ b[i];
  }
  return diff === 0;
}

export default {
  async fetch(request, env) {
    const expected = env.PROXY_TOKEN;
    if (typeof expected !== "string" || expected.length === 0) {
      // Deployed without `wrangler secret put PROXY_TOKEN`: closed, not open.
      return refuse(500, "misconfigured");
    }
    if (request.method !== "GET") {
      return refuse(405, "method_not_allowed");
    }
    const given = request.headers.get(TOKEN_HEADER);
    if (given === null || !sameSecret(given, expected)) {
      return refuse(401, "unauthorized");
    }
    const url = new URL(request.url);
    const origin = Object.hasOwn(ALLOWED, url.pathname) ? ALLOWED[url.pathname] : undefined;
    if (origin === undefined) {
      return refuse(404, "path_not_allowed");
    }

    // A fresh request: none of the caller's headers (the token above all)
    // reach Open-Meteo. `no-store` bypasses Cloudflare's caches for a
    // non-Cloudflare origin (compatibility_date >= 2024-11-11).
    const upstream = await fetch(origin + url.pathname + url.search, {
      method: "GET",
      headers: { Accept: "application/json" },
      cache: "no-store",
      redirect: "manual",
    });

    // Status, body and headers as Open-Meteo sent them.
    return new Response(upstream.body, {
      status: upstream.status,
      statusText: upstream.statusText,
      headers: new Headers(upstream.headers),
    });
  },
};
