# Open-Meteo Worker (#637)

This Worker gives Layer 5 (weather) its own Open-Meteo quota. It is a pass-through to `api.open-meteo.com` on Cloudflare Workers Free.

**Owner-run.** Deploy it with `scripts/setup_openmeteo_worker.sh`. Agents never deploy it or create its secrets.

## Why

Open-Meteo's free API counts weighted calls per outgoing IP. GitHub runners share IPs with other tenants. On 2026-11-25 one of those tenants used up the day's quota before Layer 5 made its first request (#637). Open-Meteo counts Worker traffic by Worker identity instead of by IP (see [Sources](#sources)).

## What it does

| Request | Answer |
|---|---|
| `GET /v1/forecast?…` with the right `X-Proxy-Token` | Open-Meteo's own status, body (each entry's `latitude`/`longitude`/`location_id`, a 429's JSON `reason`) and headers (`Retry-After`, `X-RateLimit-*`), all unchanged, plus one added header: `X-Mirror-Proxy-Upstream: api.open-meteo.com` |
| missing or wrong token | `401` with `X-Mirror-Proxy-Error: unauthorized` |
| any other path (`/v1/archive`, `/v1/forecast/`, `/`, …) | `404` with `path_not_allowed` |
| any method other than GET | `405` with `method_not_allowed` |
| Worker deployed without its `PROXY_TOKEN` secret | `500` with `misconfigured`. The Worker refuses every request, so it is never an open proxy. |

- **Token.** Compared in constant time and never forwarded to Open-Meteo. The upstream request is built fresh and carries only `Accept`.
- **No caching.** The upstream fetch uses `cache: "no-store"`. This needs `compatibility_date` ≥ 2024-11-11; `wrangler.jsonc` sets 2025-11-01. A stale forecast is worse than a refused one.
- **Refusals.** The Worker's own refusals carry `X-Mirror-Proxy-Error`. `fetchers/weather.py` raises `OpenMeteoProxyError` on that header and never retries.
- **Who answered.** Every relayed answer carries `X-Mirror-Proxy-Upstream`. Through the Worker, the fetcher reads a response *without* it as Cloudflare's, not Open-Meteo's:
  - a 429 (e.g. Error 1027, the Workers Free daily cap) is `rate_limited` with a reason naming Cloudflare's limit and the page title, never Open-Meteo's quota;
  - a 200 is not weather: `OpenMeteoProxyError`;
  - a 5xx is retried like any transport failure.
- **Relayed answers.** A quota 429 is `rate_limited` naming this Worker's quota, with `Retry-After`; a concurrency 429 and a 5xx are retried. A 403 "Your API key is over quota." (the paid customer API's monthly refusal, rate-limit doc L134) is `rate_limited` too, never retried — the seam for the later paid key.
- **Allowlist.** It has one entry because forecast is the only Open-Meteo API we call (`config.OPENMETEO_FORECAST_PATH`). Adding an endpoint means adding it to `ALLOWED` in `src/worker.js` and to `tests/test_openmeteo_worker.py`.

## Configuration

| Where | Name | What |
|---|---|---|
| Worker secret | `PROXY_TOKEN` | Random 64-hex token from `openssl rand -hex 32`, set with `wrangler secret put PROXY_TOKEN` (stdin) |
| GitHub secret + `.env` | `OPENMETEO_PROXY_URL` | `https://mirror-market-openmeteo.<subdomain>.workers.dev`: https only, no path |
| GitHub secret + `.env` | `OPENMETEO_PROXY_TOKEN` | Same value as `PROXY_TOKEN` |

- **Set both or neither.** With both set, Layer 5 goes through the Worker. With neither, it goes direct on the shared quota and logs that it did. With only one, the layer fails on purpose.
- **Check the result.** `python main.py --doctor` lists both. A run log says `via Worker <host>` or `direct to api.open-meteo.com`.

## Tests

```bash
node --test --test-reporter=tap infra/openmeteo-worker/test/worker.test.mjs   # 23 cases, no network
pytest tests/test_openmeteo_worker.py tests/test_weather_proxy.py
```

The Worker tests replay the same recorded Open-Meteo bodies the fetcher tests use (`tests/fixtures/openmeteo_*_2026-11-25.json`).

## Limits we live inside

**Cloudflare Workers Free:**

- **Requests:** 100,000 a day, reset at midnight UTC. We send about 2 a day: one batched request per daily run, two with the #577 second pass.
- **CPU:** 10 ms per request. Time spent waiting on `fetch()` does not count.
- **Subrequests:** 50 per request. We make 1.
- **Over the daily cap:** Cloudflare returns Error 1027. The fetcher then sees a non-200 answer without the proxy marker. It handles that like any upstream failure: a 429 is `rate_limited`, anything else is retried and then empty.

**Open-Meteo free tier:** 10,000 weighted calls a day, 5,000 an hour and 600 a minute, all applying together. One run costs 24 weighted calls: 1 per pin at 3 variables × 37 days. Batching does not lower that.

## Known unknown: what "Worker identity" means on workers.dev

The exact grouping is **not verified at a primary source.** It is the one thing the wizard measures rather than assumes.

1. **The code.** Open-Meteo's code keys Worker traffic on a hash of the `CF-Worker` request header (`ApiKeyManager.swift`, below).
2. **What Cloudflare sends.** Cloudflare documents that header as "the name of the zone which owns the Worker making the subrequest".
3. **The gap.** No Cloudflare doc states that value for a Worker served from `*.workers.dev`. If it is the account's `<subdomain>.workers.dev`, the quota is ours. If it were plain `workers.dev`, every workers.dev Worker would share one quota.

**The wizard's check.** Its end-to-end stage prints `X-RateLimit-Remaining` from a call through the Worker. A fresh, unshared quota reads close to 10,000.

**If the quota turns out to be shared.** Attach a custom domain on a zone you own to the Worker. `CF-Worker` is then that zone's name, which nobody else has.

## Commercial use: this is a pre-sale stopgap

- **The terms.** Open-Meteo's free API is **non-commercial only**. Their terms list "Commercial products or promotional activities" as commercial use.
- **What replaces it.** Before this product is sold, a paid Open-Meteo customer key replaces this Worker: API Standard, €29/month for 1M calls/month as listed 2026-11-25. Its endpoint is `customer-api.open-meteo.com` with the same syntax plus `apikey`. Tracked in `ROADMAP.md` → "Open-Meteo customer key".
- **Why the Worker does not solve this.** It fixes the shared-IP quota and nothing else. It does not make the free tier licence-clean.

## Sources

All retrieved 2026-11-25.

**Open-Meteo**

- **Rate-limiting doc**, [`docs/api-rate-limiting.md` @ aeab72e](https://github.com/open-meteo/open-meteo/blob/aeab72e33f6a30f1a87d1b5444fd169d536ce914/docs/api-rate-limiting.md):
  - L15: "Usage is generally shared by requests from the same public IP address, so applications using the same outgoing connection can share a quota. Cloudflare Worker traffic may be grouped by Worker identity instead."
  - L148: "…or by Worker identity for identified Cloudflare Worker traffic."
  - L7–13: minute 600, hour 5,000, day 10,000. "All three limits apply together."
  - L134: "If customer access is restricted for exceeding the monthly allowance, the response is HTTP `403 Forbidden` with the reason `Your API key is over quota.`"
- **Code**, [`ApiKeyManager.swift` @ a358d6c L190–203](https://github.com/open-meteo/open-meteo/blob/a358d6c591f21f3adbdcfa10517f761d688b527d/Sources/App/Helper/Vapor/ApiKeyManager.swift#L190-L203): "Limit is IP based, except for CF Workers, where limits are applied per user." "For CloudFlare worker applications, use the `CF-Worker` header for rate limiting instead of IP address."
- **Terms**, <https://open-meteo.com/en/terms>:
  - "Less than 10'000 API calls per day, 5'000 per hour and 600 per minute."
  - "You may only use the free API services for non-commercial purposes."
  - "Consider using Cloudflare Workers, as we apply limits per Cloudflare user."
- **Pricing**, <https://open-meteo.com/en/pricing>: "A subscription grants a commercial use licence and an API key for the dedicated customer endpoint at customer-api.open-meteo.com."

**Cloudflare**

- **Workers limits**, <https://developers.cloudflare.com/workers/platform/limits/>:
  - "Accounts on the Workers Free plan have a daily request limit of 100,000 requests, resetting at midnight UTC."
  - 10 ms CPU, and network waits don't count.
  - 50 subrequests.
  - "When a Worker exceeds this limit, Cloudflare returns Error 1027."
- **Pricing**, <https://developers.cloudflare.com/workers/platform/pricing/>.
- **`fetch` cache option**, <https://developers.cloudflare.com/workers/runtime-apis/fetch/>: "For `no-store`, requests to origins not hosted by Cloudflare bypass the use of Cloudflare's caches."
- **Compatibility flags**, <https://developers.cloudflare.com/workers/configuration/compatibility-flags/>: `cache_option_enabled` is the "Default as of 2024-11-11".
- **Secrets**, <https://developers.cloudflare.com/workers/configuration/secrets/>: "Secret values are not visible within Wrangler or Cloudflare dashboard after you define them." `wrangler secret put` accepts piped input.
- **Wrangler config**, <https://developers.cloudflare.com/workers/wrangler/configuration/>: "At a minimum, the `name`, `main` and `compatibility_date` keys are required to deploy a Worker."
- **`CF-Worker` header**, <https://developers.cloudflare.com/fundamentals/reference/http-headers/>: set to "the name of the zone which owns the Worker making the subrequest".
