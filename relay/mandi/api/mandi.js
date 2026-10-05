// India-egress relay for the data.gov.in Agmarknet mandi resource (Layer 16).
//
// api.data.gov.in refuses TCP connections from outside India (observed
// 2026-10-05: refused from US and Israel, accepted from Mumbai and Rajpura).
// This function runs in Vercel's bom1 (Mumbai) region and forwards the
// fetcher's query string to the one fixed resource URL, returning upstream's
// status and body unchanged. It is not an open proxy: the upstream is a
// constant and every request must carry the shared RELAY_TOKEN.
import { timingSafeEqual } from "node:crypto";

const UPSTREAM =
  "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070";
const UPSTREAM_TIMEOUT_MS = 55_000;

function tokenMatches(given, expected) {
  const a = Buffer.from(String(given ?? ""));
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

export default async function handler(req, res) {
  res.setHeader("x-relay-region", process.env.VERCEL_REGION ?? "unknown");
  const expected = process.env.RELAY_TOKEN;
  if (!expected) {
    res.status(500).json({ error: "relay misconfigured: RELAY_TOKEN unset" });
    return;
  }
  if (!tokenMatches(req.headers["x-relay-token"], expected)) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }
  const query = new URL(req.url, "http://relay.invalid").search;
  let upstream;
  try {
    upstream = await fetch(UPSTREAM + query, {
      headers: {
        "User-Agent": req.headers["user-agent"] ?? "Mirror-Market mandi relay",
        Accept: "application/json",
      },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
  } catch (err) {
    // A transport failure is the relay's own 502, never a fake 200.
    res.status(502).json({ error: `relay upstream fetch failed: ${err}` });
    return;
  }
  res.status(upstream.status);
  res.setHeader(
    "content-type",
    upstream.headers.get("content-type") ?? "application/octet-stream",
  );
  res.send(Buffer.from(await upstream.arrayBuffer()));
}
