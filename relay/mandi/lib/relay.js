// Shared India-egress forwarding for the relay's routes. Each route fixes
// its own upstream; nothing here lets a caller choose the host.
import { timingSafeEqual } from "node:crypto";

const UPSTREAM_TIMEOUT_MS = 55_000;

function tokenMatches(given, expected) {
  const a = Buffer.from(String(given ?? ""));
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

// Returns false (response already sent) unless the request carries RELAY_TOKEN.
export function authorise(req, res) {
  res.setHeader("x-relay-region", process.env.VERCEL_REGION ?? "unknown");
  const expected = process.env.RELAY_TOKEN;
  if (!expected) {
    res.status(500).json({ error: "relay misconfigured: RELAY_TOKEN unset" });
    return false;
  }
  if (!tokenMatches(req.headers["x-relay-token"], expected)) {
    res.status(401).json({ error: "unauthorized" });
    return false;
  }
  return true;
}

// GET `url` from bom1 and hand back upstream's status and body unchanged.
export async function forward(res, url, headers) {
  let upstream;
  try {
    upstream = await fetch(url, {
      headers,
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
