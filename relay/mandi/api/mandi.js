// India-egress relay for the data.gov.in Agmarknet mandi resource (Layer 16).
//
// api.data.gov.in refuses TCP connections from outside India (observed
// 2026-10-05: refused from US and Israel, accepted from Mumbai and Rajpura).
// This function runs in Vercel's bom1 (Mumbai) region and forwards the
// fetcher's query string to the one fixed resource URL. It is not an open
// proxy: the upstream is a constant and every request needs RELAY_TOKEN.
import { authorise, forward } from "../lib/relay.js";

const UPSTREAM =
  "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070";

export default async function handler(req, res) {
  if (!authorise(req, res)) return;
  const query = new URL(req.url, "http://relay.invalid").search;
  await forward(res, UPSTREAM + query, {
    "User-Agent": req.headers["user-agent"] ?? "Mirror-Market mandi relay",
    Accept: "application/json",
  });
}
