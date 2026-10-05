// India-egress relay for the keyless Agmarknet 2.0 report API
// (api.agmarknet.gov.in/v1), which answers 403 outside India (observed
// 2026-10-05: 403 from US, 200 from Mumbai and Rajpura).
//
// `?path=/prices-and-arrivals/...` picks a read-only /v1 report path; every
// other query parameter is passed through. The host is a constant, the path
// is restricted to /v1 report characters, and every request needs RELAY_TOKEN.
import { authorise, forward } from "../lib/relay.js";

const UPSTREAM = "https://api.agmarknet.gov.in/v1";
const PATH_RE = /^\/[a-z0-9-]+(\/[a-z0-9-]+)*\/?$/;

export default async function handler(req, res) {
  if (!authorise(req, res)) return;
  if (req.method !== "GET") {
    res.status(405).json({ error: "GET only" });
    return;
  }
  const url = new URL(req.url, "http://relay.invalid");
  const path = url.searchParams.get("path") ?? "";
  if (!PATH_RE.test(path)) {
    res.status(400).json({ error: "path must be a /v1 report path" });
    return;
  }
  url.searchParams.delete("path");
  await forward(res, UPSTREAM + path + url.search, {
    // The site's backend 403s requests that do not look like its own frontend.
    "User-Agent":
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
      "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    Accept: "application/json, text/plain, */*",
    Origin: "https://agmarknet.gov.in",
    Referer: "https://agmarknet.gov.in/",
  });
}
