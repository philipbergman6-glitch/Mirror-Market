# Draft: data-use request to AFEX Commodities Exchange

- **Date drafted:** 2026-08-10
- **Purpose:** obtain written permission to display AFEX's Nigerian soybean reference price on the Mirror Market dashboard, per AFEX's terms of use ("You agree not to reproduce, duplicate, copy, sell, resell or exploit any portion of the Platform or our services without the express written permission by us").
- **Status:** NOT SENT — awaiting the user's review. Outward-facing contact under the user's name.
- **Route:** contact form at `https://www.afex.africa/contact-us`. If a direct address surfaces (data/market-data team), prefer that.
- **Related:** `research/2026-08-10-nigeria-price-source.md`

Two things to decide before sending: whether to name the project publicly, and whether to ask about the `Origin`-header access route explicitly. The draft below does both, because raising it now is cheaper than being blocked later.

---

**Subject:** Permission request — displaying AFEX soybean reference prices on a non-commercial market dashboard

Hello,

I maintain Mirror Market, a non-commercial dashboard that tracks the global soybean complex — CBOT, Dalian, SAFEX, CONAB and CEPEA in Brazil, MAGyP in Argentina, and Indian mandi prices. It is a personal research project, published free and without advertising.

Nigeria is currently the largest gap in that picture. It is a major producer, but I have not been able to find a public daily soybean price series for it other than yours. AFEX's soybean reference price is the only Nigerian series I found that is daily, consistent, and corroborated — I cross-checked its level against NCX and LCFE quotes for the same date and they agree within about 10%.

I would like your permission to display it, and I want to ask before using it rather than after.

Specifically, I am asking to:

- show the AFEX soybean reference price, credited to AFEX Commodities Exchange with a link back to your site;
- display it converted to USD per metric tonne alongside other origins, and as a premium or discount to CBOT;
- retain history locally so the chart has context.

I am not asking to redistribute your data in bulk, resell it, or offer it through an API. If you would prefer that only a derived figure appears — for example the premium to CBOT rather than the naira price itself — that works, and I am happy to agree to any attribution wording, update frequency, or delay you require.

One technical point I would rather raise than leave unsaid: your market-data endpoint at `api-md.afexnigeria.com` is reachable without credentials, and returns data when a browser-style `Origin` header is sent. I have not attempted to bypass any authentication, and I would rather access the data through whatever route you consider appropriate — a documented feed, a licensed key, or a periodic file — than depend on an undocumented one.

If there is a licence, data-use agreement, or fee that applies, please let me know and I will follow it. If the answer is no, that is a clear answer and I will leave the series out.

Thank you for considering it.

[Name]
[Email]
[Project URL]

---

## Notes for the user

- **If they say yes:** set `AFEX_PUBLISH_RAW = True` in `config.py`, add the agreed attribution to the dashboard footer and the sources list, and record any constraints (delay, derived-only, wording) alongside it.
- **If they say no:** the layer stays local-only. Delete nothing — Nigeria falls back to the demand-only page shape (PSD + NGN/USD + Benue/Kaduna weather), and the research note's §7 open questions stand.
- **If they don't reply:** that is not consent. The current code already defaults to local-only, so silence changes nothing.
- **Do not raise the Tableau credential leak noted incidentally in the NBS research** — different organisation, unrelated to AFEX, and not ours to report through this channel.
