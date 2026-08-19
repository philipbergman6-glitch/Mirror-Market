# Onboarding a route — what has to be entered before it is comparable

This is the operational half of the landed-cost feature. `README.md` in this
directory is the *contract* (fields, units, selection rules); this file is the
*work*: for each route the desk trades, exactly which inputs must exist, in
which unit, at which scope, before the origin page can produce a landed total
and rank it against another origin.

Nothing here proposes a number. Every command below carries a `<VALUE>`
placeholder, because a suggested default is a fabricated default with an extra
step, and the one nobody changes is the one that quietly decides which origin
the page recommends.

**Two live views of this same list, generated from the code rather than from
this file, so they cannot go stale:**

```bash
# per-route checklist, with the exact command for every missing input.
# Reads no database — works on a fresh clone.
python scripts/enter_assumption.py --onboarding --window 2026-10-01:2026-10-31

# the renewal queue: what lapses, when, who owns it, which routes go dark
python scripts/enter_assumption.py --review --horizon 30
```

The same two views are on the page itself: **section 02, Route readiness**, and
**section 09, Renewals due**, at `docs/origins.html`.

---

## The three supported routes

All three price into **North China (`cn_north`)** — the Qingdao / Rizhao /
Dalian discharge range, which is how freight to China is quoted.

| Route | Leg id | Origin key | What the price leg is | Delivery term |
|---|---|---|---|---|
| US Gulf → North China | `us_gulf` | `us_gulf` | AMS report 3147 CIF NOLA barge bid | **CIF barge** |
| Brazil Paranaguá → North China | `br_paranagua` | `br_paranagua` | AgRural Paranaguá FOB (BRL/MT) | FOB vessel |
| Argentina Up River → North China | `ar_up_river` | `ar_up_river` | MAGyP official FOB (USD/MT) | FOB vessel |

A fourth leg, **US PNW (`us_pnw`)**, is declared with no price series and is
therefore not an onboarding task: no assumption makes it comparable. It is
shown on the page as an explicit "no source" row rather than dropped, so a
three-origin board is never mistaken for a complete one.

---

## What every route needs

Seven inputs, in the order the waterfall applies them. The order matters:
duty is a percentage of the CIF value and VAT a percentage of the duty-paid
value, so the sequence is part of the arithmetic.

| # | Component | Unit | Scope it must name | What it is |
|---|---|---|---|---|
| 1 | `ocean_freight` | `usd_per_mt` | origin **and** destination | The voyage rate for that exact leg. Per route — the US Gulf and Paranaguá voyages to North China are different distances, and the difference between them is routinely larger than the FOB spread being compared. |
| 2 | `marine_insurance` | `fraction` | destination | Cargo insurance as a fraction of the CFR value (`0.0012` = 0.12%). |
| 3 | `import_duty` | `fraction` | destination | Ad-valorem duty on the CIF value. **Shipped** for `cn_north` as a published policy rate — see `china_import_policy.yml`, and enter an *origin-scoped* entry if a retaliatory rate is in force rather than editing the MFN one. |
| 4 | `import_vat` | `fraction` | destination | VAT on the duty-paid value. **Shipped** for `cn_north` on the same terms. |
| 5 | `destination_port_costs` | `usd_per_mt` | destination | Discharge, handling and storage at the destination range. |
| 6 | `financing` | `rate_per_annum` **+ `--days`** | destination | Your cost of carry and the period it runs over. There is no market-wide number here — the rate and the days are yours. |
| 7 | `quality_adjustment` | `usd_per_mt`, signed | origin and destination | The protein / FM / moisture differential against what the destination pays for. **Not zero by default**: US No. 2 Yellow and Brazilian contract standard are different specifications and a Chinese crusher pays for protein. Entering zero is a decision, and it gets your name on it. |

### Plus, per route

| Route | Extra input | Why |
|---|---|---|
| **US Gulf** | `elevation` (`usd_per_mt`, origin-scoped) | The AMS bid is CIF *onto a barge* — one lift short of being on a vessel. Published nowhere free. Defaulted to zero it would make the US structurally cheapest every single day, in the same direction, and never look like an error. |
| Brazil Paranaguá | — | Already FOB vessel. |
| Argentina Up River | — | Already FOB vessel. |

**Route-specific inland cost** (`inland_transport`, plus `origin_port_costs`)
is required only by a leg quoted away from the berth — an ex-works or FCA
truck/rail price. None of the three routes above is quoted that way today, so
neither input is asked for; add such a leg to `config.ORIGIN_LEGS` and the
checklist grows the two rungs on its own, because the requirement is derived
from the delivery term rather than listed per origin.

---

## Entering one

```bash
python scripts/enter_assumption.py \
  --component ocean_freight --value <VALUE> --unit usd_per_mt \
  --origin br_paranagua --destination cn_north \
  --window 2026-10-01:2026-10-31 \
  --basis "<what the number is and where it came from>" \
  --entered-by you@example.com \
  --expires <YYYY-MM-DD> \
  --confidence indicative
```

Every entry carries all eight of these, and each one is refused if absent:
**value, unit, basis, source, owner (`entered_by`), entry date, expiry,
confidence** — plus the route and shipment-window scope above. An entry with no
owner, no reason and no expiry is indistinguishable from a guess.

`--window` is optional and means "this entry applies to every shipment window".
Use it for a genuinely period-independent number (a policy rate); do not use it
for freight, which is priced per window.

### What the validator will refuse

Run `python scripts/enter_assumption.py --check` (exit 1 on any of these):

* **unit incompatible with the component** — an ad-valorem rate entered as
  `usd_per_mt` parses cleanly and is wrong by two orders of magnitude;
* **scope too wide** — a freight, elevation, inland or quality entry with no
  `origin`, or a freight/duty/VAT/port entry with no `destination`;
* **a scope key that matches nothing** — `us-gulf` instead of `us_gulf` never
  matches a route, so it reads on the page as "never entered";
* **ambiguous overlap** — two entries of the same component and scope whose
  shipment windows *and* lifetimes overlap. Two answers to one question means
  one of them is stale;
* **a window that already sailed** — a shipment window ending before the entry
  date, which is almost always a mistyped year;
* **expiry before entry date**, a fraction outside `[0, 1)`, a flat cost above
  1000 USD/MT, a `rate_per_annum` with no `--days`.

Expired and expiring entries are **reported, not refused**: the record of what
was believed and when is the audit trail, and renewing it is a decision with a
person's name on it rather than a build break.

---

## Renewing

An expired input **blocks the route**. It is never replaced by a wider entry —
a lapsed US Gulf → North China freight does not quietly become a global rate,
because the resulting page would show a number for a route whose own assumption
had lapsed.

Renew before the lapse, not after:

```bash
python scripts/enter_assumption.py --review --horizon 30
```

Entering a renewal while the outgoing entry is still live is an **ambiguous
overlap**. Shorten the outgoing entry's `expires_on` to the day before the new
one starts — the two then form a chain, and the history of what was believed
when survives intact.

---

## What "ready" does and does not mean

A route is **ready** when every input it needs is entered and live. That is not
the same as **ranked**. Three things still stop a ready route from being
compared, and all three are facts about the market rather than about the
onboarding:

* the origin is quoting a **different shipment window** than the one asked for;
* the origin publishes **no shipment window at all** (AgRural's Paranaguá level
  is a port-side assessment with no period attached, so it is shown and never
  ranked against a dated offer);
* the origins on the board were **observed too many days apart**
  (`config.ORIGIN_MAX_OBSERVATION_SPREAD_DAYS`), over which gap the difference
  between them measures the calendar as much as the market.

The page states each of these in its own words on the row it applies to.

---

## Once two routes are ready

Section 05 of the page answers the question the desk actually has — not "what
is the landed cost" but **"how wrong does an input have to be before the
cheapest origin changes"**. Every input gets a solved move, stated in its own
entered unit and ordered by how wrong the number would have to be as a
percentage of itself.

An input **both** origins are costed off — duty, VAT, discharge at the same
destination — moves both landed totals together and mostly cannot flip
anything. That is said in words on the row rather than shown as a large number,
which would read as "unlikely, but possible".
