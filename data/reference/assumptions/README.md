# Landed-cost assumptions

Hand-entered cost inputs for the origin comparison (`analysis/origins/`).
One YAML file per topic; every file is a list of assumption mappings.

**Onboarding a route** — exactly what must be entered before US Gulf, Brazil
Paranaguá or Argentina Up River becomes comparable into North China, and in
what unit and scope: see [ONBOARDING.md](ONBOARDING.md), or run
`python scripts/enter_assumption.py --onboarding`.

## Why this directory exists

Ocean freight, barge-to-vessel elevation, port charges, financing terms and
quality differentials are not available to this stack for free. Baltic and
Platts route assessments are licensed products; nobody publishes a NOLA
elevation spread at all.

The choice is therefore between **fabricating precision** and **asking a human
to enter a number and sign for it**. This is the second one. A clearly entered
freight assumption with an owner and an expiry is a better input than a
plausible-looking default, and an *absent* one is better than both, because the
page then says "not comparable — enter a US Gulf → North China freight" instead
of publishing a confident wrong ranking.

**Nothing here may be a guess dressed as data.** If you do not know a number,
do not enter one. The blocked state is a correct answer.

## Fields

Every field below is required except `origin`, `destination`, `window`, `days`
and `citation`. A missing required field fails the load — a silently skipped
assumption file is the same failure as an expired one with better manners.

```yaml
- id: ocean_freight.us_gulf.cn_north.2026-10     # unique across all files
  component: ocean_freight                        # see CostComponent
  value: 52.0
  unit: usd_per_mt                                # usd_per_mt | fraction | rate_per_annum
  days: 45                                        # required for rate_per_annum only
  origin: us_gulf                                 # config.ORIGIN_PORTS key; omit = any origin
  destination: cn_north                           # config.DESTINATION_PORTS key; omit = any
  window:                                         # omit = applies to every shipment window
    start: 2026-10-01
    end: 2026-10-31
  basis: "Panamax 60,000t USG–North China, own broker indication"
  source: "manual"
  entered_by: "you@example.com"
  entered_at: 2026-08-18
  expires_on: 2026-09-17                          # HARD stop; past this the row blocks again
  confidence: indicative                          # indicative | administered | provisional
  citation: "optional URL or document reference"
```

### Units are checked against the component

| component                | required unit    |
|--------------------------|------------------|
| `ocean_freight`, `elevation`, `inland_transport`, `origin_port_costs`, `destination_port_costs`, `quality_adjustment`, `processing_cost`, `energy_cost`, `plant_freight_in` | `usd_per_mt` |
| `import_duty`, `import_vat`, `marine_insurance` | `fraction` (0.03 = 3%) |
| `financing`, `working_capital` | `rate_per_annum` + `days` |

An ad-valorem rate entered as `usd_per_mt` parses cleanly and is wrong by two
orders of magnitude, so the loader rejects it.

### Selection

The most **specific** live assumption wins: route + window beats route beats
destination beats global. Two equally specific live assumptions for the same
component is an error, not a tie-break — one of them is stale, so retire it.

An **expired** assumption is never replaced by a wider one. If your
`us_gulf → cn_north` freight lapses, the row blocks; it does not silently fall
back to a global rate.

## Entering one

```bash
python scripts/enter_assumption.py \
  --component ocean_freight --value 52.0 --unit usd_per_mt \
  --origin us_gulf --destination cn_north \
  --window 2026-10-01:2026-10-31 \
  --basis "Panamax 60kt USG–N.China, broker indication" \
  --entered-by you@example.com --expires 2026-09-17 \
  --confidence indicative

python scripts/enter_assumption.py --list          # what is live, and what lapses soon
python scripts/enter_assumption.py --check         # validate every file, exit 1 on error
python scripts/enter_assumption.py --onboarding    # per-route checklist + the command for each gap
python scripts/enter_assumption.py --review        # the renewal queue, and what each lapse blocks
python scripts/enter_assumption.py --gaps          # what the page cannot rank yet (reads the DB)
```

### Set-level validation

`--check` fails (exit 1) on faults in the *files* — a unit that does not match
its component, a freight with no `origin` (it would price every leg off one
indication), a scope key that matches no route, two entries of the same scope
whose windows and lifetimes overlap, or a shipment window that ended before the
entry was made. `load_assumptions()` raises on the same set, so an unusable file
can never quietly cost a route.

Expired and expiring entries are **reported, not refused**: the record is the
audit trail, and a renewal is a decision with a person's name on it.

## What is shipped here, and what is not

`china_import_policy.yml` carries China's soybean import duty and VAT. Those
are **published policy rates**, not price estimates, and their provenance is
recorded in the entries themselves. They are still given an expiry, because
policy rates move and a stale one is as wrong as a stale freight.

Everything else is deliberately absent: ocean freight, elevation, port costs,
financing and quality differentials ship **empty**. The origin comparison page
therefore opens in a blocked state on a fresh clone, naming each missing input
and the command that supplies it. That is the intended behaviour, not an
unfinished feature.
