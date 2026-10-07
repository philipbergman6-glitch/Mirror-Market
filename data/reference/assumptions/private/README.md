# Desk assumptions — the private tier

**Deliberately empty, and gitignored.** Every `*.yml` here is ignored by git
(see `.gitignore`); only this README is tracked. The committed tier one level
up carries the China import duty and VAT — published policy rates with public
provenance. Everything a desk enters itself lands here, because an entry
carries the owner's email in `entered_by` and the broker or the facility in
`basis`, and a tracked file would publish that client record on the next push
(invariant 4; B12 #409).

`scripts/enter_assumption.py` routes every desk component here and refuses a
`--file` outside it. `load_assumptions()` reads this tier only for the
`private` audience; every page builder that writes into `docs/` calls it bare
and gets the policy rates alone, so the public Origins page stays blocked by
design on every machine, the desk's included. The costed chain renders only in
the private edition, written to `data/workspace/origins.html`.

`python scripts/enter_assumption.py --check` fails if a desk component is found
in a committed file, or if anything under this directory is tracked by git.

## Why nothing is shipped here

Nothing in this stack can source these for free, and a plausible default here
would silently decide which origin the page recommends — always in the same
direction, and never looking like an error. The origin comparison therefore
opens blocked on a fresh clone, names each missing input, and prints the
command that supplies it. That is the designed behaviour: see `../README.md`
and `../ONBOARDING.md`.

**Do not hand-write a number you do not have.** An explicit zero is a
fabricated number wearing an assumption's clothes.

## Landed-cost components (`freight_and_logistics.yml`)

| component | why it is missing |
|---|---|
| `ocean_freight` | Panamax/Supramax route assessments are Baltic and Platts products. Per origin **and** per destination — US Gulf and Paranaguá to North China are different voyages, and the difference between them is routinely larger than the FOB spread the page is comparing. |
| `elevation` | Barge-to-vessel at the NOLA area. Published nowhere free, and required only by the US Gulf leg, whose AMS bid is CIF onto a barge rather than FOB into a vessel. Leaving it at zero would make the US structurally cheapest every single day. |
| `inland_transport`, `origin_port_costs` | Only where a leg's delivery term needs them; derived per route by `analysis/origins/readiness.py`. |
| `marine_insurance` | The buyer's own cover rate, as a fraction of the insured value. |
| `destination_port_costs` | Discharge, handling and storage at the destination range. |
| `financing` | Rate and carry days. Both depend on the buyer's own credit and shipment terms, so there is no market-wide number to enter — only yours. |
| `quality_adjustment` | Protein / FM / moisture differential against the destination's contract specification. This stack ingests no protein series, and the differential is **not** zero — US No. 2 Yellow and Brazilian contract standard are different specifications and a Chinese crusher pays for protein. It is left absent rather than entered as zero for exactly that reason. |

```bash
python scripts/enter_assumption.py \
  --component ocean_freight --value 52.0 --unit usd_per_mt \
  --origin us_gulf --destination cn_north \
  --window 2026-10-01:2026-10-31 \
  --basis "Panamax 60kt USG–N.China, broker indication" \
  --entered-by you@example.com --expires 2026-09-17 \
  --confidence indicative
```

## Crush-plant components (`crush_plant.yml`)

The inputs an estimated net plant margin needs, and the most confidently wrong
number this stack could produce if invented: a crusher reading "+18 USD/MT
net" acts on it.

`origin` here is a **market slug** (`argentina`, `cbot`, `brazil`, `dalian`),
not a port key — a plant's cost stack belongs to the market whose physical
legs it crushes, not to a loading berth.

The four rungs, applied to the gross physical crush in this order:

| component | unit | what it is |
|---|---|---|
| `plant_freight_in` | `usd_per_mt` | Getting the bean from the pricing point to the plant gate. Zero only if the physical bean leg is already quoted at the gate. |
| `processing_cost` | `usd_per_mt` | Hexane, labour, maintenance, depreciation — everything but energy. |
| `energy_cost` | `usd_per_mt` | Steam and power, the line that moves with the gas curve and the one worth its own rung. |
| `working_capital` | `rate_per_annum` + `days` | Charged on the **bean cost**, not on the margin — the plant is carrying beans, not margin. |

Only Argentina currently has a gross physical crush to net these against
(three administered FOB legs on one circular). The US, Brazil and China have no
complete physical triplet ingested, so a net plant margin there is blocked one
level further up and entering costs would not unblock it.

```bash
python scripts/enter_assumption.py \
  --component processing_cost --value 22.0 --unit usd_per_mt \
  --origin argentina --basis "own plant, USD/MT of beans crushed" \
  --entered-by you@example.com --expires 2026-11-30 --confidence indicative

python scripts/enter_assumption.py \
  --component working_capital --value 0.085 --unit rate_per_annum --days 30 \
  --origin argentina --basis "own facility cost, 30-day bean carry" \
  --entered-by you@example.com --expires 2026-11-30 --confidence indicative
```
