# Positions — the entered book

**Deliberately empty.** This project ingests no account, no broker statement and
no clearing feed. A position here can only come from you, and an empty directory
is the correct state for a clone that has entered nothing — the workstation says
so on the page rather than showing a zero.

Every `*.yml` file in this directory is read by `analysis/futures/positions.py`
at site build time. A missing directory is an empty book; a **present but
malformed** file raises and fails the build, because "nothing entered" and
"something entered wrongly" are different states and only one of them is safe to
render as zero.

## Shape

```yaml
# One document may carry any combination of the three keys.

physical:
  - commodity: Soybeans              # must match a CONTRACT_SPECS key
    side: long                       # long = you own it / bought forward unpriced
    quantity: 12000
    unit: mt                         # mt | short_ton | bushel | lb
    average_cost_usd_mt: 402.50
    currency: USD                    # non-USD requires fx_pair
    fx_pair: null                    # e.g. BRL/USD — USD per unit of home currency
    mark_contract: ZSX26             # which board leg this is marked against
    current_basis_usd_mt: -12.5      # physical minus board, USD/MT
    # The three entry-time levels an attribution needs. All or nothing: without
    # them the futures/basis/FX split is arithmetic over unknowns and is
    # withheld with the reason stated.
    entry_futures_usd_mt: 415.00
    entry_basis_usd_mt: -8.0
    entry_fx_rate: null
    location: Paranagua
    note: "Sep-Oct loading, unpriced"

futures:
  # Either a list of fills (average cost and realised P&L are then derived by
  # weighted-average cost) ...
  - contract: ZSX26
    account: house
    fills:
      - {date: 2026-08-04, side: sell, quantity: 60, price: 1172.25}
      - {date: 2026-08-07, side: sell, quantity: 28, price: 1180.00}
      - {date: 2026-08-11, side: buy,  quantity: 20, price: 1165.50}
  # ... or a stated position, taken at its word and labelled as not derived.
  - contract: ZMZ26
    contracts: -40                   # signed: negative is short
    average_price: 308.40
    realised_usd: 0

limits:
  # Checked and reported on the page. Never enforced — this software does not
  # stop anyone doing anything; it says which line was crossed.
  - {key: net_mt,       scope: Soybeans, maximum: 15000, note: "desk mandate"}
  - {key: notional_usd, scope: "*",      maximum: 8000000}
  - {key: loss_usd,     scope: "*",      maximum: 250000}
```

Limit keys: `net_mt`, `unhedged_mt`, `notional_usd`, `loss_usd`. Anything else
raises rather than being silently skipped.

## CSV import

For a broker or ERP export, `analysis.futures.positions.positions_from_csv`
reads a flat file with the columns

```
kind,commodity,contract,side,quantity,unit,price,trade_date,currency,fx_pair,basis_usd_mt,location
```

`kind=futures` rows become fills against their named contract, so several rows
for one contract accumulate into one position at weighted-average cost.
`kind=physical` rows become physical positions.

## What the marks mean

Marks are **delayed daily closes, not proven exchange settlements**. The P&L
this workspace produces is a management figure. It is not a margin calculation
and it will not match a clearing statement.
