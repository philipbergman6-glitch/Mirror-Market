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

### Limit keys

Every key resolves through `analysis.futures.exposure`, so a limit and the
exposure table on the page cannot disagree about what the number means.
Anything not in this list raises rather than being silently skipped — a mandate
nobody is checking must not look like one that passes.

| key | unit | measures |
|---|---|---|
| `flat_price_mt` | MT | tonnes still exposed to a move in the board price |
| `basis_mt` | MT | tonnes exposed to a move in the basis, hedged tonnes included |
| `crush_mt` | MT | bean-equivalent tonnes exposed to a move in the crush margin |
| `residual_mt` | MT | physical tonnes the futures hedge does not cover |
| `month_mt` | MT | tonnes in one named delivery month |
| `first_notice_contracts` | lots | contracts open inside the first-notice window |
| `fx_usd` | USD | USD value exposed to one currency pair |
| `notional_usd` | USD | marked value of the physical book |
| `loss_usd` | USD | unrealised mark-to-market loss, management basis |
| `net_mt` | MT | net position in a commodity — kept so older files still parse |
| `unhedged_mt` | MT | same as `residual_mt` — kept for the same reason |

`scope` is a commodity, a contract symbol, an FX pair, or `"*"` for every scope
the metric has. `warn_at` is optional and **must be below** `maximum`: a
warning that fires only after the breach is not a warning, and is refused.

A limit whose exposure cannot be measured produces **no row at all** rather
than a passing one. A green line nobody checked is the most dangerous output
this workspace could produce, so the page says how many limits were configured
and how many were measured.

### The pricing convention, and why it is worth stating

Each physical position may carry `pricing:`, one of `unpriced`,
`basis_over_futures`, `formula_priced` or `flat_price`. It decides which
exposure view the tonnes land in — a flat-priced cargo has left the board,
an unpriced one has not — so it is the difference between a flat-price limit
that means something and one that does not.

Omitting it is legal and means *not stated*: the tonnes are then counted at
their most exposed reading and every line built from them says the convention
was a default rather than a statement. A **wrong** value is not legal; it would
move tonnes silently between views, which is a risk report saying something
untrue.

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
