# Options — the hand-entered ladder

**Deliberately empty.** No source this project ingests publishes an option
chain: not a strike, not a premium, not an implied volatility, for any contract
on CBOT, CME or ICE. That was checked against the incumbent price provider
directly — `yfinance.Ticker(t).options` returns `()` for `ZS=F`, `ZM=F`, `ZL=F`,
`ZC=F`, `SB=F`, `CT=F` and for a named contract like `ZSX26.CBT` — and it is
asserted in `tests/test_futures_options.py`. The page reports the chain
*unavailable* rather than rendering an empty ladder, because an empty ladder
reads as "no options traded", which is a claim about the market rather than
about us.

This directory is the other half of that sentence. An option you were quoted is
a number only you have, exactly like a position, so it arrives the same way a
position does: as a YAML document you write. Every row is stamped
`PriceType.MANUAL` and carries your own `source` string, so nothing here can be
confused with something the pipeline sourced.

Read by `analysis/futures/options.py:load_ladder()` at site build time. A
missing directory is an empty ladder; a **present but malformed** file raises
and fails the build, because "nothing entered" and "something entered wrongly"
are different states and only one of them is safe to render as empty.

## Shape

```yaml
options:
  - underlying: ZSX26          # a named contract — the option's, not the front month
    right: call                # call | put
    strike: 1200               # native units of the underlying (cents/bu for ZS)
    expiry: 2026-10-23         # the OPTION's expiration, not the future's last trade
    style: american            # american (default) | european
    quoted_on: 2026-08-18
    quoted_at: 2026-08-18T20:40:00+00:00     # optional, but must be timezone-aware
    source: "Broker XYZ, 15:40 CT screen"    # required — who quoted it
    # Exactly one of the two below. The other is derived.
    premium: 24.5              # native units, same as the strike
    implied_volatility: null   # decimal, e.g. 0.185 for 18.5%
    note: "against the Nov hedge"
```

## Three things worth knowing before you trust the output

**Expiry is yours to supply.** This project encodes futures termination rules
off the exchange rulebooks, but it does **not** encode option expiry rules, so
`expiry` is required and has no default. It is not the underlying future's last
trade date — a Nov soybean option typically expires in late October.

**The value is Black-76, and listed grain options are American.** The
early-exercise premium is not modelled, so a value printed here is a *floor*
for an American option rather than a price for one. Every row says so.

**One volatility, no smile.** Black-76 assumes a single constant volatility to
expiry. Agricultural options have a pronounced smile that steepens into weather
markets, so a value struck well away from the money is systematically wrong.
Entering the `implied_volatility` your broker quoted for *that strike* avoids
this; entering one at-the-money vol and reading values across a ladder does not.


## The source timestamp

`quoted_at` is optional and, when given, must be **timezone-aware** and must
fall on `quoted_on`. A naive timestamp is refused rather than assumed to be
local time — whose local? the desk's, the broker's, or the CI runner's, and
they are hours apart. A quote with no `quoted_at` says so on the row: an option
premium moves intraday with the underlying, so a date alone cannot be pinned to
a board session.

## Importing an exported ladder

If your broker can export the chain, you need not retype it.
`analysis.futures.options.chain_from_csv` reads a flat file:

```
right,strike,expiry,premium,implied_volatility,quoted_at
call,1150,2026-10-23,42.50,,2026-08-19T14:45:00+00:00
put,1100,2026-10-23,,0.2450,2026-08-19T14:45:00+00:00
```

Optional extra columns: `bid`, `ask`, `settlement`, `volume`, `open_interest`.

Four refusals, all the same rule — nothing is invented:

* a row with **neither** a premium nor an implied volatility is refused, not
  left unpriced;
* a row with **both** is refused, because two inconsistent numbers on one row
  leave nothing saying which was believed;
* a ladder with **no timestamp** anywhere — neither in the file nor supplied by
  the caller — is refused, because it cannot be compared with a board session;
* a ladder carrying **two different timestamps** is refused. One chain is one
  moment, and a Greeks table struck across two sessions is not a Greeks table.

Every imported row is stamped `PriceType.MANUAL`. A file somebody sent us is
not a feed this project ingests, and it is never rendered as a settlement.

## Where these files are rendered

The entered ladder appears **only in the private edition** of the workstation,
written to `data/workspace/workstation.html`, outside `docs/`. The public page
carries the chain's absence, the model and the model's limits — facts about
this project — and none of your quotes.
