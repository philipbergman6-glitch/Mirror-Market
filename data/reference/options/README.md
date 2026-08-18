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
