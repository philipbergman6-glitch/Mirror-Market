# Import profiles

One small YAML file per counterparty, mapping **their** export's column names,
sign convention and product codes onto this project's vocabulary.

Files here are **gitignored**: a profile names your broker and their account
column. Only this README is tracked.

## Why a profile at all

A client's export is the one file this project cannot control the shape of.
The dangerous failure is not a crash — it is a clean parse of the wrong thing.
A short 68 lots read as a long 68 is a 136-lot error that looks exactly like a
position. A profile makes every one of those decisions explicit, written once,
and reviewable by a person before any row becomes a book.

## File shape

```yaml
name: my-broker               # what you pass to --profile
source: "Your Clearing LLC"   # who produced the file
kind: futures                 # futures | physical
quantity_sign: side_column    # signed | side_column — declared, never inferred
date_format: '%Y-%m-%d'       # tried once; a second format is never attempted
delimiter: ','

columns:                      # our field: their column header
  trade_date: TradeDate
  contract: Ticker            # futures
  side: B/S                   # required when quantity_sign is side_column
  quantity: Lots
  price: AvgPx
  account: Acct               # optional

side_values:                  # their vocabulary -> ours
  b: long
  s: short

symbol_map:                   # optional: their ticker -> ours
  SX26: ZSX26

# physical exports only
# commodity: Product
# unit: Unit                  # or declare default_unit
# default_unit: MT
# location: Loc
# commodity_map:
#   SOJ: Soybeans
```

Required columns are `trade_date`, `quantity`, `price` plus `contract`
(futures) or `commodity` (physical). A physical profile must map a `unit`
column or declare a `default_unit` — a quantity with no unit is a number, not a
tonnage.

## The workflow

**Two steps, and the first writes nothing.**

```bash
# what profiles exist
python scripts/import_positions.py --list-profiles

# the dry run: what would be imported, what would be rejected, and why
python scripts/import_positions.py --file exports/2026-08-19.csv --profile my-broker

# write it out, having read the dry run
python scripts/import_positions.py --file exports/2026-08-19.csv --profile my-broker \
  --apply --out data/reference/positions/imported-2026-08-19.yml
```

Exit codes: `0` clean, `1` something was rejected, `2` the file or profile
could not be read at all — so a nightly import can gate on it.

`--apply` **refuses** a report with rejected rows unless `--allow-partial` is
also given. A partial book is a book quietly short a position; asking for one
out loud is the least this can require, and the resulting file says on its face
how many rows were dropped.

## Every refusal is the same rule: nothing is guessed

* A **missing required column** refuses the whole file before any row is read.
  Per-row failures there would read as bad data rather than a bad mapping.
* An **unrecognised side value** rejects that row. It does not pick one.
* A **date that does not match** `date_format` rejects that row. A second
  format is never tried — that is how one file's March becomes another's April.
* An **unmapped product code** rejects that row. `SOJA` is probably soybeans;
  probably is not a book.
* A **blank number** is a rejection, never a zero.
* A **column the profile does not claim** is reported, not ignored. It might be
  a sign, a commission or a second account.

## Re-importing is safe

Every row's reference is `<sha256[:8]>:<row number>`, derived from the file's
own bytes. The same export read twice produces the same references, so a
re-import is idempotent and a changed file is visible as a different digest.

## After the import

The pricing convention on a physical row is **not** in anybody's export, so
imported physical positions carry a commented `pricing:` line for you to fill
in. Until it is stated, those tonnes are counted at their most exposed reading
and every exposure line built from them says so. See
`data/reference/positions/README.md`.
