# Soy player knowledge base

Static reference data on physical soy-complex players (map #86; rendered by #74).
One YAML file per scope so parallel research tickets never conflict:

- `global.yml` — international trading houses (ABCD+) — #87
- `brazil.yml` — #88
- `us.yml` — #89
- `argentina.yml` — #90
- `paraguay.yml` — #91
- `india.yml` — #92

## Schema

Each file is a YAML list of player entries:

```yaml
- name: ""            # canonical company name
  aka: []             # optional: former names, tickers, JV shorthand
  scope: ""           # global | brazil | us | argentina | paraguay | india
  ownership: ""       # public/private/co-op/state + parent(s), JV partners with stakes if known
  roles: []           # e.g. originator, crusher, refiner, exporter, importer, terminal-operator
  products: []        # beans, meal, oil (soy complex only)
  footprint: ""       # origination regions, plants, export terminals/ports (prose)
  destinations: ""    # principal destination markets, or "unverified"
  size_evidence: ""   # best FREE evidence of scale: filings, disclosed capacity, Trase,
                      # association data. Never market-share estimates (not free — see #45).
  confidence: ""      # observed | inferred  (worst-case across the entry's claims)
  as_of: ""           # YYYY-MM-DD the research was done
  citations:          # per-claim where practical; every entry needs >= 1
    - url: ""
      accessed: ""    # YYYY-MM-DD
      supports: ""    # which claim(s) this URL backs
  notes: ""           # caveats, what could NOT be verified, transshipment honesty, etc.
```

## Rules

- Tag every claim `observed` (seen in a primary source) vs `inferred`; entry-level
  `confidence` is the weakest tag inside it.
- No fabricated numbers. If scale evidence doesn't exist free, say so in `notes`.
- Keep entries "as of" dated — this is slow-moving reference data, not a feed.
