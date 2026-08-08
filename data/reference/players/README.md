# Soy player knowledge base

Static reference data on physical soy-complex players (map #86; rendered by #74).
One YAML file per scope so parallel research tickets never conflict:

Sell-side (origins):

- `global.yml` — international trading houses (ABCD+) — #87
- `brazil.yml` — #88
- `us.yml` — #89
- `argentina.yml` — #90
- `paraguay.yml` — #91
- `secondary_origins.yml` — Canada, Ukraine/Russia, Uruguay, Bolivia — #108

Buy-side (destinations) and mixed:

- `india.yml` — importers/crushers/refiners — #92
- `china.yml` — importers/crushers/state buyers — #104
- `asia_importers.yml` — JP/KR/TW/VN/ID/TH — #105
- `europe.yml` — EU/UK/Norway import crushers — #106
- `mena.yml` — Egypt/Turkey/Iran/North Africa — #107
- `africa.yml` — South Africa + Nigeria — #109
- `latam_importers.yml` — Mexico + Andean/Central America — #110

## Schema

Each file is a YAML list of player entries:

```yaml
- name: ""            # canonical company name
  aka: []             # optional: former names, tickers, JV shorthand
  scope: ""           # file the entry lives in: global | brazil | us | argentina |
                      # paraguay | india | china | asia_importers | europe | mena |
                      # secondary_origins | africa | latam_importers
  side: ""            # seller | buyer | both — international soy-complex trade role
  website: ""         # optional: corporate site (public contact surface for #111)
  ownership: ""       # public/private/co-op/state + parent(s), JV partners with stakes if known
  roles: []           # e.g. originator, crusher, refiner, exporter, importer, terminal-operator
  products: []        # beans, meal, oil (soy complex only)
  footprint: ""       # origination regions, plants, export terminals/ports (prose)
  tier: 1             # optional: majors only (~40-60 across all files); absent = tier 2.
                      # Gates curated-activity coverage (#111 decision 8).
  destinations: ""    # principal destination markets, or "unverified"
  destinations_structured:  # optional: queryable form of `destinations` (#122). Derived
                      # from the entry's own prose only — omit when purely unverified;
                      # hedged inferences stated in the prose are allowed with the
                      # hedge carried into `note`. Never fabricate.
    - code: ""        # ISO-3166 alpha-2 (uppercase), or region enum where sources
                      # don't name countries: EU | MENA | SE-ASIA | ASIA | LATAM |
                      # AFRICA | GLOBAL (see scripts/validate_players.py)
      products: []    # non-empty subset of beans/meal/oil for this destination
      note: ""        # optional: share figures, hedges ("inferred", "unverified split")
  size_evidence: ""   # best FREE evidence of scale: filings, disclosed capacity, Trase,
                      # association data. Never market-share estimates (not free — see #45).
  confidence: ""      # observed | inferred  (worst-case across the entry's claims)
  as_of: ""           # YYYY-MM-DD the research was done
  citations:          # per-claim where practical; every entry needs >= 1
    - url: ""
      accessed: ""    # YYYY-MM-DD
      supports: ""    # which claim(s) this URL backs
  notes: ""           # caveats, what could NOT be verified, transshipment honesty, etc.
  contacts:           # optional (T3 #124): company-level always, desk-level only when
                      # published, NEVER named individuals (#111 decision 5)
    website: ""
    contact_url: ""   # corporate contact / commercial page
    offices: []
    trade_desks:      # only when publicly listed
      - {label: "", email: "", phone: "", office: ""}
    accessed: ""      # YYYY-MM-DD
  activity:           # optional (T4 #125): curated dated notes, tier-1 only, newest first
    - date: ""        # YYYY-MM-DD
      category: ""    # ma | capacity | trade | policy | distress
      headline: ""
      detail: ""      # optional
      citation: {url: "", accessed: ""}
```

Schema is enforced at build time by `scripts/validate_players.py` (hard-fails on
unknown codes/regions, bad enums, malformed dates, activity without citations).
Cross-listing one company under two scopes (e.g. CHS in global.yml + us.yml) is
allowed; a duplicate name within the same scope fails. When one corporate group
appears in several files — cross-listed, or a parent plus its named asset (e.g.
Mitsui & Co. and United Grain) — put `tier: 1` on ONE entry only: the one that
best represents the operating group (usually global.yml; for Sodrugestvo the
post-split regional entry, since the global entry documents a defunct structure).

## Rules

- Tag every claim `observed` (seen in a primary source) vs `inferred`; entry-level
  `confidence` is the weakest tag inside it.
- No fabricated numbers. If scale evidence doesn't exist free, say so in `notes`.
- Keep entries "as of" dated — this is slow-moving reference data, not a feed.
