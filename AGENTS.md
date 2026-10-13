# AGENTS.md

The entry point for any coding agent, whatever the model. It points; it does
not restate. `CLAUDE.md` is the one source of the mission, the invariants and
the commands — the filename is historical, and its rules bind every model.

**Mission:** Public-data soy intelligence and private desk decision support for physical buyers making daily cargo, basis, origin, and hedge decisions.

## Read in this order

1. `CLAUDE.md` — mission, what the product is not, invariants, commands. Always.
2. `ARCHITECTURE.md` → "Capability status" — what is specified, implemented,
   tested, deployed and operationally verified, each with its evidence. A spec
   is not a build and a merged module is not a running one; check the row
   before assuming either.
3. Then the doc for the area you are touching:

| Touching | Read first |
|---|---|
| A fetcher or a source | `LAYERS.md` (mandatory), then the module's fixture tests |
| Storage, history, orchestration, any module's owner | `ARCHITECTURE.md` |
| What a price is, FX, spreads, crush | `ARCHITECTURE.md` "Price semantics" and "FX alignment"; `pricing/` |
| Pages, layout, anything visual | `DESIGN.md` |
| Data age, refresh slots, deploy timing | `LATENCY.md`; `.github/workflows/` |
| Domain words (weather, hazards, desk terms) | `CONTEXT.md` |
| Scope — whether something belongs at all | `ROADMAP.md` |
| Desk-entered files | `data/reference/*/README.md` |

## Status and handoff

- Status lives in one place: the capability-status table. A change that moves
  a capability's evidence edits its row in the same PR. Do not start a second
  status or state file.
- Work is tracked as GitHub issues; decisions and handoffs are recorded on the
  issue and in the PR body, where the next session finds them.
- The checks CI runs are `.github/workflows/ci.yml`. Report what you ran and
  what it returned; never fill a status cell with evidence you did not see.
