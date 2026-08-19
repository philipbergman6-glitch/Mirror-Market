# Fixture assumption set — TESTS ONLY

Every number in `routes.yml` is **invented for a test**. None of it is a market
observation, a broker indication, or a rate anybody quoted.

It exists because the success path of the origin page cannot otherwise be
looked at: the shipped `data/reference/assumptions/` directory ships empty by
design, so on any clone the page correctly renders as blocked, and a renderer
that has never been exercised against a *complete* route is a renderer nobody
has seen work.

Two rules keep that safe:

* nothing here is ever loaded by the site — `config.ASSUMPTIONS_DIR` points at
  `data/reference/assumptions/`, and the `MIRROR_ASSUMPTIONS_DIR` override that
  reaches this directory is set by tests and by the dev loop, never in CI;
* nothing here may be copied into `data/reference/assumptions/`. A fixture
  freight rate in a shipped file would be a fabricated default carrying a
  test's credibility, which is worse than one carrying none.

The `entered_by` on every entry says `fixture` rather than a person, so a row
rendered from this set is identifiable on sight in any screenshot.
