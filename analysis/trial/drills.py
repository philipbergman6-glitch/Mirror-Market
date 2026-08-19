"""The five failure drills (Phase 5, requirement 9).

A drill simulates a failure and then checks that the product *degrades the way
it claims to*. That is a different exercise from a unit test, and both are
needed: the unit tests pin each mechanism in isolation, and the drills assert
that the whole chain — grading, freshness, page, promotion contract — reaches
the trader as a visible degradation rather than as a silently wrong number.

The five, and what each is really testing:

1. **Critical source outage.** ``prices`` returns nothing. The claim under test
   is that this records ``failed``, preserves the prior ``last_success``, and
   never stamps a fresh one. A layer that recorded ``success`` on an empty fetch
   would leave every downstream surface reading yesterday's number as today's.
2. **Partial key coverage.** ``weather`` is run twice against its 19-region
   catalog: once at its ``LAYER_MIN_KEYS`` floor and once one region under it.
   The claim is that the two verdicts differ — at the floor the layer is still
   usable and grades ``success``, but records coverage as 14/19 rather than
   self-reporting a full house; under the floor it grades ``incomplete`` and
   holds ``last_success`` back. Both halves matter: asserting only the first
   would pass against a build that had lost the demotion, and asserting only the
   second would pass against one that demoted every partial run. This is the
   failure mode #212 found in Layer 16, where a half-dark layer took the success
   path because it had *some* rows.
3. **Stale source payload.** ``safex`` fetches cleanly and returns a frame a
   month old. The claim is ``stale``, with ``last_success`` held back so the
   layer ages out on its own. This is the frozen-upstream case: gate 1 passes
   every day forever.
4. **Page-generation failure.** A page raises. The claim is that a dated
   tombstone replaces it — never yesterday's file — and that the promotion
   contract *rejects* the tombstoned candidate.
5. **Deployment failure.** The candidate is incomplete. The claim is that
   ``verify_site_candidate`` refuses it, which is what keeps the last good
   public edition live.

Drills 1-3 run against ``main``'s real grading functions with the freshness
write captured, so what they verify is the code the daily pipeline runs, not a
reimplementation of it. Drills 4-5 run against the real Jinja template and the
real ``trust.site_promotion`` contract.

Nothing here mutates the production database, writes into ``docs/``, or touches
the network. A drill that could leave the product degraded would be a worse
outage than the one it was rehearsing.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from analysis.trial.domain import TrialError

log = logging.getLogger(__name__)

__all__ = [
    "DRILLS",
    "DrillResult",
    "drill_critical_source_outage",
    "drill_deployment_failure",
    "drill_page_generation_failure",
    "drill_partial_key_coverage",
    "drill_stale_payload",
    "run_all_drills",
    "run_drill",
]


@dataclass(frozen=True)
class DrillResult:
    """What was simulated, what should have happened, and what did.

    ``expected`` and ``observed`` are both recorded even when the drill passes.
    A drill result that only says "pass" is unreadable six weeks later, and the
    whole point of running these inside the trial is that a trader is shown the
    degraded product and asked what they can tell.
    """

    drill: str
    title: str
    simulated: str
    expected: str
    observed: str
    passed: bool
    evidence: tuple[str, ...] = ()
    trader_prompt: str = ""

    def __post_init__(self) -> None:
        for name, value in (
            ("simulated", self.simulated),
            ("expected", self.expected),
            ("observed", self.observed),
        ):
            if not value.strip():
                raise TrialError(f"drill {self.drill} needs a non-empty {name}")

    @property
    def verdict(self) -> str:
        return "pass" if self.passed else "FAIL"

    def to_dict(self, **_: Any) -> dict[str, Any]:
        return {
            "drill": self.drill,
            "title": self.title,
            "simulated": self.simulated,
            "expected": self.expected,
            "observed": self.observed,
            "passed": self.passed,
            "verdict": self.verdict,
            "evidence": list(self.evidence),
            "trader_prompt": self.trader_prompt,
        }


@contextmanager
def _captured_freshness() -> Iterator[list[dict[str, Any]]]:
    """Run ``main``'s grading with the freshness write intercepted.

    The drill has to see what *would* have been written without writing it: a
    rehearsal that marked the real ``prices`` layer failed would take the whole
    product down to prove that the product goes down. ``main``'s module-level
    failure sets are cleared either side, because they are read together with
    the freshness row and a leftover entry from a previous drill would make the
    next one lie.
    """
    import main

    calls: list[dict[str, Any]] = []

    def capture(
        layer_name: str,
        rows_fetched: int = 0,
        status: str = "success",
        keys_returned: int | None = None,
        keys_expected: int | None = None,
        clock: Any = None,
    ) -> None:
        # `clock` is accepted and ignored: these drills grade the freshness
        # *verdict*, and the latency stamps riding along with it are a
        # different question asked by latency/. Accepting it keeps this
        # stand-in substitutable for the real save_freshness — a double that
        # rejects an argument the real function takes stops being a double.
        calls.append({
            "layer": layer_name,
            "rows": rows_fetched,
            "status": status,
            "keys_returned": keys_returned,
            "keys_expected": keys_expected,
        })

    original = main.save_freshness
    main.save_freshness = capture
    for bucket in (
        main._HARD_FAILURES,
        main._NO_PUBLICATION,
        main._STALE_LAST_KNOWN_GOOD,
        main._INCOMPLETE_KEY_COVERAGE,
    ):
        bucket.clear()
    try:
        yield calls
    finally:
        main.save_freshness = original
        for bucket in (
            main._HARD_FAILURES,
            main._NO_PUBLICATION,
            main._STALE_LAST_KNOWN_GOOD,
            main._INCOMPLETE_KEY_COVERAGE,
        ):
            bucket.clear()


def _frame(days_old: int, rows: int = 3) -> pd.DataFrame:
    """A small, plausible OHLCV frame whose newest bar is ``days_old`` days back."""
    end = pd.Timestamp(date.today() - timedelta(days=days_old))
    index = pd.date_range(end=end, periods=rows, freq="D", name="Date")
    return pd.DataFrame(
        {
            "Open": [100.0] * rows,
            "High": [101.0] * rows,
            "Low": [99.0] * rows,
            "Close": [100.5] * rows,
            "Volume": [1000.0] * rows,
        },
        index=index,
    )


# ---------------------------------------------------------------------------
# 1 — a critical source goes dark
# ---------------------------------------------------------------------------
def drill_critical_source_outage() -> DrillResult:
    import main

    with _captured_freshness() as calls:
        success = main._finalize_layer("prices", {})
        hard_failed = "prices" in main._HARD_FAILURES

    row = calls[-1] if calls else {}
    status = row.get("status")
    passed = (not success) and status == "failed" and hard_failed
    return DrillResult(
        drill="critical_source_outage",
        title="A critical source goes dark",
        simulated="Layer 1 (prices) returned zero frames — the CBOT board is unreachable.",
        expected=(
            "status='failed', the run counted as a hard failure, and no fresh last_success "
            "stamped, so every surface keeps naming the last known good date."
        ),
        observed=(
            f"_finalize_layer returned {success!r}; freshness recorded status={status!r}; "
            f"hard-failure set {'contains' if hard_failed else 'does NOT contain'} 'prices'."
        ),
        passed=passed,
        evidence=(f"main._finalize_layer('prices', {{}}) -> {success!r}", f"freshness row: {row}"),
        trader_prompt=(
            "Open the headline and the CBOT page. Without being told what broke: can you say "
            "which number is missing, how old the last good one is, and whether you would "
            "quote off this page?"
        ),
    )


# ---------------------------------------------------------------------------
# 2 — most of a layer answers, some of it never does
# ---------------------------------------------------------------------------
def drill_partial_key_coverage() -> DrillResult:
    """Both sides of the key floor, in one drill.

    A partial run has two legitimate verdicts and the distinction is the whole
    point of the check. Above the floor the layer is still usable, so it grades
    ``success`` — but the coverage pair must record what actually came back,
    because the failure this guards against is a fetcher self-reporting
    fourteen-of-fourteen when five regions were never asked (the shape of #212).
    Below the floor the layer is not usable and must demote to ``incomplete``.

    Asserting only the first half would pass against a build that had lost the
    demotion entirely; asserting only the second would pass against one that
    demoted every partial run and made the weather layer permanently red. So the
    drill runs the layer twice — at the floor and one key under it — and passes
    only if the two verdicts differ in the direction the config calls for.
    """
    import config
    import main

    catalog = config.LAYER_KEY_CATALOGS.get("weather", {})
    expected_keys = len(catalog) or 19
    floor = config.LAYER_MIN_KEYS.get("weather", 1)
    names = list(catalog) or [f"region_{i}" for i in range(expected_keys)]

    def _run(returned: int) -> tuple[bool, dict[str, Any], bool]:
        data = {name: _frame(0) for name in names[:returned]}
        with _captured_freshness() as calls:
            graded = main._finalize_layer("weather", data)
            demoted = "weather" in main._INCOMPLETE_KEY_COVERAGE
        return graded, (calls[-1] if calls else {}), demoted

    # Above (or at) the floor: usable, but coverage must not claim a full house.
    at_floor = max(1, min(floor, expected_keys))
    ok_graded, ok_row, ok_demoted = _run(at_floor)
    above_ok = (
        ok_graded is True
        and ok_row.get("status") == "success"
        and not ok_demoted
        and ok_row.get("keys_returned") == at_floor
        and ok_row.get("keys_expected") == expected_keys
    )

    # One key under the floor: not usable, must demote rather than grade green.
    under = max(0, at_floor - 1)
    low_graded, low_row, low_demoted = _run(under)
    below_ok = low_graded is False and low_row.get("status") == "incomplete" and low_demoted

    return DrillResult(
        drill="partial_key_coverage",
        title="Part of a layer never answers",
        simulated=(
            f"Layer 5 (weather) run twice against a floor of {floor}: once returning "
            f"{at_floor} of {expected_keys} regions, once returning {under}. The absent "
            "regions failed transport rather than returning zero rows."
        ),
        expected=(
            f"At {at_floor} keys: status='success' but coverage recorded as "
            f"{at_floor}/{expected_keys}, not a self-reported full house. At {under} keys: "
            "status='incomplete', the run demoted, and no fresh last_success stamped."
        ),
        observed=(
            f"at floor -> status={ok_row.get('status')!r}, "
            f"coverage={ok_row.get('keys_returned')}/{ok_row.get('keys_expected')}, "
            f"returned {ok_graded!r}; below floor -> status={low_row.get('status')!r}, "
            f"coverage={low_row.get('keys_returned')}/{low_row.get('keys_expected')}, "
            f"returned {low_graded!r}, demoted={low_demoted!r}."
        ),
        passed=above_ok and below_ok,
        evidence=(
            f"at-floor freshness row: {ok_row}",
            f"below-floor freshness row: {low_row}",
            f"LAYER_MIN_KEYS['weather'] = {floor}, catalog = {expected_keys} regions",
        ),
        trader_prompt=(
            "Open the headline weather section. Can you tell that regions are missing, "
            "or does it read like a complete picture with a quiet week?"
        ),
    )


# ---------------------------------------------------------------------------
# 3 — the upstream freezes
# ---------------------------------------------------------------------------
def drill_stale_payload(layer: str = "safex") -> DrillResult:
    import config
    import main

    budget = config.LAYER_MAX_DATA_AGE_DAYS.get(layer)
    if budget is None:
        raise TrialError(
            f"{layer} carries no LAYER_MAX_DATA_AGE_DAYS budget, so a stale payload drill "
            "against it would assert a check that deliberately does not exist"
        )
    age = budget + 21
    data = {"Soybeans": _frame(age)}

    with _captured_freshness() as calls:
        success = main._finalize_layer(layer, data)
        stale = layer in main._STALE_LAST_KNOWN_GOOD

    row = calls[-1] if calls else {}
    status = row.get("status")
    passed = (not success) and status == "stale" and stale
    return DrillResult(
        drill="stale_payload",
        title="The upstream freezes and keeps serving",
        simulated=(
            f"Layer '{layer}' fetched cleanly and returned rows whose newest observation is "
            f"{age} days old, against its {budget}-day budget — HTTP 200, frozen content."
        ),
        expected=(
            "status='stale', the rows still stored, and last_success held back so the layer "
            "ages out of its own window instead of reading green forever."
        ),
        observed=(
            f"_finalize_layer returned {success!r}; freshness recorded status={status!r}; "
            f"stale set {'contains' if stale else 'does NOT contain'} {layer!r}."
        ),
        passed=passed,
        evidence=(f"freshness row: {row}", f"LAYER_MAX_DATA_AGE_DAYS[{layer!r}] = {budget}"),
        trader_prompt=(
            f"Open the market page fed by '{layer}'. Does the page tell you the number is a "
            "month old, and does the propagation ledger show that leg as dark rather than flat?"
        ),
    )


# ---------------------------------------------------------------------------
# 4 — a page fails to build
# ---------------------------------------------------------------------------
def drill_page_generation_failure(output_dir: str | Path | None = None) -> DrillResult:
    """Render a real tombstone and prove the promotion contract refuses it.

    The tombstone is produced by ``scripts.generate_site._tombstone`` — the same
    function the daily build calls when a page raises — so what the contract is
    shown here is the real artifact, not a stand-in with the word "tombstone" in
    it.
    """
    import tempfile

    from analysis.trial.domain import utc_now
    from scripts.generate_site import _tombstone
    from trust.site_promotion import expected_site_paths, verify_site_candidate

    root = Path(output_dir) if output_dir else Path(tempfile.mkdtemp(prefix="mm-drill-"))
    relpath = "origins.html"
    path = _tombstone(
        root,
        relpath,
        "origins",
        "ValueError: simulated page-generation failure (Phase 5 drill)",
        [],
        utc_now(),
    )
    html = path.read_text(encoding="utf-8")

    # Every other expected page is supplied as a minimal but *valid* stand-in, so
    # the only thing the contract can object to is the tombstone itself.
    pages = {url: _minimal_page(url) for url in expected_site_paths()}
    pages[relpath] = html
    verdict = verify_site_candidate(pages)
    tombstone_failures = tuple(f for f in verdict.failures if "tombstone" in f)
    passed = not verdict.verified and bool(tombstone_failures)

    return DrillResult(
        drill="page_generation_failure",
        title="A page fails to build",
        simulated=(
            "The origins page raised during generation; the real tombstone path ran and wrote a "
            f"dated placeholder to {relpath}."
        ),
        expected=(
            "A dated tombstone stands at the page's URL rather than yesterday's file, and the "
            "promotion contract rejects the whole candidate because of it."
        ),
        observed=(
            f"verify_site_candidate verified={verdict.verified}; "
            f"failures naming a tombstone: {list(tombstone_failures) or 'none'}."
        ),
        passed=passed,
        evidence=(
            f"tombstone written to {path}",
            f"contract failures: {list(verdict.failures)[:4]}",
        ),
        trader_prompt=(
            "You have been given yesterday's public site because today's was blocked. Can you "
            "tell from the page that it is not today's, and would you have noticed unprompted?"
        ),
    )


def _minimal_page(url: str) -> str:
    """A page that satisfies the contract's structural checks and nothing more.

    Deliberately not a rendered page: the drill is about the *tombstone* being
    rejected, so every other page must be uncontroversial. It carries a valid
    generation stamp and, for the headline, the briefing block and the layer and
    benchmark metadata the contract insists on.
    """
    from datetime import timezone

    import config
    from analysis.trial.domain import utc_now

    now = utc_now().astimezone(timezone.utc)
    stamp = now.isoformat()
    today = now.date().isoformat()
    head = (
        "<html><head>"
        f'<meta name="mirror-market-generated-at" content="{stamp}">'
    )
    if url != "index.html":
        return head + "</head><body></body></html>"

    benchmarks = "".join(
        f'<span data-benchmark="{name}" data-as-of="{today}"></span>'
        for name in ("Soybeans", "Soybean Oil", "Soybean Meal")
    )
    layers = "".join(
        f'<tr data-layer="{layer[0]}"></tr>' for layer in config.PRODUCTION_LAYERS
    )
    return (
        head
        + f'<meta name="mirror-market-layer-count" content="{len(config.PRODUCTION_LAYERS)}">'
        + "</head><body>"
        + '<div id="briefing"><pre class="briefing">drill placeholder briefing</pre></div>'
        + benchmarks
        + f'<span data-derived="crush" data-aligned="true" data-as-of="{today}"></span>'
        + f"<table>{layers}</table>"
        + "</body></html>"
    )


# ---------------------------------------------------------------------------
# 5 — the deploy fails
# ---------------------------------------------------------------------------
def drill_deployment_failure() -> DrillResult:
    """An incomplete candidate must be refused, which is what keeps yesterday live.

    Two things are checked, and the second is the one that actually protects a
    trader. First: a candidate missing a page fails the contract. Second: the
    private editions — the desk opportunity board and the private trial
    dashboard — are not in ``expected_site_paths()`` at all, so no deploy path
    can carry them even on a day when everything else works.
    """
    import config
    from trust.site_promotion import expected_site_paths, verify_site_candidate

    paths = tuple(expected_site_paths())
    dropped = paths[-1]
    pages = {url: _minimal_page(url) for url in paths if url != dropped}
    verdict = verify_site_candidate(pages)
    missing_named = tuple(f for f in verdict.failures if dropped in f)

    private_dirs = (
        Path(config.OPPORTUNITY_PRIVATE_OUTPUT_DIR).resolve(),
        Path(config.TRIAL_PRIVATE_OUTPUT_DIR).resolve(),
    )
    docs_root = (Path(config.__file__).parent / "docs").resolve()
    private_outside_docs = all(not _is_within(d, docs_root) for d in private_dirs)
    no_private_paths = not any(
        "workspace" in url or "trial" in url.split("/")[0] for url in paths
    )

    passed = (
        not verdict.verified
        and bool(missing_named)
        and private_outside_docs
        and no_private_paths
    )
    return DrillResult(
        drill="deployment_failure",
        title="The deploy is blocked",
        simulated=(
            f"The candidate was built without {dropped!r} — the shape a half-finished or failed "
            "generation leaves behind."
        ),
        expected=(
            "The promotion contract refuses the candidate, so nothing is uploaded and the last "
            "trustworthy public edition stays live; and no private surface is in the contract's "
            "path list at all."
        ),
        observed=(
            f"verified={verdict.verified}; failure naming the missing page: "
            f"{list(missing_named) or 'none'}; private render dirs outside docs/: "
            f"{private_outside_docs}; private paths in the contract: {not no_private_paths}."
        ),
        passed=passed,
        evidence=(
            f"expected_site_paths() -> {len(paths)} URLs",
            f"private dirs: {[str(d) for d in private_dirs]}",
        ),
        trader_prompt=(
            "Today's edition never deployed. Looking at the site you have, can you tell how old "
            "it is, and did anything tell you it was not refreshed?"
        ),
    )


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


#: Drill id -> callable. The order is the order the protocol runs them in, one
#: per week of the trial, so a trader meets each failure mode once and cold.
DRILLS: dict[str, Callable[[], DrillResult]] = {
    "critical_source_outage": drill_critical_source_outage,
    "partial_key_coverage": drill_partial_key_coverage,
    "stale_payload": drill_stale_payload,
    "page_generation_failure": drill_page_generation_failure,
    "deployment_failure": drill_deployment_failure,
}


def run_drill(name: str) -> DrillResult:
    try:
        runner = DRILLS[name]
    except KeyError as exc:
        raise TrialError(f"unknown drill {name!r}; known: {sorted(DRILLS)}") from exc
    return runner()


def run_all_drills() -> tuple[DrillResult, ...]:
    """Run all five. One drill raising does not stop the others.

    A drill that cannot even run is reported as a failed drill with the
    exception as its observation, because "the rehearsal crashed" is itself a
    finding about the product's degradation path.
    """
    results: list[DrillResult] = []
    for name, runner in DRILLS.items():
        try:
            results.append(runner())
        except Exception as exc:  # noqa: BLE001 — a crashed drill is a drill result
            log.exception("drill %s raised", name)
            results.append(
                DrillResult(
                    drill=name,
                    title=name.replace("_", " "),
                    simulated="the drill itself could not be run",
                    expected="the drill runs and reports on the product's degradation",
                    observed=f"{type(exc).__name__}: {exc}",
                    passed=False,
                )
            )
    return tuple(results)
