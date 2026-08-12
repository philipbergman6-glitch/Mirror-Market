"""PROTOTYPE — THROWAWAY. M11 (#160): ledger row drill-down.

Three variants of "press a ledger row, see where this leg was", mounted on the
REAL CBOT market page so they are judged at real density against real data.

    python scripts/generate_site.py --only cbot
    python scripts/prototype_m11_drilldown.py
    open docs/prototype/m11-drilldown.html          # ?variant=A|B|C

Not production code: no tests, no error handling, hand-rolled SVG instead of
the stack's Plotly, data inlined as JSON. Delete with the branch.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from app.block_builders import SiteContext, _home_unit
from app.markets import load_markets

PAGE = Path("docs/markets/cbot.html")
OUT = Path("docs/prototype/m11-drilldown.html")
MARKET = "cbot"


def leg_payload(leg, ctx) -> dict:
    """One leg's full print history, dual-quoted, plus its FX component."""
    source = leg.source
    owner = leg.market
    pair = owner.currency_pair
    points = []
    for day, value, quotes in ctx.leg_prints(leg):
        fx = ctx.fx_on(pair, day)
        usd = source.to_usd_mt(value, leg.key, fx)
        points.append(
            {
                "d": day.isoformat(),
                "usd": None if usd is None else round(usd, 2),
                "home": round(value, 4),
                "fx": None if fx is None else round(fx, 6),
                "n": quotes,
            }
        )
    return {
        "id": leg.leg_id,
        "label": leg.label,
        "market": owner.name,
        "href": owner.url,
        "unit": source.unit,
        "home_unit": _home_unit(source, leg.key, owner.home_currency or ""),
        "has_home": source.unit == "home_per_mt",
        "fx_pair": pair,
        "max_age": source.max_age_days,
        "expected_gap": leg.expected_gap_days,
        "points": points,
    }


def main() -> None:
    markets = load_markets()
    ctx = SiteContext.open()
    ledger = markets[MARKET].ledger
    payload = {
        "legs": {leg.leg_id: leg_payload(leg, ctx) for leg in ledger.legs},
        "order": [leg.leg_id for leg in ledger.legs],
        "reference": [
            {"d": day.isoformat(), "usd": round(usd, 2)}
            for day, usd in ctx.reference_series(markets)
        ],
        "today": ctx.today.isoformat(),
    }
    ctx.close()

    html = PAGE.read_text()
    # Tag each ledger row with its leg id. Matched by LABEL, not position: the
    # builder re-sorts counterparts by print date, so row order is not config
    # order and a positional tag would silently mislabel the whole drill-down.
    by_label = {leg["label"]: leg_id for leg_id, leg in payload["legs"].items()}

    def tag(match: re.Match) -> str:
        row = match.group(0)
        for label, leg_id in by_label.items():
            if f">{label}</a>" in row:
                return row.replace('<tr class="lrow', f'<tr data-leg="{leg_id}" class="lrow', 1)
        raise SystemExit(f"prototype: unlabelled ledger row\n{row[:300]}")

    html = re.sub(r'<tr class="lrow.*?</tr>', tag, html, flags=re.S)
    injected = ASSETS.replace("__DATA__", json.dumps(payload))
    html = html.replace("</body>", injected + "\n</body>")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html)
    print(f"wrote {OUT} ({len(html) // 1024} KB)")
    for leg_id, leg in payload["legs"].items():
        print(f"  {leg_id:22s} {len(leg['points']):5d} prints  {leg['home_unit'] or 'USD/MT'}")


ASSETS = """
<style>
/* ---- PROTOTYPE ONLY (M11 #160) ---- */
.lrow { cursor: pointer; }
.lrow:hover td { background: rgba(45,106,79,0.09); }
.lrow td:first-child::after { content: " ›"; color: var(--text-muted); }
.dd-row td { padding: 0 !important; background: #fbfaf7; }
.dd-pane { padding: 14px 16px 18px; border-left: 3px solid var(--rule-heavy); }
.dd-head { display: flex; justify-content: space-between; align-items: baseline;
           gap: 12px; flex-wrap: wrap; margin-bottom: 8px; }
.dd-title { font-weight: 600; font-size: 13px; }
.dd-sub { font-size: 11px; color: var(--text-muted); }
.dd-warn { font-size: 11px; color: #8a5a00; background: #fdf6e6;
           border-left: 2px solid #c98a00; padding: 6px 9px; margin: 8px 0; }
.dd-thin { font-size: 11px; color: var(--text-muted); font-style: italic; }
.dd-btns { display: flex; gap: 6px; }
.dd-btn { font: inherit; font-size: 11px; padding: 2px 8px; cursor: pointer;
          border: 1px solid var(--rule-heavy); background: #fff; border-radius: 2px; }
.dd-btn.on { background: var(--text); color: #fff; }
.dd-stats { display: flex; gap: 18px; font-size: 11px; color: var(--text-muted);
            margin-top: 6px; flex-wrap: wrap; }
.dd-stats b { color: var(--text); font-variant-numeric: tabular-nums; }
#dd-modal { position: fixed; inset: 0; background: rgba(20,20,18,0.45);
            display: none; align-items: center; justify-content: center; z-index: 900; }
#dd-modal.on { display: flex; }
.dd-card { background: #fff; width: min(760px, 92vw); max-height: 88vh; overflow: auto;
           padding: 20px 22px; box-shadow: 0 18px 50px rgba(0,0,0,0.28); }
.dd-x { float: right; cursor: pointer; border: 0; background: none; font-size: 20px; }
.dd-since { font-size: 11px; color: var(--text-muted); }
.dd-bar { display: inline-block; height: 5px; background: var(--rule-heavy); vertical-align: middle; }
.dd-dot { fill: var(--text); }
#pv-bar { position: fixed; left: 50%; transform: translateX(-50%); bottom: 18px;
          background: #141412; color: #fff; border-radius: 999px; padding: 8px 10px;
          display: flex; gap: 12px; align-items: center; z-index: 1000;
          box-shadow: 0 6px 22px rgba(0,0,0,0.35); font-size: 12px; }
#pv-bar button { background: #333; color: #fff; border: 0; width: 26px; height: 26px;
                 border-radius: 50%; cursor: pointer; font-size: 14px; }
#pv-bar .pv-label { font-weight: 600; letter-spacing: 0.02em; }
</style>
<div id="dd-modal"><div class="dd-card"><button class="dd-x">×</button><div id="dd-card-body"></div></div></div>
<div id="pv-bar"><button id="pv-prev">‹</button><span class="pv-label"></span><button id="pv-next">›</button></div>
<script>
const D = __DATA__;
const NAMES = {
  A: "A — inline expansion, one leg alone",
  B: "B — modal, leg vs CBOT + FX component",
  C: "C — no chart: range band + jump to the page"
};
const params = new URLSearchParams(location.search);
let VARIANT = (params.get('variant') || 'A').toUpperCase();
if (!NAMES[VARIANT]) VARIANT = 'A';
const MIN_POINTS = 8;              // the honesty threshold this prototype is testing

const fmt = (n, dp=2) => n == null ? "—" : n.toLocaleString('en-US',
  {minimumFractionDigits: dp, maximumFractionDigits: dp});
const clip = (pts, days) => {
  if (!days) return pts;
  const cut = new Date(D.today); cut.setDate(cut.getDate() - days);
  return pts.filter(p => new Date(p.d) >= cut);
};

function path(pts, pick, w, h, pad=6) {
  const vals = pts.map(pick).filter(v => v != null);
  if (!vals.length) return {d: "", dots: [], lo: null, hi: null};
  const lo = Math.min(...vals), hi = Math.max(...vals), span = (hi - lo) || 1;
  const t0 = new Date(pts[0].d).getTime();
  const t1 = new Date(pts[pts.length-1].d).getTime();
  const tspan = (t1 - t0) || 1;
  const xy = pts.filter(p => pick(p) != null).map(p => [
    pad + (w - 2*pad) * (new Date(p.d).getTime() - t0) / tspan,
    pad + (h - 2*pad) * (1 - (pick(p) - lo) / span)
  ]);
  return {d: xy.map(([x,y],i) => (i?'L':'M') + x.toFixed(1) + ' ' + y.toFixed(1)).join(' '),
          dots: xy, lo, hi};
}

function chart(pts, opts={}) {
  const w = opts.w || 660, h = opts.h || 150;
  const main = path(pts, p => p.usd, w, h);
  let svg = `<svg viewBox="0 0 ${w} ${h}" style="width:100%;height:${h}px">`;
  svg += `<rect x="0" y="0" width="${w}" height="${h}" fill="#fff" stroke="#e7e3da"/>`;
  if (opts.ref && opts.ref.length > 1) {
    const r = path(opts.ref, p => p.usd, w, h);
    svg += `<path d="${r.d}" fill="none" stroke="#b9b2a4" stroke-width="1" stroke-dasharray="3 3"/>`;
  }
  svg += `<path d="${main.d}" fill="none" stroke="#2d6a4f" stroke-width="1.8"/>`;
  // Every observation is a dot — a sparse series must LOOK sparse.
  main.dots.forEach(([x,y]) => svg += `<circle class="dd-dot" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="2.4"/>`);
  svg += `<text x="6" y="12" font-size="10" fill="#8c8577">${fmt(main.hi)}</text>`;
  svg += `<text x="6" y="${h-4}" font-size="10" fill="#8c8577">${fmt(main.lo)}</text>`;
  svg += `</svg>`;
  return svg;
}

function stats(leg, pts) {
  const usd = pts.map(p => p.usd).filter(v => v != null);
  if (!usd.length) return "";
  const first = usd[0], last = usd[usd.length-1];
  const chg = first ? ((last-first)/first*100) : null;
  return `<div class="dd-stats">
    <span>observations <b>${pts.length}</b></span>
    <span>history starts <b>${pts[0].d}</b></span>
    <span>range <b>${fmt(Math.min(...usd))} – ${fmt(Math.max(...usd))}</b></span>
    <span>over window <b>${chg==null?'—':(chg>0?'+':'')+fmt(chg,1)+'%'}</b></span>
  </div>`;
}

function thinNote(leg, pts) {
  if (pts.length >= MIN_POINTS) return "";
  return `<div class="dd-warn"><b>Not a history — ${pts.length} observation${pts.length===1?'':'s'}.</b>
    ${leg.label} is a snapshot-only source: the venue publishes today's number and no archive,
    so this series exists only from the day we started keeping it (${pts.length ? pts[0].d : '—'}).
    A line through ${pts.length} point${pts.length===1?'':'s'} would draw a trend that has not been observed.</div>`;
}

function paneHTML(leg, days) {
  const pts = clip(leg.points, days);
  const thin = pts.length < MIN_POINTS;
  return `<div class="dd-pane">
    <div class="dd-head">
      <div>
        <div class="dd-title">${leg.label} — USD/MT</div>
        <div class="dd-sub">${leg.has_home ? leg.home_unit + ' converted at each row\\'s own ' + leg.fx_pair + ' rate' : 'quoted in USD/MT at source'}
          · every dot is one observation</div>
      </div>
      <div class="dd-btns">
        ${[[30,'30d'],[90,'90d'],[0,'all']].map(([n,l]) =>
          `<button class="dd-btn ${n===days?'on':''}" data-days="${n}" data-leg="${leg.id}">${l}</button>`).join('')}
      </div>
    </div>
    ${thinNote(leg, pts)}
    ${thin && pts.length < 3 ? '' : chart(pts, {h: 130})}
    ${stats(leg, pts)}
  </div>`;
}

/* ---------- A: inline expansion under the row ---------- */
function openA(tr, leg, days=90) {
  document.querySelectorAll('.dd-row').forEach(r => r.remove());
  if (tr.dataset.open === '1' && days === 90) { tr.dataset.open = '0'; return; }
  tr.dataset.open = '1';
  const cols = tr.children.length;
  const row = document.createElement('tr');
  row.className = 'dd-row';
  row.innerHTML = `<td colspan="${cols}">${paneHTML(leg, days)}</td>`;
  tr.after(row);
}

/* ---------- B: modal, leg against CBOT, FX broken out ---------- */
function openB(tr, leg, days=90) {
  const pts = clip(leg.points, days);
  const ref = clip(D.reference, days);
  const fxpts = leg.has_home ? pts.map(p => ({d: p.d, usd: p.fx})) : [];
  document.getElementById('dd-card-body').innerHTML = `
    <div class="dd-title">${leg.label}</div>
    <div class="dd-sub">${leg.market} · vs CBOT board (dashed) · USD/MT</div>
    ${thinNote(leg, pts)}
    ${chart(pts, {ref, h: 190})}
    ${stats(leg, pts)}
    <div class="dd-btns" style="margin:12px 0">
      ${[[30,'30d'],[90,'90d'],[0,'all']].map(([n,l]) =>
        `<button class="dd-btn ${n===days?'on':''}" data-days="${n}" data-leg="${leg.id}">${l}</button>`).join('')}
    </div>
    ${leg.has_home ? `
      <div class="dd-title" style="margin-top:14px">Currency component — ${leg.fx_pair}</div>
      <div class="dd-sub">the USD line above moves when the venue moves OR when this does;
        separating them is the only way "+0.03% local, +0.44% USD" is legible</div>
      ${chart(fxpts, {h: 90})}
      <div class="dd-stats"><span>home print <b>${fmt(pts.length?pts[pts.length-1].home:null)} ${leg.home_unit}</b></span>
        <span>rate <b>${fmt(pts.length?pts[pts.length-1].fx:null, 5)}</b></span></div>`
    : `<div class="dd-thin" style="margin-top:14px">Quoted in USD/MT at source — there is no second
        currency here, and restating the same figure would not be a second view of it.</div>`}
    <div class="dd-sub" style="margin-top:16px">
      <a href="${leg.href}">Open the ${leg.market} page →</a></div>`;
  document.getElementById('dd-modal').classList.add('on');
}

/* ---------- C: no chart in the ledger — a range band + a jump ---------- */
function decorateC() {
  document.querySelectorAll('tr.lrow').forEach(tr => {
    const leg = D.legs[tr.dataset.leg];
    if (!leg || tr.querySelector('.dd-since')) return;
    const usd = leg.points.map(p => p.usd).filter(v => v != null);
    const cell = tr.children[2];
    if (!usd.length) return;
    const lo = Math.min(...usd), hi = Math.max(...usd), last = usd[usd.length-1];
    const pos = hi === lo ? 50 : (last - lo) / (hi - lo) * 100;
    cell.insertAdjacentHTML('beforeend', `
      <div class="dd-since" title="range of every observation we hold">
        <span class="dd-bar" style="width:52px;position:relative">
          <span style="position:absolute;left:${pos.toFixed(0)}%;top:-3px;width:2px;height:11px;background:#2d6a4f"></span>
        </span>
        ${usd.length >= MIN_POINTS
          ? `${fmt(lo,0)}–${fmt(hi,0)} since ${leg.points[0].d}`
          : `<span style="color:#8a5a00">${usd.length} obs since ${leg.points[0].d} — no range yet</span>`}
      </div>`);
  });
}
function openC(tr, leg) { location.href = leg.href + '#block-price'; }

/* ---------- wiring ---------- */
function bind() {
  document.querySelectorAll('tr.lrow').forEach(tr => {
    tr.onclick = (e) => {
      if (e.target.closest('a')) return;
      const leg = D.legs[tr.dataset.leg];
      if (!leg) return;
      if (VARIANT === 'A') openA(tr, leg);
      else if (VARIANT === 'B') openB(tr, leg);
      else openC(tr, leg);
    };
  });
  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('.dd-btn');
    if (!b) return;
    e.stopPropagation();
    const leg = D.legs[b.dataset.leg], days = +b.dataset.days;
    const tr = document.querySelector(`tr.lrow[data-leg="${leg.id}"]`);
    if (VARIANT === 'A') openA(tr, leg, days); else openB(tr, leg, days);
  });
  document.querySelector('.dd-x').onclick = () => document.getElementById('dd-modal').classList.remove('on');
  document.getElementById('dd-modal').onclick = (e) => {
    if (e.target.id === 'dd-modal') e.currentTarget.classList.remove('on');
  };
  if (VARIANT === 'C') decorateC();
}

function go(next) {
  const keys = Object.keys(NAMES);
  const i = (keys.indexOf(VARIANT) + next + keys.length) % keys.length;
  params.set('variant', keys[i]);
  location.search = params.toString();
}
document.querySelector('#pv-bar .pv-label').textContent = NAMES[VARIANT];
document.getElementById('pv-prev').onclick = () => go(-1);
document.getElementById('pv-next').onclick = () => go(1);
document.addEventListener('keydown', (e) => {
  if (/^(INPUT|TEXTAREA)$/.test(document.activeElement.tagName)) return;
  if (e.key === 'ArrowLeft') go(-1);
  if (e.key === 'ArrowRight') go(1);
});
bind();
</script>
"""


if __name__ == "__main__":
    main()
