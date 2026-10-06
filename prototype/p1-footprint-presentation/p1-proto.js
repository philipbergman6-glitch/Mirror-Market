/* PROTOTYPE — P1 #362 (map #356). Throwaway. Never merge.
 * Three variants of footprint weather + cyclone hazard flags, mounted on
 * copies of the live CBOT and Brazil market pages (snapshot 2026-10-06).
 * ?variant=A|B|C  ?scenario=today|francine   (← → keys cycle variants, S flips scenario)
 * Numbers: K2 #361 spike results, ECMWF IFS 00z 2026-10-05 run, ERA5 1991–2020
 * normal, SPAM-2020-soy-weighted admin-1. Francine = NHC al062024 advisory 010
 * replayed as if it were today. Only Iowa and Mato Grosso were computed in the
 * spike; other footprints render as hatched placeholders, never invented numbers.
 */
(function () {
  const qs = new URLSearchParams(location.search);
  const VARIANTS = { A: 'Cards in place', B: 'Scan table', C: 'Story first' };
  const keys = Object.keys(VARIANTS);
  const variant = keys.includes(qs.get('variant')) ? qs.get('variant') : 'A';
  const scenario = qs.get('scenario') === 'francine' ? 'francine' : 'today';
  const page = location.pathname.includes('brazil') ? 'brazil' : 'cbot';

  // ---- data (K2 results/*.json) -------------------------------------------
  const RUN = { stamp: 'forecast · ECMWF 00z 5 Oct', window: 'valid 5–19 Oct', normal: 'vs ERA5 1991–2020' };
  const FP = {
    'US-IA': { name: 'Iowa', role: 'domestic crop', tp15: 10.6, normal: 34.3, median: 28.4, terc: [17.7, 38.4], minmax: [0.7, 116.0],
      rank: 6, pct: -69, tmaxAnom: 5.1, tmaxSd: 2.7, heat34: 0, tmaxPeak: 26.8, soil0: 0.30, soil15: 0.23, dryArea5d: 57, dryArea10d: 13,
      pin: { tp15: 15.4, pct: -59, rank: 12, tercile: 'middle', obs: '0.0 mm · 20.5 °C max · 5 Oct' },
      daily: [0,0,0,0,1.3,0,0,0,0.1,6.8,2.4,0,0,0,0], tmax: [21.6,23.7,24.3,19.7,22.4,26.4,26.8,25.7,25.3,24.4,19.6,16.9,22.3,22.8,23.3] },
    'BR-MT': { name: 'Mato Grosso', role: 'origin crop — planting', tp15: 74.1, normal: 57.0, median: 49.3, terc: [43.4, 72.1], minmax: [5.6, 129.4],
      rank: 23, pct: 30, tmaxAnom: 0.4, tmaxSd: 1.6, heat34: 7, tmaxPeak: 36.0, soil0: 0.28, soil15: 0.40, dryArea5d: 0, dryArea10d: 0,
      pin: { tp15: 65.6, pct: 7, rank: 19, tercile: 'middle', obs: '19.1 mm · 25.6 °C max · 5 Oct' },
      daily: [10.8,3.6,0.8,0.2,0.6,2.9,2.5,1.6,4.7,9.1,5.5,10.2,9.8,5.2,6.8], tmax: [30.2,32.4,34.4,35.9,36.0,36.0,34.2,35.0,34.4,32.1,33.1,31.8,30.5,31.3,31.7] },
  };
  const PAGE_FP = { cbot: ['US-IA', 'Illinois', 'Nebraska'], brazil: ['BR-MT', 'Paraná', 'Rio Grande do Sul'] };
  const PORT_RAIN = { 'P-PNG': { tp15: 208, wet5: 11 }, 'P-NOLA': { tp15: 120, wet5: 5 } };
  const PORTS = {
    today: {
      'P-NOLA': { name: 'New Orleans (Gulf export)', state: 'clear', text: 'nearest storm Rachel (NHC) 2,828 km' },
      'P-PNG': { name: 'Paranaguá', state: 'not_covered', text: 'South Atlantic — no publishable cyclone source' },
    },
    francine: {
      'P-NOLA': { name: 'New Orleans (Gulf export)', state: 'flag', sev: 'warning', text: 'Francine (NHC adv 10): tropical-storm-force winds forecast from +24 h, closest 107 km at +27 h' },
      'P-PNG': { name: 'Paranaguá', state: 'not_covered', text: 'South Atlantic — no publishable cyclone source' },
    },
  };
  const PAGE_PORTS = { cbot: ['P-NOLA'], brazil: ['P-PNG', 'P-NOLA'] };
  const LEG = {
    today: {
      'cbot-board': ['clear', 'no storm within 120 h of New Orleans'],
      'us_gulf-cif': ['clear', 'no storm within 120 h of New Orleans'],
      'dalian-board': ['partial', 'New Orleans clear · Paranaguá, Mato Grosso not covered'],
      'brazil-paranagua': ['not_covered', 'South Atlantic — no cyclone source'],
      'brazil-cepea': ['not_covered', 'South Atlantic — no cyclone source'],
      'argentina-fob': ['not_covered', 'South Atlantic — no cyclone source'],
    },
    francine: {
      'cbot-board': ['flag', 'Francine: TS winds at New Orleans from +24 h'],
      'us_gulf-cif': ['flag', 'Francine: TS winds at New Orleans from +24 h'],
      'dalian-board': ['flag', 'Francine: TS winds at New Orleans from +24 h · Paranaguá not covered'],
      'brazil-paranagua': ['not_covered', 'South Atlantic — no cyclone source'],
      'brazil-cepea': ['not_covered', 'South Atlantic — no cyclone source'],
      'argentina-fob': ['not_covered', 'South Atlantic — no cyclone source'],
    },
  };
  const ATTR = 'Forecast: ECMWF Open Data (IFS), CC BY 4.0. Normal: contains modified Copernicus Climate Change Service information (ERA5, 1991–2020). Soy weights: SPAM 2020.';

  // forecast alert rules (K2 §7.4) applied to the two real footprints
  function fcAlerts(k) {
    const f = FP[k], out = [];
    if (f.tp15 / f.normal <= 0.6) out.push(`15-day rain ${f.tp15} mm, ${Math.round(100 * f.tp15 / f.normal)}% of normal (≤ 60%)`);
    if (f.dryArea5d >= 50) out.push(`dry days ahead on ${f.dryArea5d}% of soy area`);
    return out;
  }
  function withheld(k) {
    return k === 'BR-MT' ? ['pod-fill heat not assessed: October is planting in Mato Grosso (7 forecast days ≥ 34 °C)'] : [];
  }
  const tercile = f => f.tp15 < f.terc[0] ? 'lower' : f.tp15 > f.terc[1] ? 'upper' : 'middle';
  const signed = (v, d = 0, u = '') => (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v).toFixed(d) + u;

  // ---- shared bits ---------------------------------------------------------
  const css = `
  .p1-ph{border:1px dashed var(--rule);background:repeating-linear-gradient(45deg,#fff,#fff 6px,#F4F6F3 6px,#F4F6F3 12px);}
  .p1-track{position:relative;display:inline-block;width:110px;height:6px;background:#E4E8E4;border-radius:3px;vertical-align:middle;margin-right:6px}
  .p1-track .t{position:absolute;top:0;bottom:0;background:#D8DCD8}
  .p1-track .x{position:absolute;top:-3px;width:2px;height:12px;background:var(--ink)}
  .p1-stamp{font-family:var(--mono);font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--info)}
  .p1-fc{border-left-style:dashed}
  .p1-sub{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--text-dim);margin:16px 0 4px}
  .p1-chip{font-family:var(--display);font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;padding:2px 8px;border-radius:3px;white-space:nowrap;display:inline-block;margin-top:4px}
  .p1-chip.flag{background:#E8C983;color:#4A3608}.p1-chip.clear{background:transparent;border:1px solid var(--rule);color:var(--text-dim)}
  .p1-chip.not_covered,.p1-chip.partial{background:#E4E8E4;color:var(--text-muted)}
  .p1-line{font-size:11px;margin-top:3px}.p1-line.flag{color:var(--warning);font-weight:600}.p1-line.not_covered,.p1-line.partial{color:var(--text-muted)}.p1-line.clear{color:var(--text-dim)}
  .p1-badge{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;vertical-align:middle}
  .p1-badge.flag{background:var(--warning)}.p1-badge.not_covered,.p1-badge.partial{background:transparent;border:1.5px solid var(--text-dim)}
  .p1-banner{max-width:1180px;margin:0 auto;padding:8px 32px;font-size:13px;color:#4A3608;background:rgba(168,115,10,.10);border-left:3px solid var(--warning)}
  .kv{display:flex;justify-content:space-between;gap:12px;padding:6px 0;border-bottom:1px dotted var(--rule);font-size:13px}.kv>span:first-child{color:var(--text-muted)}
  #p1-bar{position:fixed;left:50%;bottom:18px;transform:translateX(-50%);z-index:9999;background:#111;color:#fff;border-radius:999px;padding:8px 14px;
    font:600 13px/1 -apple-system,sans-serif;display:flex;gap:10px;align-items:center;box-shadow:0 6px 24px rgba(0,0,0,.35)}
  #p1-bar button{background:#333;color:#fff;border:0;border-radius:999px;padding:6px 10px;font:inherit;cursor:pointer}
  #p1-bar .on{background:#E8C983;color:#111}
  #p1-notes{position:fixed;right:16px;bottom:70px;width:340px;max-height:60vh;overflow:auto;z-index:9999;background:#111;color:#eee;border-radius:10px;
    padding:12px 14px;font:12px/1.45 -apple-system,sans-serif;box-shadow:0 6px 24px rgba(0,0,0,.35)}
  #p1-notes b{color:#E8C983}`;
  document.head.insertAdjacentHTML('beforeend', `<style>${css}</style>`);

  function track(f) {
    const [lo, hi] = f.minmax, p = v => Math.max(0, Math.min(100, 100 * (v - lo) / (hi - lo)));
    return `<span class="p1-track" title="31-year range ${lo}–${hi} mm; middle band = middle tercile">
      <span class="t" style="left:${p(f.terc[0])}%;width:${p(f.terc[1]) - p(f.terc[0])}%"></span>
      <span class="x" style="left:${p(f.tp15)}%"></span></span>`;
  }
  function placeholder(name, cls = 'mc') {
    return `<div class="${cls} p1-ph"><div class="mc-label">${name} · soy area</div>
      <div class="caption">not computed in the K2 spike (Iowa and Mato Grosso only) — placeholder, no number invented</div></div>`;
  }
  function alertsFor(keys, label) {
    let h = '';
    keys.filter(k => FP[k]).forEach(k => fcAlerts(k).forEach(t =>
      h += `<div class="alert alert-warn p1-fc"><span class="kind">forecast</span> ${FP[k].name} soy area: ${t}</div>`));
    keys.filter(k => FP[k]).forEach(k => withheld(k).forEach(t => h += `<div class="caption">${FP[k].name}: ${t}</div>`));
    return h;
  }

  // ---- variant A: cards in place --------------------------------------------
  function weatherA() {
    const ks = PAGE_FP[page];
    let h = `<div class="p1-stamp">${RUN.stamp} · ${RUN.window} · ${RUN.normal} · soy-weighted state</div><div class="grid grid-3" style="margin-top:6px">`;
    ks.forEach(k => {
      const f = FP[k];
      if (!f) { h += placeholder(k); return; }
      h += `<div class="mc"><div class="mc-label">${f.name} · soy area</div>
        <div class="mc-val">${f.tp15}<small class="muted"> mm · 15-day rain</small></div>
        <div class="mc-delta">${track(f)}<span class="${f.pct < 0 ? 'down' : 'up'}">${signed(f.pct, 0, '%')}</span> · ${tercile(f)} tercile · rank ${f.rank} of 31</div>
        <div class="caption">Tmax ${signed(f.tmaxAnom, 1, ' °C')} vs normal · ${f.heat34} days ≥ 34 °C · soil ${f.soil0.toFixed(2)} → ${f.soil15.toFixed(2)} m³/m³</div>
        <div class="caption" style="margin-bottom:2px">${f.role} · pin observed: ${f.pin.obs}</div></div>`;
    });
    h += `</div>` + alertsFor(ks);
    h += `<div class="caption">${ATTR}</div>` + hazardsA();
    return h;
  }
  function hazardsA() {
    const ps = PAGE_PORTS[page].map(p => PORTS[scenario][p]);
    let h = `<div class="river"><div class="river-head">Storms — the port leg · NHC + JTWC, checked 15:00Z</div>`;
    PAGE_PORTS[page].forEach(id => {
      const p = PORTS[scenario][id], r = PORT_RAIN[id];
      h += `<div class="river-row"><div class="lbl">${p.name}${r ? `<div class="caption">port rain ${r.tp15} mm over 15 days · ${r.wet5} days ≥ 5 mm (loading delays)</div>` : ''}</div>
        <div class="val">${p.state === 'flag' ? `<span class="p1-chip flag">storm</span>` : p.state === 'clear' ? `<span class="muted">no storm ≤ 120 h</span>` : `<span class="muted">not covered</span>`}</div></div>`;
    });
    h += `</div>`;
    ps.filter(p => p.state === 'flag').forEach(p => h += `<div class="alert alert-warn">${p.name}: ${p.text}</div>`);
    if (!ps.some(p => p.state === 'flag')) h += `<div class="alert alert-ok">No active storm threatens ${ps.filter(p => p.state === 'clear').map(p => p.name).join(', ') || 'a covered port'}</div>`;
    ps.filter(p => p.state === 'not_covered').forEach(p => h += `<div class="caption">${p.name}: ${p.text} — absence of a flag is not a clear reading</div>`);
    return h;
  }

  // ---- variant B: scan table -------------------------------------------------
  function weatherB() {
    const ks = PAGE_FP[page];
    let h = `<div class="table-scroll"><table class="dtable"><thead><tr><th>Soy area</th><th class="num">15-day rain</th><th class="num">Normal</th>
      <th class="num">vs normal</th><th>Tercile · rank</th><th class="num">Tmax vs normal</th><th class="num">Days ≥34 °C</th><th class="num">Soil now → d15</th><th class="num">Pin says</th></tr></thead><tbody>`;
    ks.forEach(k => {
      const f = FP[k];
      if (!f) { h += `<tr class="p1-ph"><td>${k}</td><td colspan="8" class="muted">not computed in the K2 spike — placeholder</td></tr>`; return; }
      h += `<tr><td><strong>${f.name}</strong><div class="caption">${f.role}</div></td><td class="num strong">${f.tp15} mm</td><td class="num">${f.normal} mm</td>
        <td class="num"><span class="${f.pct < 0 ? 'down' : 'up'}">${signed(f.pct, 0, '%')}</span></td><td>${track(f)}${tercile(f)} · ${f.rank}/31</td>
        <td class="num">${signed(f.tmaxAnom, 1, ' °C')}</td><td class="num">${f.heat34}</td><td class="num">${f.soil0.toFixed(2)} → ${f.soil15.toFixed(2)}</td>
        <td class="num muted">${f.pin.tp15} mm · ${f.pin.tercile}</td></tr>`;
    });
    h += `</tbody></table></div><div class="caption">${RUN.stamp} · ${RUN.window} · ${RUN.normal}. "Pin says" = the old single point's forecast, shown for one release then dropped. ${ATTR}</div>`;
    h += alertsFor(ks);
    // hazard table: every place this page's legs price at
    h += `<div class="p1-sub">Hazards — places this page's legs price at</div><div class="table-scroll"><table class="dtable"><thead><tr><th>Place</th><th>Cyclone</th><th>Detail</th><th class="num">Port rain 15 d</th></tr></thead><tbody>`;
    PAGE_PORTS[page].forEach(id => {
      const p = PORTS[scenario][id], r = PORT_RAIN[id];
      h += `<tr><td>${p.name}</td><td><span class="p1-chip ${p.state}">${p.state.replace('_', ' ')}</span></td><td class="caption">${p.text}</td>
        <td class="num">${r ? `${r.tp15} mm · ${r.wet5} d ≥ 5 mm` : '—'}</td></tr>`;
    });
    h += `</tbody></table></div><div class="caption">NHC + JTWC (US government), 34/50/64-kt radii, 0–120 h. Checked 15:00Z.</div>`;
    return h;
  }

  // ---- variant C: story first ------------------------------------------------
  function bars(f) {
    const W = 560, H = 120, max = Math.max(12, ...f.daily) * 1.1, bw = W / 15, pace = f.normal / 15;
    let s = `<svg viewBox="0 0 ${W} ${H + 18}" style="width:100%;max-width:${W}px;display:block;margin:8px 0">`;
    f.daily.forEach((v, i) => { const h = H * Math.max(v, 0) / max;
      s += `<rect x="${i * bw + 3}" y="${H - h}" width="${bw - 6}" height="${h}" fill="var(--info)" opacity=".75"/>
        <text x="${i * bw + bw / 2}" y="${H + 13}" font-size="9" text-anchor="middle" fill="var(--text-dim)" font-family="var(--mono)">${i + 5 > 30 ? i - 25 : i + 5}</text>`; });
    const y = H - H * pace / max;
    s += `<line x1="0" x2="${W}" y1="${y}" y2="${y}" stroke="var(--text-dim)" stroke-dasharray="4 3"/>
      <text x="${W - 2}" y="${y - 3}" font-size="9" text-anchor="end" fill="var(--text-muted)">normal pace ${pace.toFixed(1)} mm/day</text></svg>`;
    return s;
  }
  function weatherC() {
    const ks = PAGE_FP[page], main = FP[ks[0]];
    let h = `<div style="font-family:var(--display);font-weight:700;font-size:30px;letter-spacing:-.02em" class="${main.pct < 0 ? 'down' : 'up'}">
        ${main.tp15} mm<small style="font-size:14px;color:var(--text-dim)"> next 15 days · ${main.name} soy area</small></div>
      <div class="caption" style="font-size:13px">${Math.round(100 * main.tp15 / main.normal)}% of the ${main.normal} mm normal — ${main.rank === 6 ? '6th driest' : `rank ${main.rank}`} of 31 years, ${tercile(main)} tercile.
        The single point we used to show says ${main.pin.tp15} mm, ${main.pin.tercile} tercile.</div>
      ${bars(main)}
      <div class="p1-stamp">${RUN.stamp} · ${RUN.window} · ${RUN.normal} · ECMWF CC BY 4.0</div>`;
    h += alertsFor(ks);
    h += `<div style="margin-top:10px">`;
    ks.slice(1).forEach(k => h += `<div class="kv p1-ph" style="padding:4px 6px"><span>${k} soy area</span><span class="muted">not computed in spike</span></div>`);
    h += `<div class="kv"><span>Tmax vs normal · days ≥ 34 °C</span><span class="num">${signed(main.tmaxAnom, 1, ' °C')} · ${main.heat34}</span></div>
      <div class="kv"><span>Soil moisture (top 7 cm) now → day 15</span><span class="num">${main.soil0.toFixed(2)} → ${main.soil15.toFixed(2)}</span></div>
      <div class="kv"><span>Pin observed (yesterday)</span><span class="num">${main.pin.obs}</span></div></div>`;
    const flagged = PAGE_PORTS[page].filter(id => PORTS[scenario][id].state === 'flag');
    h += flagged.length ? '' : `<div class="caption">No storm flag on this page's ports · NHC + JTWC checked 15:00Z${PAGE_PORTS[page].includes('P-PNG') ? ' · Paranaguá not covered (South Atlantic)' : ''}</div>`;
    h += `<div class="caption">${ATTR}</div>`;
    return h;
  }
  function bannerC() {
    const flagged = PAGE_PORTS[page].map(id => PORTS[scenario][id]).filter(p => p.state === 'flag');
    if (!flagged.length) return;
    const el = document.querySelector('.stale-note, .masthead, header');
    const html = flagged.map(p => `<div class="p1-banner"><strong>Storm · ${p.name}</strong> — ${p.text}. Legs: CBOT board, US Gulf CIF, Dalian No.2.</div>`).join('');
    (el ? el : document.body.firstElementChild).insertAdjacentHTML('afterend', html);
  }

  // ---- ledger decoration -----------------------------------------------------
  function decorateLedger() {
    document.querySelectorAll('#block-ledger tr.lrow').forEach(tr => {
      const id = (tr.dataset.panel || '').replace('drill-', ''), st = LEG[scenario][id];
      if (!st) return;
      const [s, why] = st, nameCell = tr.cells[0], stateCell = tr.cells[tr.cells.length - 1];
      if (variant === 'A') {
        if (s === 'clear') return;  // A: silence when clear
        stateCell.insertAdjacentHTML('beforeend', `<div><span class="p1-chip ${s}" title="${why}">${s === 'flag' ? 'storm' : s === 'partial' ? 'storm: part covered' : 'storm: not covered'}</span></div>`);
      } else if (variant === 'B') {
        nameCell.insertAdjacentHTML('beforeend', `<div class="p1-line ${s}">${s === 'flag' ? '⚠ ' : ''}hazard: ${why}</div>`);
      } else {
        if (s === 'clear') return;
        const a = nameCell.querySelector('a');
        a && a.insertAdjacentHTML('beforebegin', `<span class="p1-badge ${s}" title="${why}"></span>`);
      }
    });
  }

  // ---- mount -----------------------------------------------------------------
  const blk = document.getElementById('block-weather');
  if (blk) {
    const head = blk.querySelector('.block-head');
    head.querySelector('.why').textContent = variant === 'C' ? 'is the crop getting the rain it needs' : 'this market\'s soy areas, next 15 days';
    const river = blk.querySelector('.river');
    [...blk.children].forEach(c => { if (c !== head && c !== river) c.remove(); });
    head.insertAdjacentHTML('afterend', `<div id="p1-w">${{ A: weatherA, B: weatherB, C: weatherC }[variant]()}</div>`);
  }
  decorateLedger();
  if (variant === 'C') bannerC();

  // ---- switcher + notes ------------------------------------------------------
  const NOTES = {
    A: ['<b>Where</b>: block 06 only; pin cards replaced by footprint cards (pin observed kept as a caption line).',
        '<b>Reader sees</b>: 15-day rain big, % vs normal + tercile track + rank, Tmax anomaly, heat days, soil now→d15.',
        '<b>Hazard on a leg</b>: chip in the ledger State column, only when flagged / not covered (clear = silent).',
        '<b>Active-storm detail</b>: "Storms — the port leg" rows under the river, alert line when flagged.',
        '<b>No storm</b>: explicit green "No active storm threatens …".',
        '<b>Port rain</b>: renders, as a caption on the port row.',
        '<b>Attribution</b>: one caption under the cards.'],
    B: ['<b>Where</b>: block 06 becomes a dense table, one row per soy area.',
        '<b>Reader sees</b>: every number side by side incl. the normal and the old pin\'s forecast ("Pin says", one release only).',
        '<b>Hazard on a leg</b>: a caption line under every leg name, including "clear".',
        '<b>Active-storm detail</b>: a hazard table of every place the page\'s legs price at.',
        '<b>No storm</b>: every place row says clear / not covered — no summary line.',
        '<b>Port rain</b>: renders, as a table column.',
        '<b>Attribution</b>: table footnote.'],
    C: ['<b>Where</b>: block 06 leads with one headline footprint + 15-day daily rain chart; other areas as kv rows.',
        '<b>Reader sees</b>: the story sentence (mm, % of normal, rank, and what the old pin said).',
        '<b>Hazard on a leg</b>: a small dot before the leg name (hover for why); clear = no dot.',
        '<b>Active-storm detail</b>: page-wide banner under the masthead, only while flagged.',
        '<b>No storm</b>: one muted caption.',
        '<b>Port rain</b>: does NOT render.',
        '<b>Attribution</b>: in the stamp line.'],
  };
  const go = (v, s) => { const u = new URLSearchParams(location.search); u.set('variant', v); u.set('scenario', s); location.search = u.toString(); };
  const i = keys.indexOf(variant);
  document.body.insertAdjacentHTML('beforeend', `
    <div id="p1-notes" hidden><div style="margin-bottom:6px"><b>PROTOTYPE · variant ${variant} — ${VARIANTS[variant]}</b></div>${NOTES[variant].map(n => `<div>• ${n}</div>`).join('')}
      <div style="margin-top:8px;color:#aaa">Data: K2 spike, ECMWF 00z 5 Oct 2026. Hatched = not computed. Francine = 2024 advisory replayed.</div></div>
    <div id="p1-bar"><button id="p1-prev">◀</button><span>${variant} — ${VARIANTS[variant]}</span><button id="p1-next">▶</button>
      <button id="p1-scn" class="${scenario === 'francine' ? 'on' : ''}">${scenario === 'francine' ? 'Francine replay' : 'Today (no storm)'}</button>
      <button id="p1-page">${page === 'cbot' ? '→ Brazil page' : '→ CBOT page'}</button><button id="p1-n">notes</button></div>`);
  document.getElementById('p1-prev').onclick = () => go(keys[(i + keys.length - 1) % keys.length], scenario);
  document.getElementById('p1-next').onclick = () => go(keys[(i + 1) % keys.length], scenario);
  document.getElementById('p1-scn').onclick = () => go(variant, scenario === 'francine' ? 'today' : 'francine');
  document.getElementById('p1-page').onclick = () => { location.href = `${page === 'cbot' ? 'brazil' : 'cbot'}.html?variant=${variant}&scenario=${scenario}#block-weather`; };
  const notes = document.getElementById('p1-notes');
  document.getElementById('p1-n').onclick = () => { notes.hidden = !notes.hidden; };
  notes.hidden = false;
  document.addEventListener('keydown', e => {
    if (e.target.closest('input,textarea,[contenteditable]')) return;
    if (e.key === 'ArrowRight') go(keys[(i + 1) % keys.length], scenario);
    if (e.key === 'ArrowLeft') go(keys[(i + keys.length - 1) % keys.length], scenario);
    if (e.key === 's' || e.key === 'S') go(variant, scenario === 'francine' ? 'today' : 'francine');
  });
})();
