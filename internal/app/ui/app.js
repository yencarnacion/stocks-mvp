let timer = null;
let loadingTimer = null;
let loadingStartedAt = 0;
let defaultGates = {};
let tickerURLTemplate = '';

const GATE_FIELDS = [
  'top_k',
  'min_price',
  'max_price',
  'min_rvol',
  'min_dollar_vol_per_min',
  'min_adr_expansion',
  'min_avg_dollar_vol_10d',
  'max_staleness_ms',
  'spread_cap_bps_50',
  'spread_cap_bps_10',
  'spread_cap_bps_5',
];

const INT_FIELDS = new Set(['top_k', 'max_staleness_ms']);

function qs(id) {
  return document.getElementById(id);
}

function num(v, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(v)) {
    return '';
  }
  return Number(v).toFixed(digits);
}

async function bootstrapNowNY() {
  try {
    const res = await fetch('/api/time', { cache: 'no-store' });
    const data = await res.json();
    qs('clock').textContent = `${data.now_ny}`;
    qs('date').value = data.date;
    qs('time').value = data.time;
  } catch (err) {
    console.error(err);
  }
}

async function bootstrapGateDefaults() {
  try {
    const res = await fetch('/api/settings', { cache: 'no-store' });
    if (!res.ok) {
      throw new Error(await res.text());
    }
    const data = await res.json();
    defaultGates = data.scan || {};
    tickerURLTemplate = String(data.ticker_url_template || '').trim();
    applyGateValues(defaultGates);
  } catch (err) {
    console.error(err);
  }
}

function formatTickerURL(symbol, data) {
  if (!tickerURLTemplate) return '';
  const date = String(data?.as_of_ny || '').slice(0, 10);
  const time = String(data?.as_of_ny || '').slice(11, 16);
  return tickerURLTemplate
    .replaceAll('{symbol}', encodeURIComponent(symbol))
    .replaceAll('{date}', encodeURIComponent(date))
    .replaceAll('{time}', encodeURIComponent(time))
    .replaceAll('{as_of}', encodeURIComponent(String(data?.as_of_ny || '')));
}

function applyGateValues(values) {
  for (const key of GATE_FIELDS) {
    const el = qs(key);
    if (!el) continue;
    const v = values[key];
    el.value = v === undefined || v === null ? '' : `${v}`;
  }
}

function disableAutocomplete() {
  for (const el of document.querySelectorAll('input, select')) {
    el.setAttribute('autocomplete', 'off');
  }
}

function gateParams() {
  const params = new URLSearchParams();
  let changed = 0;

  for (const key of GATE_FIELDS) {
    const el = qs(key);
    if (!el) continue;

    const raw = String(el.value || '').trim();
    if (raw === '') continue;

    const parsed = INT_FIELDS.has(key) ? parseInt(raw, 10) : parseFloat(raw);
    if (!Number.isFinite(parsed)) continue;

    const baseRaw = defaultGates[key];
    if (baseRaw === undefined || baseRaw === null) {
      params.set(key, `${parsed}`);
      changed += 1;
      continue;
    }

    const base = Number(baseRaw);
    const equal = INT_FIELDS.has(key)
      ? parsed === base
      : Math.abs(parsed - base) < 1e-9;

    if (!equal) {
      params.set(key, `${parsed}`);
      changed += 1;
    }
  }

  return { params, changed };
}

function buildURL() {
  const mode = qs('mode').value;
  const date = qs('date').value;
  const time = qs('time').value;
  const params = new URLSearchParams({ mode });
  if (mode === 'historical') {
    if (date) params.set('date', date);
    if (time) params.set('time', time);
  }

  const gate = gateParams();
  for (const [k, v] of gate.params.entries()) {
    params.set(k, v);
  }

  return { url: `/api/top?${params.toString()}`, gateChanges: gate.changed };
}

async function refresh() {
  const mode = qs('mode').value;
  setLoading(true, mode === 'historical' ? 'Replaying historical data' : 'Loading live data');
  try {
    const built = buildURL();
    const res = await fetch(built.url, { cache: 'no-store' });
    if (!res.ok) {
      throw new Error(await res.text());
    }
    const data = await res.json();

    const gateSuffix = built.gateChanges > 0 ? ` | Gate overrides: ${built.gateChanges}` : '';
    const dbg = data.gate_debug || {};
    const dbgText = ` | Gates fail: stale=${dbg.fail_stale || 0}, adv10d=${dbg.fail_min_avg_dollar_vol_10d || 0}, rvol=${dbg.fail_min_rvol || 0}, $/min=${dbg.fail_min_dollar_vol_per_min || 0}, adr=${dbg.fail_min_adr_expansion || 0}, spread=${dbg.fail_spread_cap || 0}`;
    const sourceText = ` | 10d$vol src: csv=${dbg.avg_dollar_vol_from_csv || 0}, derived=${dbg.avg_dollar_vol_derived || 0}, const=${dbg.avg_dollar_vol_fallback_const || 0}`;
    const maxText = ` | MaxRVOL: ${num(dbg.max_rvol, 2)} (${dbg.max_rvol_symbol || '-'})`;
    const msgText = data.message ? ` | Note: ${data.message}` : '';
    qs('meta').textContent = `Mode: ${data.mode} | As-of NY: ${data.as_of_ny} | Generated: ${data.generated_at_ny} | Benchmark: ${data.benchmark} | Seen: ${data.count_seen} | Ranked: ${data.count_ranked}${gateSuffix}${dbgText}${sourceText}${maxText}${msgText}`;

    const tbody = document.querySelector('#tbl tbody');
    tbody.innerHTML = '';

    for (const c of data.candidates || []) {
      const tr = document.createElement('tr');
      const link = formatTickerURL(c.symbol, data);
      const cells = [
        c.rank,
        { value: c.symbol, href: link },
        num(c.score, 4),
        num(c.price, 2),
        num(c.rvol, 2),
        num(c.spread_bps, 2),
        Math.round(c.dollar_vol_per_min || 0).toLocaleString(),
        Math.round(c.avg_dollar_vol_10d || 0).toLocaleString(),
        num((c.range_pct || 0) * 100, 2),
        num((c.adr_pct_10d || 0) * 100, 2),
        num(c.adr_expansion, 2),
        `${num((c.rel_strength_vs_spy || 0) * 100, 2)}%`,
        c.updated_ms_ago,
      ];

      for (const cell of cells) {
        const td = document.createElement('td');
        if (cell && typeof cell === 'object' && cell.href) {
          const a = document.createElement('a');
          a.href = cell.href;
          a.target = '_blank';
          a.rel = 'noopener noreferrer';
          a.textContent = cell.value ?? '';
          td.appendChild(a);
        } else {
          td.textContent = cell ?? '';
        }
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  } catch (err) {
    console.error(err);
    qs('meta').textContent = `Error: ${err.message}`;
  } finally {
    setLoading(false);
  }
}

function schedule() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  const auto = qs('auto').checked;
  const mode = qs('mode').value;
  if (auto && mode === 'live') {
    timer = setInterval(refresh, 5000);
  }
}

function wire() {
  qs('load').addEventListener('click', refresh);
  qs('auto').addEventListener('change', schedule);
  qs('mode').addEventListener('change', () => {
    schedule();
    refresh();
  });
  qs('date').addEventListener('change', refresh);
  qs('time').addEventListener('change', refresh);

  for (const key of GATE_FIELDS) {
    const el = qs(key);
    if (!el) continue;
    el.addEventListener('change', refresh);
  }

  qs('reset-gates').addEventListener('click', () => {
    applyGateValues(defaultGates);
    refresh();
  });
}

function setLoading(isLoading, baseText = 'Loading data') {
  const box = qs('progress');
  const label = qs('progress-text');
  if (!box || !label) {
    return;
  }

  if (!isLoading) {
    box.classList.add('hidden');
    if (loadingTimer) {
      clearInterval(loadingTimer);
      loadingTimer = null;
    }
    return;
  }

  box.classList.remove('hidden');
  loadingStartedAt = Date.now();
  label.textContent = `${baseText}... 0.0s`;

  if (loadingTimer) {
    clearInterval(loadingTimer);
  }
  loadingTimer = setInterval(() => {
    const elapsed = ((Date.now() - loadingStartedAt) / 1000).toFixed(1);
    label.textContent = `${baseText}... ${elapsed}s`;
  }, 150);
}

(async function init() {
  disableAutocomplete();
  await bootstrapNowNY();
  await bootstrapGateDefaults();
  // Always start from config.yaml-backed defaults returned by /api/settings.
  applyGateValues(defaultGates);
  wire();
  await refresh();
  schedule();
})();

window.addEventListener('pageshow', (ev) => {
  if (!ev.persisted) return;
  applyGateValues(defaultGates);
  refresh();
});
