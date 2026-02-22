let timer = null;
let loadingTimer = null;
let loadingStartedAt = 0;
let clockTimer = null;
let audioCtx = null;
let lastBacksideSignature = '';
let hasBacksideSignatureBaseline = false;
let lastRubberBandSignature = '';
let hasRubberBandSignatureBaseline = false;
let soundEnabled = false;
let defaultGates = {};
let tickerURLTemplate = '';
let activeTab = 'strongest';
let lastSnapshot = null;
let backsideHistory = [];
let backsideHistoryByAsOf = new Map();
let backsideHistoryDateNY = '';
let backsideHistoryKnownKeys = new Set();
let backsideHistoryNewKeys = new Set();
let hasBacksideHistoryBaseline = false;
let rbHistory = [];
let rbHistoryByAsOf = new Map();
let rbHistoryDateNY = '';
let rbHistoryKnownKeys = new Set();
let rbHistoryNewKeys = new Set();
let hasRBHistoryBaseline = false;
const nyClockFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
  timeZoneName: 'short',
});

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

const MAIN_TABLE_HEADERS = [
  '#',
  'Symbol',
  'Score',
  'Price',
  'RVOL',
  'Spread bps',
  '$/min',
  '10d $vol',
  'Range %',
  'ADR 10d %',
  'ADR x',
  'RS vs Bench',
  'Upd ms',
];

const HISTORY_TAB_HEADERS = [
  'Time',
  '#',
  'Symbol',
  'Score',
  'Price',
  'RVOL',
  'Spread bps',
  '$/min',
  'ADR x',
  'RS vs Bench',
];

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

function renderNowNYClock() {
  const parts = nyClockFormatter.formatToParts(new Date());
  const byType = {};
  for (const p of parts) {
    byType[p.type] = p.value;
  }
  const y = byType.year || '----';
  const m = byType.month || '--';
  const d = byType.day || '--';
  const hh = byType.hour || '--';
  const mm = byType.minute || '--';
  const ss = byType.second || '--';
  const tz = byType.timeZoneName || 'ET';
  qs('clock').textContent = `${y}-${m}-${d} ${hh}:${mm}:${ss} ${tz}`;
}

function startClockTicker() {
  renderNowNYClock();
  if (clockTimer) {
    clearInterval(clockTimer);
  }
  clockTimer = setInterval(renderNowNYClock, 1000);
}

function ensureAudioContext() {
  const Ctx = window.AudioContext || window.webkitAudioContext;
  if (!Ctx) return null;
  if (!audioCtx) {
    audioCtx = new Ctx();
  }
  if (audioCtx.state === 'suspended') {
    audioCtx.resume().catch(() => {});
  }
  return audioCtx;
}

function wireAudioUnlock() {
  const unlock = () => {
    ensureAudioContext();
    document.removeEventListener('pointerdown', unlock);
    document.removeEventListener('keydown', unlock);
  };
  document.addEventListener('pointerdown', unlock, { once: true });
  document.addEventListener('keydown', unlock, { once: true });
}

function playBacksideChangeAlert(force = false, startOffsetSec = 0) {
  if (!force && !soundEnabled) return;
  const ctx = ensureAudioContext();
  if (!ctx) return;

  const now = ctx.currentTime + Math.max(0, Number(startOffsetSec) || 0);
  const beepAt = (start, freq) => {
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'triangle';
    osc.frequency.setValueAtTime(freq, start);
    gain.gain.setValueAtTime(0.0001, start);
    gain.gain.exponentialRampToValueAtTime(0.11, start + 0.01);
    gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.12);
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(start);
    osc.stop(start + 0.13);
  };

  beepAt(now + 0.00, 2200);
  beepAt(now + 0.17, 2480);
}

function playRubberBandChangeAlert(force = false, startOffsetSec = 0) {
  if (!force && !soundEnabled) return;
  const ctx = ensureAudioContext();
  if (!ctx) return;

  const now = ctx.currentTime + Math.max(0, Number(startOffsetSec) || 0);
  const beepAt = (start, freq) => {
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.type = 'square';
    osc.frequency.setValueAtTime(freq, start);
    gain.gain.setValueAtTime(0.0001, start);
    gain.gain.exponentialRampToValueAtTime(0.13, start + 0.006);
    gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.09);
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(start);
    osc.stop(start + 0.095);
  };

  beepAt(now + 0.00, 3300);
  beepAt(now + 0.12, 3950);
  beepAt(now + 0.24, 4700);
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

function parseAsOfParts(asOfNY) {
  const asOf = String(asOfNY || '');
  const date = asOf.length >= 10 ? asOf.slice(0, 10) : '';
  const timeWithColon = asOf.length >= 16 ? asOf.slice(11, 16) : '';
  const timeHHMM = timeWithColon.replace(':', '');
  return { asOf, date, timeWithColon, timeHHMM };
}

function openChartBaseFromTemplate(template) {
  const trimmed = String(template || '').trim();
  if (!trimmed) {
    return '/api/open-chart';
  }
  const i = trimmed.indexOf('/api/open-chart');
  if (i < 0) {
    return '';
  }
  return `${trimmed.slice(0, i)}/api/open-chart`;
}

function formatHistoryTickerURL(symbol, asOfNY, signal = 'buy') {
  const tpl = String(tickerURLTemplate || '').trim();
  const parts = parseAsOfParts(asOfNY);
  const openChartBase = openChartBaseFromTemplate(tpl);
  if (openChartBase) {
    const q = new URLSearchParams();
    q.set('ticker', symbol);
    if (parts.date) q.set('date', parts.date);
    // For history links we include optional time=HHMM when available.
    if (parts.timeHHMM) q.set('time', parts.timeHHMM);
    q.set('signal', signal);
    return `${openChartBase}?${q.toString()}`;
  }
  if (!tpl) return '';
  return tpl
    .replaceAll('{symbol}', encodeURIComponent(symbol))
    .replaceAll('{date}', encodeURIComponent(parts.date))
    .replaceAll('{time}', encodeURIComponent(parts.timeHHMM))
    .replaceAll('{signal}', encodeURIComponent(signal))
    .replaceAll('{as_of}', encodeURIComponent(parts.asOf));
}

function applyGateValues(values) {
  for (const key of GATE_FIELDS) {
    const el = qs(key);
    if (!el) continue;
    const v = values[key];
    el.value = v === undefined || v === null ? '' : `${v}`;
  }
}

function listForTab(data) {
  if (!data) return [];
  if (activeTab === 'backside') {
    return data.backside_candidates || [];
  }
  if (activeTab === 'rubber-band') {
    return data.rubber_band_candidates || [];
  }
  if (activeTab === 'rvol') {
    return data.rvol_candidates || [];
  }
  if (activeTab === 'hp') {
    return data.hard_pass_candidates || [];
  }
  if (activeTab === 'weakest') {
    return data.weakest_candidates || [];
  }
  return data.strongest_candidates || data.candidates || [];
}

function appendCell(tr, cell) {
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

function renderTableHeader() {
  const row = document.querySelector('#tbl thead tr');
  if (!row) return;
  row.innerHTML = '';
  const headers = activeTab === 'backside-history' || activeTab === 'rb-history' ? HISTORY_TAB_HEADERS : MAIN_TABLE_HEADERS;
  for (const h of headers) {
    const th = document.createElement('th');
    th.textContent = h;
    row.appendChild(th);
  }
}

function renderDefaultRows(data) {
  const tbody = document.querySelector('#tbl tbody');
  tbody.innerHTML = '';
  for (const c of listForTab(data)) {
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
      appendCell(tr, cell);
    }
    tbody.appendChild(tr);
  }
}

function renderHistoryRows(items, newKeys, signal = 'buy') {
  const tbody = document.querySelector('#tbl tbody');
  tbody.innerHTML = '';

  for (const item of items) {
    for (const c of item.rows || []) {
      const tr = document.createElement('tr');
      const link = formatHistoryTickerURL(c.symbol, item.as_of_ny, signal);
      const key = `${item.as_of_ny || ''}|${c.symbol || ''}`;
      const isNew = newKeys.has(key);
      if (isNew) {
        tr.classList.add('row-new');
        setTimeout(() => tr.classList.remove('row-new'), 1500);
      }
      const cells = [
        item.time_label || '',
        c.rank,
        { value: c.symbol, href: link },
        num(c.score, 4),
        num(c.price, 2),
        num(c.rvol, 2),
        num(c.spread_bps, 2),
        Math.round(c.dollar_vol_per_min || 0).toLocaleString(),
        num(c.adr_expansion, 2),
        `${num((c.rel_strength_vs_spy || 0) * 100, 2)}%`,
      ];
      for (const cell of cells) {
        appendCell(tr, cell);
      }
      tbody.appendChild(tr);
    }
  }
}

function renderRows(data) {
  if (activeTab === 'backside-history') {
    renderHistoryRows(backsideHistory, backsideHistoryNewKeys, 'buy');
    return;
  }
  if (activeTab === 'rb-history') {
    renderHistoryRows(rbHistory, rbHistoryNewKeys, 'buy');
    return;
  }
  renderDefaultRows(data);
}

function syncModeDependentTabs() {
  const mode = qs('mode')?.value;
  const isLive = mode === 'live';
  qs('tab-backside-history')?.classList.toggle('hidden', !isLive);
  qs('tab-rb-history')?.classList.toggle('hidden', !isLive);
  if (!isLive && activeTab === 'backside-history') {
    activeTab = 'backside';
  }
  if (!isLive && activeTab === 'rb-history') {
    activeTab = 'rubber-band';
  }
}

function setTab(tab) {
  const mode = qs('mode')?.value || 'live';
  const isLive = mode === 'live';
  activeTab = tab === 'weakest' ||
      tab === 'rvol' ||
      tab === 'hp' ||
      tab === 'backside' ||
      tab === 'rubber-band' ||
      (isLive && (tab === 'backside-history' || tab === 'rb-history'))
    ? tab
    : 'strongest';
  qs('tab-strongest')?.classList.toggle('active', activeTab === 'strongest');
  qs('tab-weakest')?.classList.toggle('active', activeTab === 'weakest');
  qs('tab-rvol')?.classList.toggle('active', activeTab === 'rvol');
  qs('tab-hp')?.classList.toggle('active', activeTab === 'hp');
  qs('tab-backside')?.classList.toggle('active', activeTab === 'backside');
  qs('tab-rubber-band')?.classList.toggle('active', activeTab === 'rubber-band');
  qs('tab-backside-history')?.classList.toggle('active', activeTab === 'backside-history');
  qs('tab-rb-history')?.classList.toggle('active', activeTab === 'rb-history');
  renderTableHeader();
  renderRows(lastSnapshot);
}

function updateTabLabels(data) {
  const strongestCount = (data?.strongest_candidates || data?.candidates || []).length;
  const weakestCount = (data?.weakest_candidates || []).length;
  const rvolCount = (data?.rvol_candidates || []).length;
  const hpCount = (data?.hard_pass_candidates || []).length;
  const backsideCount = (data?.backside_candidates || []).length;
  const rubberBandCount = (data?.rubber_band_candidates || []).length;
  const backsideHistoryCount = (backsideHistory || []).reduce((sum, item) => sum + ((item.rows || []).length), 0);
  const rbHistoryCount = (rbHistory || []).reduce((sum, item) => sum + ((item.rows || []).length), 0);
  if (qs('tab-strongest')) qs('tab-strongest').textContent = `Strongest (${strongestCount})`;
  if (qs('tab-weakest')) qs('tab-weakest').textContent = `Weakest (${weakestCount})`;
  if (qs('tab-rvol')) qs('tab-rvol').textContent = `RVOL (${rvolCount})`;
  if (qs('tab-hp')) qs('tab-hp').textContent = `HP (${hpCount})`;
  if (qs('tab-backside')) qs('tab-backside').textContent = `Backside (${backsideCount})`;
  if (qs('tab-rubber-band')) qs('tab-rubber-band').textContent = `Rubber Band (${rubberBandCount})`;
  if (qs('tab-backside-history')) qs('tab-backside-history').textContent = `Backside Historical (${backsideHistoryCount})`;
  if (qs('tab-rb-history')) qs('tab-rb-history').textContent = `RB Historical (${rbHistoryCount})`;
}

function resetBacksideHistoryState() {
  backsideHistory = [];
  backsideHistoryByAsOf = new Map();
  backsideHistoryDateNY = '';
  backsideHistoryKnownKeys = new Set();
  backsideHistoryNewKeys = new Set();
  hasBacksideHistoryBaseline = false;
}

function resetRBHistoryState() {
  rbHistory = [];
  rbHistoryByAsOf = new Map();
  rbHistoryDateNY = '';
  rbHistoryKnownKeys = new Set();
  rbHistoryNewKeys = new Set();
  hasRBHistoryBaseline = false;
}

async function refreshBacksideHistory(mode) {
  if (mode !== 'live') {
    resetBacksideHistoryState();
    return 0;
  }
  const params = new URLSearchParams({ mode: 'live' });
  const date = qs('date')?.value;
  if (date) {
    params.set('date', date);
  }
  const res = await fetch(`/api/backside-history?${params.toString()}`, { cache: 'no-store' });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  const data = await res.json();
  const items = Array.isArray(data.items) ? data.items : [];
  const nextDateNY = String(data.date_ny || date || '').trim();
  if (nextDateNY && backsideHistoryDateNY && nextDateNY !== backsideHistoryDateNY) {
    resetBacksideHistoryState();
  }
  if (nextDateNY && !backsideHistoryDateNY) {
    backsideHistoryDateNY = nextDateNY;
  }

  const newlyAdded = new Set();
  for (const item of items) {
    const asOf = String(item.as_of_ny || '').trim();
    if (!asOf) {
      continue;
    }
    const existing = backsideHistoryByAsOf.get(asOf) || {
      time_label: item.time_label || '',
      as_of_ny: asOf,
      rows: [],
    };
    if (item.time_label) {
      existing.time_label = item.time_label;
    }

    const rowKeys = new Set((existing.rows || []).map((row) => `${asOf}|${row.symbol || ''}`));
    for (const c of item.rows || []) {
      const key = `${asOf}|${c.symbol || ''}`;
      if (!rowKeys.has(key)) {
        existing.rows.push(c);
        rowKeys.add(key);
        if (hasBacksideHistoryBaseline && !backsideHistoryKnownKeys.has(key)) {
          newlyAdded.add(key);
        }
      }
    }
    existing.rows.sort((a, b) => {
      const ar = Number(a?.rank ?? 0);
      const br = Number(b?.rank ?? 0);
      if (ar !== br) return ar - br;
      return String(a?.symbol || '').localeCompare(String(b?.symbol || ''));
    });
    backsideHistoryByAsOf.set(asOf, existing);
  }

  backsideHistory = Array.from(backsideHistoryByAsOf.values()).sort((a, b) => {
    const ak = String(a?.as_of_ny || '');
    const bk = String(b?.as_of_ny || '');
    if (ak === bk) return 0;
    return ak < bk ? 1 : -1;
  });

  const nextKnown = new Set();
  for (const item of backsideHistory) {
    for (const c of item.rows || []) {
      nextKnown.add(`${item.as_of_ny || ''}|${c.symbol || ''}`);
    }
  }
  backsideHistoryNewKeys = newlyAdded;
  backsideHistoryKnownKeys = nextKnown;
  hasBacksideHistoryBaseline = true;
  return newlyAdded.size;
}

async function refreshRBHistory(mode) {
  if (mode !== 'live') {
    resetRBHistoryState();
    return 0;
  }
  const params = new URLSearchParams({ mode: 'live' });
  const date = qs('date')?.value;
  if (date) {
    params.set('date', date);
  }
  const res = await fetch(`/api/rb-history?${params.toString()}`, { cache: 'no-store' });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  const data = await res.json();
  const items = Array.isArray(data.items) ? data.items : [];
  const nextDateNY = String(data.date_ny || date || '').trim();
  if (nextDateNY && rbHistoryDateNY && nextDateNY !== rbHistoryDateNY) {
    resetRBHistoryState();
  }
  if (nextDateNY && !rbHistoryDateNY) {
    rbHistoryDateNY = nextDateNY;
  }

  const newlyAdded = new Set();
  for (const item of items) {
    const asOf = String(item.as_of_ny || '').trim();
    if (!asOf) {
      continue;
    }
    const existing = rbHistoryByAsOf.get(asOf) || {
      time_label: item.time_label || '',
      as_of_ny: asOf,
      rows: [],
    };
    if (item.time_label) {
      existing.time_label = item.time_label;
    }

    const rowKeys = new Set((existing.rows || []).map((row) => `${asOf}|${row.symbol || ''}`));
    for (const c of item.rows || []) {
      const key = `${asOf}|${c.symbol || ''}`;
      if (!rowKeys.has(key)) {
        existing.rows.push(c);
        rowKeys.add(key);
        if (hasRBHistoryBaseline && !rbHistoryKnownKeys.has(key)) {
          newlyAdded.add(key);
        }
      }
    }
    existing.rows.sort((a, b) => {
      const ar = Number(a?.rank ?? 0);
      const br = Number(b?.rank ?? 0);
      if (ar !== br) return ar - br;
      return String(a?.symbol || '').localeCompare(String(b?.symbol || ''));
    });
    rbHistoryByAsOf.set(asOf, existing);
  }

  rbHistory = Array.from(rbHistoryByAsOf.values()).sort((a, b) => {
    const ak = String(a?.as_of_ny || '');
    const bk = String(b?.as_of_ny || '');
    if (ak === bk) return 0;
    return ak < bk ? 1 : -1;
  });

  const nextKnown = new Set();
  for (const item of rbHistory) {
    for (const c of item.rows || []) {
      nextKnown.add(`${item.as_of_ny || ''}|${c.symbol || ''}`);
    }
  }
  rbHistoryNewKeys = newlyAdded;
  rbHistoryKnownKeys = nextKnown;
  hasRBHistoryBaseline = true;
  return newlyAdded.size;
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
    if (date && time) {
      params.set('as_of', `${date}T${time}:00`);
    }
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
  let playedBacksideAlert = false;
  let playedRBAlert = false;
  const prevTab = activeTab;
  syncModeDependentTabs();
  if (prevTab !== activeTab) {
    setTab(activeTab);
  }
  setLoading(true, mode === 'historical' ? 'Replaying historical data' : 'Loading live data');
  try {
    const built = buildURL();
    const res = await fetch(built.url, { cache: 'no-store' });
    if (!res.ok) {
      throw new Error(await res.text());
    }
    const data = await res.json();
    const nextBacksideSignature = (data.backside_candidates || []).map((c) => `${c.symbol}:${c.rank}`).join('|');
    const nextRubberBandSignature = (data.rubber_band_candidates || []).map((c) => `${c.symbol}:${c.rank}`).join('|');
    if (mode === 'live') {
      const shouldPlayBacksideAlert = hasBacksideSignatureBaseline
        && !!nextBacksideSignature
        && nextBacksideSignature !== lastBacksideSignature;
      const shouldPlayRBAlert = hasRubberBandSignatureBaseline
        && !!nextRubberBandSignature
        && nextRubberBandSignature !== lastRubberBandSignature;
      if (soundEnabled) {
        if (shouldPlayBacksideAlert && shouldPlayRBAlert) {
          playBacksideChangeAlert();
          playRubberBandChangeAlert(true, 0.34);
          playedBacksideAlert = true;
          playedRBAlert = true;
        } else if (shouldPlayBacksideAlert) {
          playBacksideChangeAlert();
          playedBacksideAlert = true;
        } else if (shouldPlayRBAlert) {
          playRubberBandChangeAlert();
          playedRBAlert = true;
        }
      }
      lastBacksideSignature = nextBacksideSignature;
      hasBacksideSignatureBaseline = true;
      lastRubberBandSignature = nextRubberBandSignature;
      hasRubberBandSignatureBaseline = true;
    } else {
      lastBacksideSignature = '';
      hasBacksideSignatureBaseline = false;
      lastRubberBandSignature = '';
      hasRubberBandSignatureBaseline = false;
    }
    lastSnapshot = data;
    let backsideHistoryAdditions = 0;
    let rbHistoryAdditions = 0;
    try {
      backsideHistoryAdditions = await refreshBacksideHistory(mode);
      rbHistoryAdditions = await refreshRBHistory(mode);
      if (mode === 'live' && soundEnabled) {
        const shouldPlayBacksideHistoryAlert = backsideHistoryAdditions > 0 && !playedBacksideAlert;
        const shouldPlayRBHistoryAlert = rbHistoryAdditions > 0 && !playedRBAlert;
        if (shouldPlayBacksideHistoryAlert && shouldPlayRBHistoryAlert) {
          playBacksideChangeAlert(true);
          playRubberBandChangeAlert(true, 0.34);
        } else if (shouldPlayBacksideHistoryAlert) {
          playBacksideChangeAlert(true);
        } else if (shouldPlayRBHistoryAlert) {
          playRubberBandChangeAlert(true);
        }
      }
    } catch (histErr) {
      console.error(histErr);
      resetBacksideHistoryState();
      resetRBHistoryState();
    }

    const gateSuffix = built.gateChanges > 0 ? ` | Gate overrides: ${built.gateChanges}` : '';
    const dbg = data.gate_debug || {};
    const dbgText = ` | Gates fail: stale=${dbg.fail_stale || 0}, adv10d=${dbg.fail_min_avg_dollar_vol_10d || 0}, rvol=${dbg.fail_min_rvol || 0}, $/min=${dbg.fail_min_dollar_vol_per_min || 0}, adr=${dbg.fail_min_adr_expansion || 0}, spread=${dbg.fail_spread_cap || 0}`;
    const hardPassText = ` | Hard pass: ${dbg.passed_all_gates || 0}`;
    const sourceText = ` | 10d$vol src: csv=${dbg.avg_dollar_vol_from_csv || 0}, derived=${dbg.avg_dollar_vol_derived || 0}, const=${dbg.avg_dollar_vol_fallback_const || 0}`;
    const maxText = ` | MaxRVOL: ${num(dbg.max_rvol, 2)} (${dbg.max_rvol_symbol || '-'})`;
    const strongestTotal = data.count_strongest ?? ((data.strongest_candidates || data.candidates || []).length);
    const weakestTotal = data.count_weakest ?? ((data.weakest_candidates || []).length);
    const hardPassTotal = data.count_hard_pass ?? (dbg.passed_all_gates || 0);
    const backsideTotal = data.count_backside ?? ((data.backside_candidates || []).length);
    const rubberBandTotal = data.count_rubber_band ?? ((data.rubber_band_candidates || []).length);
    const splitText = ` | Directional pass: strongest=${strongestTotal}, weakest=${weakestTotal}, hard_pass=${hardPassTotal}, backside=${backsideTotal}, rb=${rubberBandTotal}`;
    const msgText = data.message ? ` | Note: ${data.message}` : '';
    qs('meta').textContent = `Mode: ${data.mode} | As-of NY: ${data.as_of_ny} | Generated: ${data.generated_at_ny} | Benchmark: ${data.benchmark} | Seen: ${data.count_seen} | Ranked: ${data.count_ranked}${splitText}${hardPassText}${gateSuffix}${dbgText}${sourceText}${maxText}${msgText}`;
    updateTabLabels(data);
    renderRows(data);
  } catch (err) {
    console.error(err);
    lastSnapshot = null;
    resetBacksideHistoryState();
    resetRBHistoryState();
    lastBacksideSignature = '';
    hasBacksideSignatureBaseline = false;
    lastRubberBandSignature = '';
    hasRubberBandSignatureBaseline = false;
    updateTabLabels(null);
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
  qs('sound')?.addEventListener('change', () => {
    soundEnabled = !!qs('sound')?.checked;
    if (soundEnabled) {
      ensureAudioContext();
    }
  });
  qs('sound-test')?.addEventListener('click', () => playBacksideChangeAlert(true));
  qs('rb-sound-test')?.addEventListener('click', () => playRubberBandChangeAlert(true));
  qs('mode').addEventListener('change', () => {
    syncModeDependentTabs();
    setTab(activeTab);
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
  qs('tab-strongest')?.addEventListener('click', () => setTab('strongest'));
  qs('tab-weakest')?.addEventListener('click', () => setTab('weakest'));
  qs('tab-rvol')?.addEventListener('click', () => setTab('rvol'));
  qs('tab-hp')?.addEventListener('click', () => setTab('hp'));
  qs('tab-backside')?.addEventListener('click', () => setTab('backside'));
  qs('tab-rubber-band')?.addEventListener('click', () => setTab('rubber-band'));
  qs('tab-backside-history')?.addEventListener('click', () => setTab('backside-history'));
  qs('tab-rb-history')?.addEventListener('click', () => setTab('rb-history'));
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
  wireAudioUnlock();
  soundEnabled = !!qs('sound')?.checked;
  syncModeDependentTabs();
  updateTabLabels(null);
  setTab('strongest');
  await bootstrapNowNY();
  startClockTicker();
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
