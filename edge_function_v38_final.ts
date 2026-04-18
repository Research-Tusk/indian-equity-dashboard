import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': '*',
  'Content-Type': 'application/json',
  'Connection': 'keep-alive'
};
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const NI_HEADERS: Record<string, string> = {
  'Connection': 'keep-alive',
  'Accept': 'application/json, text/javascript, */*; q=0.01',
  'X-Requested-With': 'XMLHttpRequest',
  'User-Agent': UA,
  'Content-Type': 'application/json; charset=UTF-8',
  'Origin': 'https://niftyindices.com',
  'Referer': 'https://niftyindices.com/reports/historical-data',
};

const INDICES: Record<string, {
  nseIndexName: string; label: string; source: 'api' | 'csv' | 'us';
  yahooTicker?: string; fwdEpsEnvVar?: string; staticPB?: number;
}> = {
  'nifty50': { nseIndexName: 'NIFTY 50', label: 'Nifty 50', source: 'api', yahooTicker: '%5ENSEI' },
  'nifty-capital-markets': { nseIndexName: 'Nifty Capital Markets', label: 'Nifty Capital Markets', source: 'csv' },
  'sp500': { nseIndexName: '', label: 'S&P 500', source: 'us', yahooTicker: '^GSPC', fwdEpsEnvVar: 'SP500_FWD_EPS_ANCHOR', staticPB: 4.5 },
  'nasdaq': { nseIndexName: '', label: 'NASDAQ', source: 'us', yahooTicker: '^NDX', fwdEpsEnvVar: 'NASDAQ_FWD_EPS_ANCHOR', staticPB: 8.0 },
};

// Fallback credentials (overridden by env vars when set)
const FRED_KEY_FALLBACK = 'd6d6deeb62090decbe4f9f2f684b539b';
const FWD_EPS_DEFAULTS: Record<string, number> = { sp500: 285, nasdaq: 1050 };

const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MONTH_MAP: Record<string,string> = {Jan:'01',Feb:'02',Mar:'03',Apr:'04',May:'05',Jun:'06',Jul:'07',Aug:'08',Sep:'09',Oct:'10',Nov:'11',Dec:'12'};

// 2016-04-01 is the project-wide baseline start date.
const BASELINE_START_ISO = '2016-04-01';

function getSupabase() {
  return createClient(
    Deno.env.get('SUPABASE_URL')!,
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
  );
}

// Retry helper: up to 3 attempts with exponential backoff (500ms, 1000ms, 2000ms)
async function fetchWithRetry(url: string, init?: RequestInit, attempts = 3): Promise<Response> {
  let lastErr: any;
  for (let i = 0; i < attempts; i++) {
    try {
      const resp = await fetch(url, init);
      if (resp.ok || resp.status === 404) return resp;
      lastErr = new Error(`HTTP ${resp.status}`);
    } catch (e) {
      lastErr = e;
    }
    if (i < attempts - 1) {
      await new Promise(r => setTimeout(r, 500 * Math.pow(2, i)));
    }
  }
  throw lastErr;
}

async function logAudit(sb: any, index: string, status: string, latestDate: string | null, message: string) {
  try {
    await sb.from('refresh_audit').insert({
      index_id: index,
      status,
      latest_date: latestDate,
      message,
    });
  } catch (_e) { /* best-effort */ }
}

function parseCsvLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '\"') { inQuotes = !inQuotes; }
    else if (ch === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
    else { current += ch; }
  }
  result.push(current.trim());
  return result;
}

function fmtNseDate(d: Date): string {
  return `${String(d.getDate()).padStart(2,'0')}-${MONTH_NAMES[d.getMonth()]}-${d.getFullYear()}`;
}

function parseNseDate(dateStr: string): string {
  const parts = dateStr.trim().split(' ');
  const d = parts[0].padStart(2,'0');
  const m = MONTH_MAP[parts[1]] || '01';
  const y = parts[2];
  return `${y}-${m}-${d}`;
}

function toISODate(ts: number): string {
  const d = new Date(ts * 1000);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

async function fetchPEPBCloseFromCSV(indexName: string, fromDate: string, toDate: string): Promise<{date: string; pe: number; pb: number; close: number}[]> {
  const results: {date: string; pe: number; pb: number; close: number}[] = [];
  const start = new Date(fromDate); const end = new Date(toDate);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    if (d.getDay() === 0 || d.getDay() === 6) continue;
    const dd = String(d.getDate()).padStart(2,'0');
    const mm = String(d.getMonth()+1).padStart(2,'0');
    const yyyy = d.getFullYear();
    const isoDate = `${yyyy}-${mm}-${dd}`;
    try {
      const url = `https://www.niftyindices.com/Daily_Snapshot/ind_close_all_${dd}${mm}${yyyy}.csv`;
      const resp = await fetchWithRetry(url, { headers: { 'User-Agent': UA } });
      if (!resp.ok) continue;
      const text = await resp.text();
      if (text.startsWith('<!DOCTYPE') || text.includes('<html')) continue;
      const lines = text.split('\n').filter(l => l.trim());
      if (lines.length < 2) continue;
      const hdr = parseCsvLine(lines[0]);
      const peI = hdr.findIndex(h => h.toLowerCase().includes('p/e'));
      const pbI = hdr.findIndex(h => h.toLowerCase().includes('p/b'));
      const clI = hdr.findIndex(h => h.toLowerCase().includes('closing index value'));
      if (peI === -1 || pbI === -1) continue;
      for (let i = 1; i < lines.length; i++) {
        const f = parseCsvLine(lines[i]);
        if (f[0] && f[0].trim().toLowerCase() === indexName.toLowerCase()) {
          const pe = parseFloat(f[peI]), pb = parseFloat(f[pbI]);
          const close = clI !== -1 ? parseFloat(f[clI]) : 0;
          if (!isNaN(pe) && pe > 0 && !isNaN(pb) && pb > 0)
            results.push({ date: isoDate, pe, pb, close: isNaN(close) ? 0 : close });
          break;
        }
      }
    } catch (_e) {}
  }
  return results;
}

async function fetchCloseFromCSV(indexName: string, fromDate: string, toDate: string): Promise<Map<string, number>> {
  const closeMap = new Map<string, number>();
  const start = new Date(fromDate); const end = new Date(toDate);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    if (d.getDay() === 0 || d.getDay() === 6) continue;
    const dd = String(d.getDate()).padStart(2,'0');
    const mm = String(d.getMonth()+1).padStart(2,'0');
    const yyyy = d.getFullYear();
    const isoDate = `${yyyy}-${mm}-${dd}`;
    try {
      const url = `https://www.niftyindices.com/Daily_Snapshot/ind_close_all_${dd}${mm}${yyyy}.csv`;
      const resp = await fetchWithRetry(url, { headers: { 'User-Agent': UA } });
      if (!resp.ok) continue;
      const text = await resp.text();
      if (text.startsWith('<!DOCTYPE') || text.includes('<html')) continue;
      const lines = text.split('\n').filter(l => l.trim());
      if (lines.length < 2) continue;
      const hdr = parseCsvLine(lines[0]);
      const clI = hdr.findIndex(h => h.toLowerCase().includes('closing index value'));
      if (clI === -1) continue;
      for (let i = 1; i < lines.length; i++) {
        const f = parseCsvLine(lines[i]);
        if (f[0] && f[0].trim().toLowerCase() === indexName.toLowerCase()) {
          const close = parseFloat(f[clI]);
          if (!isNaN(close) && close > 0) closeMap.set(isoDate, close);
          break;
        }
      }
    } catch (_e) {}
  }
  return closeMap;
}

async function fetchPEPB(indexName: string, fromDate: string, toDate: string) {
  const cinfo = JSON.stringify({ name: indexName, startDate: fromDate, endDate: toDate, indexName });
  const r = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', {
    method: 'POST', headers: NI_HEADERS, body: JSON.stringify({ cinfo }),
  });
  const j = await r.json();
  const raw = JSON.parse(j.d);
  return raw.map((r: any) => ({ date: parseNseDate(r.DATE), pe: parseFloat(r.pe), pb: parseFloat(r.pb) }));
}

async function fetchHistoricalClose(indexName: string, fromDate: string, toDate: string): Promise<Map<string, number>> {
  const closeMap = new Map<string, number>();
  const cinfo = JSON.stringify({ name: indexName, startDate: fromDate, endDate: toDate, indexName });
  try {
    const r = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getHistoricaldataDBtoString', {
      method: 'POST', headers: NI_HEADERS, body: JSON.stringify({ cinfo }),
    });
    if (!r.ok) return closeMap;
    const j = await r.json();
    const raw = JSON.parse(j.d);
    for (const row of raw) {
      const date = parseNseDate(row.CH_TIMESTAMP || row.HistoricalDate || row.DATE || '');
      const close = parseFloat(row.CH_CLOSING_PRICE || row.CLOSE || row.Close || row.close || '0');
      if (date && !isNaN(close) && close > 0) closeMap.set(date, close);
    }
  } catch (_e) {}
  return closeMap;
}

async function fetchYahooClose(ticker: string, fromISO: string, toISO: string): Promise<Map<string, number>> {
  const closeMap = new Map<string, number>();
  try {
    const p1 = Math.floor(new Date(fromISO).getTime() / 1000);
    const p2 = Math.floor(new Date(toISO).getTime() / 1000) + 86400;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?period1=${p1}&period2=${p2}&interval=1d`;
    const r = await fetchWithRetry(url, { headers: { 'User-Agent': UA } });
    if (!r.ok) return closeMap;
    const j = await r.json();
    const result = j?.chart?.result?.[0];
    if (!result) return closeMap;
    const timestamps = result.timestamp || [];
    const closes = result.indicators?.quote?.[0]?.close || [];
    for (let i = 0; i < timestamps.length; i++) {
      if (closes[i] != null && !isNaN(closes[i])) {
        const date = toISODate(timestamps[i]);
        closeMap.set(date, +closes[i].toFixed(2));
      }
    }
  } catch (_e) {}
  return closeMap;
}

async function fetchBY(fromDate: string, toDate: string) {
  const allData: any[] = [];
  const startYear = parseInt(fromDate.split('-')[0]);
  const endYear = parseInt(toDate.split('-')[0]);
  for (let y = startYear; y <= endYear; y++) {
    const from = y === startYear ? fromDate : `${y}-01-01`;
    const to = y === endYear ? toDate : `${y}-12-31`;
    try {
      const r = await fetchWithRetry(`https://api.investing.com/api/financialdata/historical/24014?start-date=${from}&end-date=${to}&time-frame=Daily&add-missing-rows=false`, {
        headers: { 'User-Agent': UA, 'domain-id': 'in' },
      });
      const j = await r.json();
      if (j.data) {
        for (const d of j.data) {
          allData.push({ date: d.rowDateTimestamp?.split('T')[0], by: parseFloat(String(d.last_close).replace(/,/g, '')) });
        }
      }
    } catch(e) { console.error(`BY fetch error year ${y}:`, e); }
  }
  allData.sort((a: any, b: any) => a.date.localeCompare(b.date));
  return allData;
}

// =========================================================================
// US MARKET UTILITIES
// =========================================================================

/**
 * Fetch a FRED time series (e.g. DGS10, WILL5000PR, GDP).
 * Returns Map<date, value> for all non-missing observations.
 */
async function fetchFRED(seriesId: string, apiKey: string, startDate: string): Promise<Map<string, number>> {
  const result = new Map<string, number>();
  try {
    const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${seriesId}&api_key=${apiKey}&file_type=json&observation_start=${startDate}`;
    const r = await fetchWithRetry(url, { headers: { 'User-Agent': UA } });
    if (!r.ok) { console.error(`FRED ${seriesId}: HTTP ${r.status}`); return result; }
    const j = await r.json();
    for (const obs of (j.observations || [])) {
      if (obs.value && obs.value !== '.') {
        result.set(obs.date, parseFloat(obs.value));
      }
    }
  } catch (e) { console.error(`FRED fetch error ${seriesId}:`, e); }
  return result;
}

/**
 * Forward-fill a sparse map over a sorted array of trading dates.
 * Returns a dense Map with values carried forward from the last known observation.
 */
function forwardFill(dates: string[], sparseMap: Map<string, number>): Map<string, number> {
  const filled = new Map<string, number>();
  let lastVal: number | undefined;
  for (const d of dates) {
    const v = sparseMap.get(d);
    if (v !== undefined) lastVal = v;
    if (lastVal !== undefined) filled.set(d, lastVal);
  }
  return filled;
}

/**
 * Compute RSI-14 from an array of closing prices.
 * Returns array of same length; first 14 entries are null.
 */
function computeRSI14(closes: number[]): (number | null)[] {
  const period = 14;
  const rsi: (number | null)[] = new Array(closes.length).fill(null);
  if (closes.length < period + 1) return rsi;
  let avgGain = 0, avgLoss = 0;
  for (let i = 1; i <= period; i++) {
    const ch = closes[i] - closes[i - 1];
    if (ch > 0) avgGain += ch; else avgLoss -= ch;
  }
  avgGain /= period; avgLoss /= period;
  rsi[period] = avgLoss === 0 ? 100 : +(100 - 100 / (1 + avgGain / avgLoss)).toFixed(2);
  for (let i = period + 1; i < closes.length; i++) {
    const ch = closes[i] - closes[i - 1];
    avgGain = (avgGain * (period - 1) + (ch > 0 ? ch : 0)) / period;
    avgLoss = (avgLoss * (period - 1) + (ch < 0 ? -ch : 0)) / period;
    rsi[i] = avgLoss === 0 ? 100 : +(100 - 100 / (1 + avgGain / avgLoss)).toFixed(2);
  }
  return rsi;
}

/**
 * Refresh US index data: fetches Yahoo daily closes + FRED DGS10 + WILL5000PR + GDP,
 * computes forward PE, MCap/GDP, and upserts into daily_eyby_data.
 */
async function refreshUSIndex(
  sb: any, idx: string,
  indexConfig: { yahooTicker?: string; fwdEpsEnvVar?: string; staticPB?: number; label: string }
): Promise<{ merged: number; fetchedCloses: number; fetchedBY: number; fetchedWilshire: number; fetchedGDP: number; latestDate: string | null }> {
  // Determine start date (incremental from last known row)
  const { data: latest } = await sb.from('daily_eyby_data')
    .select('date').eq('index_id', idx).order('date', { ascending: false }).limit(1);
  const lastDate = latest?.[0]?.date || BASELINE_START_ISO;
  const fromDate = new Date(lastDate);
  fromDate.setDate(fromDate.getDate() - 7);
  const today = new Date();
  today.setDate(today.getDate() + 1);
  const fromStr = fromDate.toISOString().split('T')[0];
  const toStr = today.toISOString().split('T')[0];

  // 1. Yahoo daily closes
  const ticker = indexConfig.yahooTicker || '';
  const closeSeries = await fetchYahooDailySeries(ticker, fromStr, toStr);
  if (closeSeries.length === 0) {
    return { merged: 0, fetchedCloses: 0, fetchedBY: 0, fetchedWilshire: 0, fetchedGDP: 0, latestDate: lastDate };
  }
  const tradingDates = closeSeries.map(c => c.date);

  // 2. FRED bond yields + GDP, and Yahoo Wilshire 5000 — all in parallel
  //    (FRED discontinued all Wilshire series in June 2024, so we use Yahoo ^W5000)
  const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
  const [rawBY, rawGDP, wilshireSeries] = await Promise.all([
    fetchFRED('DGS10', fredKey, fromStr),
    fetchFRED('GDP', fredKey, BASELINE_START_ISO), // always fetch full GDP for quarterly ffill
    fetchYahooDailySeries('^W5000', fromStr, toStr),
  ]);
  const rawWilshire = new Map<string, number>();
  for (const w of wilshireSeries) rawWilshire.set(w.date, w.close);
  const byFilled = forwardFill(tradingDates, rawBY);
  const wilshireFilled = forwardFill(tradingDates, rawWilshire);
  const gdpFilled = forwardFill(tradingDates, rawGDP);

  // DB fallback for bond yield if FRED returned nothing
  if (byFilled.size === 0) {
    const { data: latestByRow } = await sb.from('daily_eyby_data')
      .select('by_yield').eq('index_id', idx).not('by_yield', 'is', null)
      .order('date', { ascending: false }).limit(1);
    if (latestByRow?.[0]?.by_yield) {
      const fb = latestByRow[0].by_yield;
      for (const d of tradingDates) byFilled.set(d, fb);
    }
  }

  // 3. Forward PE from anchor EPS env var (with hardcoded fallback)
  const fwdEpsAnchor = parseFloat(Deno.env.get(indexConfig.fwdEpsEnvVar || '') || '0')
    || FWD_EPS_DEFAULTS[idx] || 0;
  const staticPB = indexConfig.staticPB || 5.0;

  // 4. Build rows with MCap/GDP
  const rows: any[] = [];
  for (const { date, close } of closeSeries) {
    const by = byFilled.get(date);
    if (by === undefined) continue;
    const pe = fwdEpsAnchor > 0 ? +(close / fwdEpsAnchor).toFixed(2) : null;
    // MCap/GDP: Wilshire5000 (Yahoo ^W5000) / GDP (FRED) * 100
    const wil = wilshireFilled.get(date);
    const gdp = gdpFilled.get(date);
    const mcapGdp = (wil && gdp && gdp > 0) ? +((wil / gdp) * 100).toFixed(2) : null;
    rows.push({
      index_id: idx, date, pe, pb: staticPB,
      by_yield: +by.toFixed(4), close_price: close,
      mcap_gdp: mcapGdp,
    });
  }

  // 5. Upsert in chunks
  let merged = 0;
  for (let i = 0; i < rows.length; i += 500) {
    const chunk = rows.slice(i, i + 500);
    const { error } = await sb.from('daily_eyby_data')
      .upsert(chunk, { onConflict: 'index_id,date' });
    if (error) throw error;
    merged += chunk.length;
  }

  const latestInserted = rows.length > 0 ? rows[rows.length - 1].date : lastDate;
  return { merged, fetchedCloses: closeSeries.length, fetchedBY: rawBY.size,
    fetchedWilshire: rawWilshire.size, fetchedGDP: rawGDP.size, latestDate: latestInserted };
}

// =========================================================================
// STOCKS ENGINE
// =========================================================================

/**
 * Fetch full daily close series for a ticker via Yahoo v8 chart API.
 * Returns entries sorted by date ascending.
 */
async function fetchYahooDailySeries(symbol: string, fromISO: string, toISO: string): Promise<{date: string; close: number}[]> {
  const out: {date: string; close: number}[] = [];
  try {
    const p1 = Math.floor(new Date(fromISO).getTime() / 1000);
    const p2 = Math.floor(new Date(toISO).getTime() / 1000) + 86400;
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?period1=${p1}&period2=${p2}&interval=1d&includeAdjustedClose=true`;
    const r = await fetchWithRetry(url, { headers: { 'User-Agent': UA } });
    if (!r.ok) return out;
    const j = await r.json();
    const result = j?.chart?.result?.[0];
    if (!result) return out;
    const timestamps: number[] = result.timestamp || [];
    const closes: (number | null)[] = result.indicators?.quote?.[0]?.close || [];
    for (let i = 0; i < timestamps.length; i++) {
      const c = closes[i];
      if (c != null && !isNaN(c) && c > 0) {
        out.push({ date: toISODate(timestamps[i]), close: +c.toFixed(4) });
      }
    }
  } catch (_e) {}
  return out;
}

/**
 * Fetch Yahoo fundamentalsTimeSeries for quarterly diluted EPS and book-value-per-share.
 * Returns two arrays sorted by asOfDate ascending.
 */
async function fetchYahooQuarterlyFundamentals(symbol: string): Promise<{
  epsQ: {date: string; value: number}[];
  bvps: {date: string; value: number}[];
}> {
  const epsQ: {date: string; value: number}[] = [];
  const bvps: {date: string; value: number}[] = [];
  try {
    // period1=0 fetches full available history on Yahoo's side.
    const end = Math.floor(Date.now() / 1000) + 86400;
    const url = `https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/${encodeURIComponent(symbol)}?symbol=${encodeURIComponent(symbol)}&type=quarterlyDilutedEPS,quarterlyBookValuePerShare&period1=0&period2=${end}&corsDomain=finance.yahoo.com`;
    const r = await fetchWithRetry(url, { headers: { 'User-Agent': UA } });
    if (!r.ok) return { epsQ, bvps };
    const j = await r.json();
    const results = j?.timeseries?.result || [];
    for (const series of results) {
      const typeKey = series?.meta?.type?.[0];
      if (!typeKey) continue;
      const rows = series[typeKey];
      if (!Array.isArray(rows)) continue;
      for (const row of rows) {
        if (!row) continue;
        const asOf = row.asOfDate;
        const raw = row.reportedValue?.raw;
        if (typeof raw === 'number' && !isNaN(raw) && typeof asOf === 'string') {
          if (typeKey === 'quarterlyDilutedEPS') epsQ.push({ date: asOf, value: raw });
          else if (typeKey === 'quarterlyBookValuePerShare') bvps.push({ date: asOf, value: raw });
        }
      }
    }
  } catch (_e) {}
  epsQ.sort((a, b) => a.date.localeCompare(b.date));
  bvps.sort((a, b) => a.date.localeCompare(b.date));
  return { epsQ, bvps };
}

/**
 * Build a (date -> TTM EPS) lookup table from quarterly diluted EPS:
 * TTM at quarter end i = sum of last 4 quarters ending at i (inclusive).
 * Returns an array of {effectiveFrom, ttmEps} sorted ascending; the effectiveFrom is the quarter-end date,
 * meaning that TTM EPS is applicable from that date onwards until the next quarter end.
 */
function buildTtmEpsTimeline(epsQ: {date: string; value: number}[]): {effectiveFrom: string; ttmEps: number}[] {
  const timeline: {effectiveFrom: string; ttmEps: number}[] = [];
  for (let i = 3; i < epsQ.length; i++) {
    const ttm = epsQ[i].value + epsQ[i-1].value + epsQ[i-2].value + epsQ[i-3].value;
    timeline.push({ effectiveFrom: epsQ[i].date, ttmEps: +ttm.toFixed(4) });
  }
  return timeline;
}

/**
 * Binary-search a sorted timeline for the entry whose effectiveFrom <= date.
 */
function pickByDate<T extends {effectiveFrom?: string; date?: string; value?: number; ttmEps?: number}>(
  timeline: T[], date: string, key: 'effectiveFrom' | 'date'
): T | null {
  if (!timeline.length) return null;
  let lo = 0, hi = timeline.length - 1, best = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const k = (timeline[mid] as any)[key];
    if (k <= date) { best = mid; lo = mid + 1; }
    else hi = mid - 1;
  }
  return best >= 0 ? timeline[best] : null;
}

/**
 * Per-stock refresh: full history from either BASELINE_START_ISO or the listing date (whichever is later).
 * Computes daily close, per-quarter TTM EPS, and per-quarter BVPS, then derives P/E and P/B per trading day.
 */
async function refreshStock(
  sb: any,
  meta: { ticker: string; yahoo_symbol: string; listing_date: string | null }
): Promise<{ ticker: string; fetchedPrices: number; epsQuarters: number; bvpsQuarters: number; merged: number; firstDate: string | null; lastDate: string | null; note?: string; }> {
  const effectiveStart = (meta.listing_date && meta.listing_date > BASELINE_START_ISO) ? meta.listing_date : BASELINE_START_ISO;
  const todayIso = new Date().toISOString().split('T')[0];

  // Incremental update: start from one week before the latest existing row, if any.
  const { data: latest } = await sb.from('stocks_daily')
    .select('date')
    .eq('ticker', meta.ticker)
    .order('date', { ascending: false })
    .limit(1);
  const lastKnown: string | null = latest?.[0]?.date || null;
  let fromIso = effectiveStart;
  if (lastKnown) {
    const d = new Date(lastKnown);
    d.setDate(d.getDate() - 7);
    const rolled = d.toISOString().split('T')[0];
    if (rolled > effectiveStart) fromIso = rolled;
  }

  const prices = await fetchYahooDailySeries(meta.yahoo_symbol, fromIso, todayIso);
  if (prices.length === 0) {
    return { ticker: meta.ticker, fetchedPrices: 0, epsQuarters: 0, bvpsQuarters: 0, merged: 0, firstDate: null, lastDate: null, note: 'no-price-data' };
  }

  // Fundamentals only need to be refreshed on full passes; fetching them is lightweight anyway.
  const { epsQ, bvps } = await fetchYahooQuarterlyFundamentals(meta.yahoo_symbol);
  const ttmTimeline = buildTtmEpsTimeline(epsQ);

  const rows: any[] = [];
  for (const p of prices) {
    const row: any = {
      ticker: meta.ticker,
      date: p.date,
      close: p.close,
    };
    const ttmMatch = pickByDate(ttmTimeline as any[], p.date, 'effectiveFrom') as any;
    if (ttmMatch && typeof ttmMatch.ttmEps === 'number' && ttmMatch.ttmEps > 0) {
      row.eps_ttm = +ttmMatch.ttmEps.toFixed(4);
      row.pe = +(p.close / ttmMatch.ttmEps).toFixed(4);
      row.earnings_yield = +((ttmMatch.ttmEps / p.close) * 100).toFixed(4);
    }
    const bvpsMatch = pickByDate(bvps as any[], p.date, 'date') as any;
    if (bvpsMatch && typeof bvpsMatch.value === 'number' && bvpsMatch.value > 0) {
      row.bvps = +bvpsMatch.value.toFixed(4);
      row.pb = +(p.close / bvpsMatch.value).toFixed(4);
    }
    rows.push(row);
  }

  // Upsert in chunks to stay within Supabase payload limits.
  let merged = 0;
  for (let i = 0; i < rows.length; i += 500) {
    const chunk = rows.slice(i, i + 500);
    const { error } = await sb.from('stocks_daily').upsert(chunk, { onConflict: 'ticker,date' });
    if (error) throw error;
    merged += chunk.length;
  }

  return {
    ticker: meta.ticker,
    fetchedPrices: prices.length,
    epsQuarters: epsQ.length,
    bvpsQuarters: bvps.length,
    merged,
    firstDate: prices[0].date,
    lastDate: prices[prices.length - 1].date,
  };
}

// =========================================================================
// HTTP HANDLER
// =========================================================================

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });
  const url = new URL(req.url);
  const mode = url.searchParams.get('mode') || 'pe-fetch';

  try {
    if (mode === 'get-data') {
      const idx = url.searchParams.get('index') || 'nifty50';
      const idxConfig = INDICES[idx];
      const sb = getSupabase();
      const allRows: any[] = [];
      let from = 0;
      const pageSize = 1000;
      const selectCols = idxConfig?.source === 'us'
        ? 'date, pe, pb, by_yield, close_price, mcap_gdp'
        : 'date, pe, pb, by_yield, close_price';
      while (true) {
        const { data, error } = await sb.from('daily_eyby_data')
          .select(selectCols)
          .eq('index_id', idx)
          .order('date', { ascending: true })
          .range(from, from + pageSize - 1);
        if (error) throw error;
        if (!data || data.length === 0) break;
        allRows.push(...data);
        if (data.length < pageSize) break;
        from += pageSize;
      }

      // For US indices, return enriched objects with computed RSI-14, fwd_ey, erp, eyby
      if (idxConfig?.source === 'us' && allRows.length > 0) {
        const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
        const fwdEps = parseFloat(Deno.env.get(idxConfig.fwdEpsEnvVar || '') || '0')
          || FWD_EPS_DEFAULTS[idx] || 0;
        const closes = allRows.map((r: any) => r.close_price || 0);
        const rsiArr = computeRSI14(closes);
        const enriched = allRows.map((r: any, i: number) => {
          const close = r.close_price || 0;
          const gsec10 = r.by_yield || 0;
          const pe = r.pe || 0;
          const fwd_ey = fwdEps > 0 && close > 0 ? +((fwdEps / close) * 100).toFixed(4) : 0;
          const earningsYield = pe > 0 ? +(100 / pe).toFixed(4) : 0;
          const eyby = gsec10 > 0 ? +(earningsYield / gsec10).toFixed(4) : 0;
          return {
            date: r.date, close, pe, pb: r.pb || 0,
            gsec10, fwd_ey,
            erp: +(fwd_ey - gsec10).toFixed(4),
            eyby,
            rsi14: rsiArr[i],
            mcap_gdp: r.mcap_gdp || null,
            fii_fut_net_long: 50, // neutral placeholder — CFTC COT TBD
            breadth: 50,          // neutral placeholder
          };
        });
        return new Response(JSON.stringify(enriched), {
          headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' }
        });
      }

      // Indian indices: compact array format (unchanged)
      const compact = allRows.map((r: any) => [r.date, r.pe, r.pb, r.by_yield, r.close_price]);
      return new Response(JSON.stringify(compact), {
        headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' }
      });
    }

    if (mode === 'health') {
      const sb = getSupabase();
      const out: any = { ts: new Date().toISOString(), indices: {}, stocks: {} };
      for (const idx of Object.keys(INDICES)) {
        const { data } = await sb.from('daily_eyby_data')
          .select('date, pe, pb, by_yield, close_price')
          .eq('index_id', idx)
          .order('date', { ascending: false })
          .limit(1);
        const row = data?.[0];
        out.indices[idx] = {
          label: INDICES[idx].label,
          latestDate: row?.date || null,
          hasPe: row?.pe != null,
          hasPb: row?.pb != null,
          hasByYield: row?.by_yield != null,
          hasClose: row?.close_price != null,
        };
      }
      // Stocks latest-row summary
      try {
        const { data: stockMeta } = await sb.from('stocks_master').select('ticker').eq('is_active', true);
        for (const s of (stockMeta || [])) {
          const { data } = await sb.from('stocks_daily')
            .select('date, close, pe, pb')
            .eq('ticker', s.ticker)
            .order('date', { ascending: false })
            .limit(1);
          const r = data?.[0];
          out.stocks[s.ticker] = {
            latestDate: r?.date || null,
            hasClose: r?.close != null,
            hasPe: r?.pe != null,
            hasPb: r?.pb != null,
          };
        }
      } catch (_e) { /* ignore */ }
      // Recent audit entries
      try {
        const { data: audit } = await sb.from('refresh_audit')
          .select('index_id, status, latest_date, message, created_at')
          .order('created_at', { ascending: false })
          .limit(15);
        out.recentAudit = audit || [];
      } catch (_e) { out.recentAudit = []; }
      return new Response(JSON.stringify(out), { headers: CORS });
    }

    // -------- STOCKS ENDPOINTS --------

    if (mode === 'stocks-list') {
      const sb = getSupabase();
      const { data, error } = await sb.from('stocks_master')
        .select('ticker, name, yahoo_symbol, exchange, listing_date, sector, is_active')
        .eq('is_active', true)
        .order('name', { ascending: true });
      if (error) throw error;
      return new Response(JSON.stringify(data || []), {
        headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' }
      });
    }

    if (mode === 'stocks-data') {
      const ticker = url.searchParams.get('ticker');
      if (!ticker) return new Response(JSON.stringify({ error: 'ticker required' }), { status: 400, headers: CORS });
      const sb = getSupabase();
      const all: any[] = [];
      let from = 0;
      const pageSize = 1000;
      while (true) {
        const { data, error } = await sb.from('stocks_daily')
          .select('date, close, pe, pb, eps_ttm, bvps, earnings_yield')
          .eq('ticker', ticker)
          .order('date', { ascending: true })
          .range(from, from + pageSize - 1);
        if (error) throw error;
        if (!data || data.length === 0) break;
        all.push(...data);
        if (data.length < pageSize) break;
        from += pageSize;
      }
      const compact = all.map((r: any) => [r.date, r.close, r.pe, r.pb, r.eps_ttm, r.bvps, r.earnings_yield]);
      return new Response(JSON.stringify({ ticker, rows: compact }), {
        headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' }
      });
    }

    if (mode === 'stocks-update') {
      const ticker = url.searchParams.get('ticker');
      if (!ticker) return new Response(JSON.stringify({ error: 'ticker required' }), { status: 400, headers: CORS });
      const sb = getSupabase();
      const { data, error } = await sb.from('stocks_master')
        .select('ticker, yahoo_symbol, listing_date, is_active')
        .eq('ticker', ticker).limit(1);
      if (error) throw error;
      const meta = data?.[0];
      if (!meta) return new Response(JSON.stringify({ error: 'unknown ticker' }), { status: 404, headers: CORS });
      try {
        const result = await refreshStock(sb, meta);
        await logAudit(sb, `stock:${ticker}`, result.note ? 'warn' : 'ok', result.lastDate,
          `fetched=${result.fetchedPrices} eps=${result.epsQuarters} bvps=${result.bvpsQuarters} merged=${result.merged}${result.note ? ' note=' + result.note : ''}`);
        return new Response(JSON.stringify(result), { headers: CORS });
      } catch (e) {
        await logAudit(sb, `stock:${ticker}`, 'error', null, String(e));
        throw e;
      }
    }

    if (mode === 'stocks-update-all') {
      const sb = getSupabase();
      const { data: list, error } = await sb.from('stocks_master')
        .select('ticker, yahoo_symbol, listing_date, is_active')
        .eq('is_active', true)
        .order('ticker', { ascending: true });
      if (error) throw error;
      const results: any[] = [];
      for (const meta of (list || [])) {
        try {
          const r = await refreshStock(sb, meta as any);
          results.push(r);
          await logAudit(sb, `stock:${meta.ticker}`, r.note ? 'warn' : 'ok', r.lastDate,
            `fetched=${r.fetchedPrices} eps=${r.epsQuarters} bvps=${r.bvpsQuarters} merged=${r.merged}${r.note ? ' note=' + r.note : ''}`);
        } catch (e) {
          results.push({ ticker: meta.ticker, error: String(e) });
          await logAudit(sb, `stock:${meta.ticker}`, 'error', null, String(e));
        }
      }
      return new Response(JSON.stringify(results), { headers: CORS });
    }

    // -------- EXISTING INDEX ENDPOINTS --------

    if (mode === 'backfill-close') {
      const idx = url.searchParams.get('index') || 'nifty50';
      const indexConfig = INDICES[idx];
      if (!indexConfig) return new Response(JSON.stringify({error: `Unknown index: ${idx}`}), { headers: CORS });
      const sb = getSupabase();
      const missingRows: any[] = [];
      let offset = 0;
      while (true) {
        const { data, error } = await sb.from('daily_eyby_data')
          .select('date')
          .eq('index_id', idx)
          .is('close_price', null)
          .order('date', { ascending: true })
          .range(offset, offset + 999);
        if (error) throw error;
        if (!data || data.length === 0) break;
        missingRows.push(...data);
        if (data.length < 1000) break;
        offset += 1000;
      }
      if (missingRows.length === 0) {
        return new Response(JSON.stringify({ success: true, message: 'No missing close prices', index: idx }), { headers: CORS });
      }
      const firstMissing = missingRows[0].date;
      const lastMissing = missingRows[missingRows.length - 1].date;
      const totalMissing = missingRows.length;
      const allCloses = new Map<string, number>();
      const startYear = parseInt(firstMissing.split('-')[0]);
      const endYear = parseInt(lastMissing.split('-')[0]);
      for (let y = startYear; y <= endYear; y++) {
        const yFrom = y === startYear ? firstMissing : `${y}-01-01`;
        const yTo = y === endYear ? lastMissing : `${y}-12-31`;
        const nseFrom = fmtNseDate(new Date(yFrom));
        const nseTo = fmtNseDate(new Date(yTo));
        const yearCloses = await fetchHistoricalClose(indexConfig.nseIndexName, nseFrom, nseTo);
        for (const [date, close] of yearCloses) allCloses.set(date, close);
      }
      if (allCloses.size === 0 && indexConfig.yahooTicker) {
        const yahooCloses = await fetchYahooClose(indexConfig.yahooTicker, firstMissing, lastMissing);
        for (const [date, close] of yahooCloses) allCloses.set(date, close);
      }
      if (allCloses.size === 0 && indexConfig.source === 'csv') {
        const csvData = await fetchPEPBCloseFromCSV(indexConfig.nseIndexName, firstMissing, lastMissing);
        for (const d of csvData) { if (d.close > 0) allCloses.set(d.date, d.close); }
      }
      let updated = 0;
      const batch: any[] = [];
      for (const row of missingRows) {
        const close = allCloses.get(row.date);
        if (close !== undefined && close > 0) {
          batch.push({ index_id: idx, date: row.date, close_price: close });
        }
      }
      for (let i = 0; i < batch.length; i += 500) {
        const chunk = batch.slice(i, i + 500);
        const { error } = await sb.from('daily_eyby_data')
          .upsert(chunk, { onConflict: 'index_id,date', ignoreDuplicates: false });
        if (error) throw error;
        updated += chunk.length;
      }
      return new Response(JSON.stringify({
        success: true, index: idx, totalMissing,
        dateRange: `${firstMissing} to ${lastMissing}`,
        fetchedFromAPI: allCloses.size, updatedRows: updated,
      }), { headers: CORS });
    }

    if (mode === 'update-data') {
      const idx = url.searchParams.get('index') || 'nifty50';
      const indexConfig = INDICES[idx];
      if (!indexConfig) return new Response(JSON.stringify({error: `Unknown index: ${idx}`}), { headers: CORS });
      const sb = getSupabase();

      // --- US index path ---
      if (indexConfig.source === 'us') {
        try {
          const result = await refreshUSIndex(sb, idx, indexConfig);
          const { count } = await sb.from('daily_eyby_data').select('*', { count: 'exact', head: true }).eq('index_id', idx);
          await logAudit(sb, idx, result.merged > 0 ? 'ok' : 'no-new-data', result.latestDate,
            `closes=${result.fetchedCloses} by=${result.fetchedBY} wil=${result.fetchedWilshire} gdp=${result.fetchedGDP} merged=${result.merged}`);
          return new Response(JSON.stringify({
            success: true, index: idx, label: indexConfig.label, source: 'us',
            newLatest: result.latestDate, totalRows: count,
            fetchedCloses: result.fetchedCloses, fetchedBY: result.fetchedBY,
            fetchedWilshire: result.fetchedWilshire, fetchedGDP: result.fetchedGDP,
            merged: result.merged,
          }), { headers: CORS });
        } catch (e) {
          await logAudit(sb, idx, 'error', null, String(e));
          throw e;
        }
      }

      // --- Indian index path ---
      try {
        const { data: latest } = await sb.from('daily_eyby_data').select('date').eq('index_id', idx).order('date', { ascending: false }).limit(1);
        const lastDate = latest?.[0]?.date || (indexConfig.source === 'csv' ? '2024-10-01' : '2016-04-01');
        const fromDate = new Date(lastDate); fromDate.setDate(fromDate.getDate() - 3);
        const today = new Date(); today.setDate(today.getDate() + 1);
        const fromStr = fromDate.toISOString().split('T')[0];
        const toStr = today.toISOString().split('T')[0];
        let pepbData: {date: string; pe: number; pb: number}[];
        let closeMap = new Map<string, number>();
        if (indexConfig.source === 'csv') {
          const csvData = await fetchPEPBCloseFromCSV(indexConfig.nseIndexName, fromStr, toStr);
          pepbData = csvData.map(d => ({ date: d.date, pe: d.pe, pb: d.pb }));
          for (const d of csvData) { if (d.close > 0) closeMap.set(d.date, d.close); }
        } else {
          const nseFrom = fmtNseDate(fromDate); const nseTo = fmtNseDate(today);
          pepbData = await fetchPEPB(indexConfig.nseIndexName, nseFrom, nseTo);
          closeMap = await fetchCloseFromCSV(indexConfig.nseIndexName.toLowerCase() === 'nifty 50' ? 'Nifty 50' : indexConfig.nseIndexName, fromStr, toStr);
        }
        const byData = await fetchBY(fromStr, toStr);
        const byMap = new Map(byData.map((r: any) => [r.date, r.by]));

        const byDates = Array.from(byMap.keys()).sort();
        function getByForDate(date: string): number | undefined {
          const exact = byMap.get(date);
          if (exact !== undefined) return exact;
          let lo = 0, hi = byDates.length - 1, best = -1;
          while (lo <= hi) {
            const mid = (lo + hi) >> 1;
            if (byDates[mid] <= date) { best = mid; lo = mid + 1; }
            else hi = mid - 1;
          }
          return best >= 0 ? byMap.get(byDates[best]) : undefined;
        }

        let dbFallbackBy: number | undefined;
        if (byData.length === 0) {
          const { data: latestByRow } = await sb.from('daily_eyby_data')
            .select('by_yield').eq('index_id', idx).not('by_yield', 'is', null)
            .order('date', { ascending: false }).limit(1);
          if (latestByRow?.[0]?.by_yield) dbFallbackBy = latestByRow[0].by_yield;
        }

        const merged: any[] = [];
        for (const r of pepbData) {
          const by = getByForDate(r.date) ?? dbFallbackBy;
          if (by !== undefined && !isNaN(r.pe) && !isNaN(r.pb) && !isNaN(by)) {
            const row: any = { index_id: idx, date: r.date, pe: r.pe, pb: r.pb, by_yield: by };
            const close = closeMap.get(r.date);
            if (close !== undefined && close > 0) row.close_price = close;
            merged.push(row);
          }
        }
        if (merged.length > 0) {
          const { error } = await sb.from('daily_eyby_data').upsert(merged, { onConflict: 'index_id,date' });
          if (error) throw error;
        }
        const { data: newLatest } = await sb.from('daily_eyby_data').select('date').eq('index_id', idx).order('date', { ascending: false }).limit(1);
        const { count } = await sb.from('daily_eyby_data').select('*', { count: 'exact', head: true }).eq('index_id', idx);
        const newLatestDate = newLatest?.[0]?.date || null;
        await logAudit(sb, idx, merged.length > 0 ? 'ok' : 'no-new-data', newLatestDate,
          `prev=${lastDate} new=${newLatestDate} merged=${merged.length} fetchedPE=${pepbData.length} fetchedBY=${byData.length}`);
        return new Response(JSON.stringify({
          success: true, index: idx, label: indexConfig.label, source: indexConfig.source,
          previousLatest: lastDate, newLatest: newLatestDate, totalRows: count,
          fetchedPE: pepbData.length, fetchedBY: byData.length, fetchedClose: closeMap.size, merged: merged.length,
          byForwardFill: byData.length > 0 ? 'enabled' : (dbFallbackBy !== undefined ? 'db-fallback' : 'none'),
        }), { headers: CORS });
      } catch (e) {
        await logAudit(sb, idx, 'error', null, String(e));
        throw e;
      }
    }

    if (mode === 'update-all') {
      const results: any[] = [];
      for (const idx of Object.keys(INDICES)) {
        try {
          const r = await fetch(`${url.origin}${url.pathname}?mode=update-data&index=${idx}`);
          const j = await r.json(); results.push(j);
        } catch(e) { results.push({ index: idx, error: String(e) }); }
      }
      return new Response(JSON.stringify(results), { headers: CORS });
    }

    if (mode === 'list-indices') {
      return new Response(JSON.stringify(INDICES), { headers: CORS });
    }

    if (mode === 'test-us') {
      const diag: any = { ts: new Date().toISOString(), tests: {} };
      const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
      // Test Yahoo
      try {
        const p1 = Math.floor(Date.now() / 1000) - 86400 * 10;
        const p2 = Math.floor(Date.now() / 1000) + 86400;
        const yUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent('^GSPC')}?period1=${p1}&period2=${p2}&interval=1d`;
        const yr = await fetch(yUrl, { headers: { 'User-Agent': UA } });
        const yText = await yr.text();
        diag.tests.yahoo = { status: yr.status, ok: yr.ok, bodyLen: yText.length, body: yText.substring(0, 500) };
      } catch (e) { diag.tests.yahoo = { error: String(e) }; }
      // Test FRED
      try {
        const fUrl = `https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key=${fredKey}&file_type=json&observation_start=2026-04-01`;
        const fr = await fetch(fUrl, { headers: { 'User-Agent': UA } });
        const fText = await fr.text();
        diag.tests.fred = { status: fr.status, ok: fr.ok, bodyLen: fText.length, body: fText.substring(0, 500) };
      } catch (e) { diag.tests.fred = { error: String(e) }; }
      return new Response(JSON.stringify(diag), { headers: CORS });
    }

    if (mode === 'pe-fetch') {
      const from = url.searchParams.get('from') || '01-Apr-2016';
      const to = url.searchParams.get('to') || '01-Apr-2026';
      const indexName = url.searchParams.get('indexName') || 'NIFTY 50';
      const cinfo = JSON.stringify({name: indexName, startDate: from, endDate: to, indexName});
      const r = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', {
        method: 'POST', headers: NI_HEADERS, body: JSON.stringify({ cinfo }),
      });
      const j = await r.json();
      return new Response(j.d, { headers: CORS });
    }

    if (mode === 'by-fetch-full') {
      const pairId = url.searchParams.get('id') || '24014';
      const allData: any[] = [];
      for (let y = 2016; y <= 2026; y++) {
        const from = y === 2016 ? '2016-04-01' : `${y}-01-01`;
        const to = y === 2026 ? '2026-04-09' : `${y}-12-31`;
        try {
          const r = await fetchWithRetry(`https://api.investing.com/api/financialdata/historical/${pairId}?start-date=${from}&end-date=${to}&time-frame=Daily&add-missing-rows=false`, {
            headers: { 'User-Agent': UA, 'domain-id': 'in' },
          });
          const j = await r.json();
          if (j.data) { for (const d of j.data) { allData.push({ date: d.rowDateTimestamp?.split('T')[0], yield: parseFloat(String(d.last_close).replace(/,/g, '')) }); } }
        } catch(e) {}
      }
      allData.sort((a: any, b: any) => a.date.localeCompare(b.date));
      return new Response(JSON.stringify(allData), { headers: CORS });
    }

    return new Response(JSON.stringify({error:'unknown mode'}), { headers: CORS });
  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), { status: 500, headers: CORS });
  }
});
