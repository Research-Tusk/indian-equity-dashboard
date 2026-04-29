import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': '*',
  'Content-Type': 'application/json',
  'Connection': 'keep-alive'
};
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const NI_HEADERS: Record<string, string> = {
  'Connection': 'keep-alive',
  'Accept': 'application/json, text/javascript, */*; q=0.01',
  'X-Requested-With': 'XMLHttpRequest',
  'User-Agent': UA,
  'Content-Type': 'application/json; charset=UTF-8',
  'Origin': 'https://niftyindices.com',
  'Referer': 'https://niftyindices.com/reports/historical-data',
};
// Realistic browser headers for Investing.com (used for direct AND proxy attempts)
const INV_HEADERS: Record<string, string> = {
  'User-Agent': UA,
  'Accept': 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9',
  'Referer': 'https://www.investing.com/rates-bonds/india-10-year-bond-yield-historical-data',
  'Origin': 'https://www.investing.com',
  'domain-id': 'in',
  'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
};

const INDICES: Record<string, {
  nseIndexName: string; label: string; source: 'api' | 'csv' | 'us';
  yahooTicker?: string; fwdEpsEnvVar?: string; bvpsEnvVar?: string;
  cftcTicker?: string; fmpBreadthExchange?: string;
  // Phase-3: per-country sovereign 10Y FRED series + world mcap share for Global Equities
  bondFredSeries?: string; worldMcapPct?: number;
}> = {
  'nifty50': { nseIndexName: 'NIFTY 50', label: 'Nifty 50', source: 'api', yahooTicker: '%5ENSEI' },
  'nifty-capital-markets': { nseIndexName: 'Nifty Capital Markets', label: 'Nifty Capital Markets', source: 'csv' },
  'nifty-fin-service': { nseIndexName: 'NIFTY FIN SERVICE', label: 'Financials (35.5%)', source: 'api', yahooTicker: 'NIFTY_FIN_SERVICE.NS' },
  'nifty-energy':      { nseIndexName: 'NIFTY ENERGY',     label: 'Energy (11.0%)',     source: 'api', yahooTicker: '%5ECNXENERGY' },
  'nifty-it':          { nseIndexName: 'NIFTY IT',         label: 'IT (9.4%)',          source: 'api', yahooTicker: '%5ECNXIT' },
  'nifty-auto':        { nseIndexName: 'NIFTY AUTO',       label: 'Auto (6.6%)',        source: 'api', yahooTicker: '%5ECNXAUTO' },
  'nifty-fmcg':        { nseIndexName: 'NIFTY FMCG',       label: 'FMCG (6.0%)',        source: 'api', yahooTicker: '%5ECNXFMCG' },
  'nifty-pharma':            { nseIndexName: 'NIFTY PHARMA',          label: 'Pharma',          source: 'api', yahooTicker: '%5ECNXPHARMA' },
  'nifty-infra':             { nseIndexName: 'NIFTY INFRA',           label: 'Infrastructure',  source: 'api', yahooTicker: '%5ECNXINFRA' },
  'nifty-commodities':       { nseIndexName: 'NIFTY COMMODITIES',     label: 'Commodities',     source: 'api', yahooTicker: '%5ECNXCMDT' },
  'nifty-services-sector':   { nseIndexName: 'NIFTY SERV SECTOR',    label: 'Services Sector', source: 'api', yahooTicker: '%5ECNXSERVICE' },
  'nifty-realty':            { nseIndexName: 'NIFTY REALTY',          label: 'Nifty Realty',    source: 'api', yahooTicker: '%5ECNXREALTY' },
  'sp500': { nseIndexName: '', label: 'S&P 500', source: 'us', yahooTicker: '^GSPC', fwdEpsEnvVar: 'SP500_FWD_EPS_ANCHOR', bvpsEnvVar: 'SP500_BVPS_ANCHOR', cftcTicker: 'CFTC/13874A_FO_ALL', fmpBreadthExchange: 'sp500', bondFredSeries: 'DGS10', worldMcapPct: 62.0 },
  'nasdaq': { nseIndexName: '', label: 'NASDAQ', source: 'us', yahooTicker: '^NDX', fwdEpsEnvVar: 'NASDAQ_FWD_EPS_ANCHOR', bvpsEnvVar: 'NASDAQ_BVPS_ANCHOR', cftcTicker: 'CFTC/209742_FO_ALL', fmpBreadthExchange: 'nasdaq', bondFredSeries: 'DGS10', worldMcapPct: 22.0 },
  // Phase-3 Global Equities: strict clone of US template; country-specific FRED 10Y series; rough fwd EPS / BVPS anchors (env-tunable).
  'kospi200': { nseIndexName: '', label: 'KOSPI 200', source: 'us', yahooTicker: '^KS200', fwdEpsEnvVar: 'KOSPI200_FWD_EPS_ANCHOR', bvpsEnvVar: 'KOSPI200_BVPS_ANCHOR', bondFredSeries: 'IRLTLT01KRM156N', worldMcapPct: 1.7 },
  'bovespa':  { nseIndexName: '', label: 'Bovespa',  source: 'us', yahooTicker: '^BVSP',  fwdEpsEnvVar: 'BOVESPA_FWD_EPS_ANCHOR',  bvpsEnvVar: 'BOVESPA_BVPS_ANCHOR',  bondFredSeries: 'INTGSTBRM193N',   worldMcapPct: 0.5 },
  'twse':     { nseIndexName: '', label: 'TWSE',     source: 'us', yahooTicker: '^TWII',  fwdEpsEnvVar: 'TWSE_FWD_EPS_ANCHOR',     bvpsEnvVar: 'TWSE_BVPS_ANCHOR',     bondFredSeries: 'DGS10',           worldMcapPct: 1.6 },
  // Phase-4 Global Equities expansion
  'nikkei':   { nseIndexName: '', label: 'Nikkei 225', source: 'us', yahooTicker: '^N225',  fwdEpsEnvVar: 'NIKKEI_FWD_EPS_ANCHOR',   bvpsEnvVar: 'NIKKEI_BVPS_ANCHOR',   bondFredSeries: 'IRLTLT01JPM156N', worldMcapPct: 5.5 },
  'stoxx600': { nseIndexName: '', label: 'STOXX 600',  source: 'us', yahooTicker: '^STOXX', fwdEpsEnvVar: 'STOXX600_FWD_EPS_ANCHOR', bvpsEnvVar: 'STOXX600_BVPS_ANCHOR', bondFredSeries: 'IRLTLT01EZM156N', worldMcapPct: 11.0 },
  'hangseng': { nseIndexName: '', label: 'Hang Seng',  source: 'us', yahooTicker: '^HSI',   fwdEpsEnvVar: 'HANGSENG_FWD_EPS_ANCHOR', bvpsEnvVar: 'HANGSENG_BVPS_ANCHOR', bondFredSeries: 'DGS10',           worldMcapPct: 3.5 },
};

const FRED_KEY_FALLBACK = 'd6d6deeb62090decbe4f9f2f684b539b';
const FWD_EPS_DEFAULTS: Record<string, number> = { sp500: 285, nasdaq: 1050, kospi200: 37, bovespa: 13900, twse: 1250, nikkei: 2100, stoxx600: 36, hangseng: 1900 };
const BVPS_DEFAULTS: Record<string, number> = { sp500: 1210, nasdaq: 3530, kospi200: 370, bovespa: 73500, twse: 7300, nikkei: 27000, stoxx600: 255, hangseng: 17000 };
const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MONTH_MAP: Record<string,string> = {Jan:'01',Feb:'02',Mar:'03',Apr:'04',May:'05',Jun:'06',Jul:'07',Aug:'08',Sep:'09',Oct:'10',Nov:'11',Dec:'12'};
const BASELINE_START_ISO = '2016-04-01';
const DEPLOY_VERSION = 'v48-phase5-realty';
// Phase-2: India Mcap/GDP anchor — FRED 'DDDM01INA156NWDB' is annual Stock Mcap to GDP for India.
// Daily Buffett ratio = anchor_ratio * (NSE Total Market index today / index on anchor date).
const INDIA_MCAP_GDP_FRED = 'DDDM01INA156NWDB';
const INDIA_MCAP_PROXY_TICKER = '^CRSLDX';   // Nifty Total Market — raw caret; encodeURIComponent inside fetchYahooDailySeries handles encoding
// Phase-2: NSE constituent weights — JSON from equity-stockIndices.
// Map our index_id -> NSE 'index' query param (URL-encoded form lives in fetchIndexWeights).
const NSE_WEIGHTS_QUERY: Record<string, string> = {
  'nifty50': 'NIFTY 50',
  'nifty-fin-service': 'NIFTY FINANCIAL SERVICES',
  'nifty-energy': 'NIFTY ENERGY',
  'nifty-it': 'NIFTY IT',
  'nifty-auto': 'NIFTY AUTO',
  'nifty-fmcg': 'NIFTY FMCG',
  'nifty-pharma': 'NIFTY PHARMA',
  'nifty-infra': 'NIFTY INFRASTRUCTURE',
  'nifty-commodities': 'NIFTY COMMODITIES',
  'nifty-services-sector': 'NIFTY SERVICES SECTOR',
  'nifty-capital-markets': 'NIFTY CAPITAL MARKETS',
  'nifty-realty': 'NIFTY REALTY',
};

function getSupabase() {
  return createClient(Deno.env.get('SUPABASE_URL')!, Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!);
}

async function fetchWithRetry(url: string, init?: RequestInit, attempts = 3): Promise<Response> {
  let lastErr: any;
  for (let i = 0; i < attempts; i++) {
    try {
      const resp = await fetch(url, init);
      if (resp.ok || resp.status === 404) return resp;
      lastErr = new Error(`HTTP ${resp.status}`);
    } catch (e) { lastErr = e; }
    if (i < attempts - 1) await new Promise(r => setTimeout(r, 500 * Math.pow(2, i)));
  }
  throw lastErr;
}

// Proxy chain for Investing.com (they 403 Supabase Edge IPs directly).
// Tries direct → corsproxy.io → allorigins.win.
async function fetchInvestingViaProxies(targetUrl: string): Promise<{ data: any[] } | null> {
  const attempts: Array<{ name: string; url: string }> = [
    { name: 'direct', url: targetUrl },
    { name: 'corsproxy.io', url: `https://corsproxy.io/?url=${encodeURIComponent(targetUrl)}` },
    { name: 'allorigins.win', url: `https://api.allorigins.win/raw?url=${encodeURIComponent(targetUrl)}` },
  ];
  for (const a of attempts) {
    try {
      const r = await fetchWithRetry(a.url, { headers: INV_HEADERS }, 2);
      if (!r.ok) { console.warn(`[inv] ${a.name} HTTP ${r.status}`); continue; }
      const text = await r.text();
      if (!text || text.startsWith('<')) { console.warn(`[inv] ${a.name} non-JSON body (${text.length} bytes)`); continue; }
      let parsed: any;
      try { parsed = JSON.parse(text); } catch { console.warn(`[inv] ${a.name} JSON.parse failed`); continue; }
      if (parsed && Array.isArray(parsed.data)) {
        console.log(`[inv] ${a.name} OK: ${parsed.data.length} rows`);
        return parsed;
      }
      console.warn(`[inv] ${a.name} unexpected shape`);
    } catch (e) {
      console.warn(`[inv] ${a.name} error:`, e);
    }
  }
  return null;
}

async function logAudit(sb: any, index: string, status: string, latestDate: string | null, message: string) {
  try { await sb.from('refresh_audit').insert({ index_id: index, status, latest_date: latestDate, message }); } catch (_e) {}
}

function parseCsvLine(line: string): string[] {
  const result: string[] = []; let current = ''; let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') inQuotes = !inQuotes;
    else if (ch === ',' && !inQuotes) { result.push(current.trim()); current = ''; }
    else current += ch;
  }
  result.push(current.trim()); return result;
}

function fmtNseDate(d: Date): string { return `${String(d.getDate()).padStart(2,'0')}-${MONTH_NAMES[d.getMonth()]}-${d.getFullYear()}`; }
function parseNseDate(dateStr: string): string { const parts = dateStr.trim().split(' '); return `${parts[2]}-${MONTH_MAP[parts[1]] || '01'}-${parts[0].padStart(2,'0')}`; }
function toISODate(ts: number): string { const d = new Date(ts * 1000); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`; }

async function fetchPEPBCloseFromCSV(indexName: string, fromDate: string, toDate: string): Promise<{date: string; pe: number; pb: number; close: number}[]> {
  const results: {date: string; pe: number; pb: number; close: number}[] = [];
  const start = new Date(fromDate); const end = new Date(toDate);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    if (d.getDay() === 0 || d.getDay() === 6) continue;
    const dd = String(d.getDate()).padStart(2,'0'); const mm = String(d.getMonth()+1).padStart(2,'0'); const yyyy = d.getFullYear();
    const isoDate = `${yyyy}-${mm}-${dd}`;
    try {
      const url = `https://www.niftyindices.com/Daily_Snapshot/ind_close_all_${dd}${mm}${yyyy}.csv`;
      const resp = await fetchWithRetry(url, { headers: { 'User-Agent': UA } });
      if (!resp.ok) continue;
      const text = await resp.text();
      if (text.startsWith('<!DOCTYPE') || text.includes('<html')) continue;
      const lines = text.split('\n').filter(l => l.trim()); if (lines.length < 2) continue;
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
          if (!isNaN(pe) && pe > 0 && !isNaN(pb) && pb > 0) results.push({ date: isoDate, pe, pb, close: isNaN(close) ? 0 : close });
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
    const dd = String(d.getDate()).padStart(2,'0'); const mm = String(d.getMonth()+1).padStart(2,'0'); const yyyy = d.getFullYear();
    const isoDate = `${yyyy}-${mm}-${dd}`;
    try {
      const url = `https://www.niftyindices.com/Daily_Snapshot/ind_close_all_${dd}${mm}${yyyy}.csv`;
      const resp = await fetchWithRetry(url, { headers: { 'User-Agent': UA } });
      if (!resp.ok) continue;
      const text = await resp.text();
      if (text.startsWith('<!DOCTYPE') || text.includes('<html')) continue;
      const lines = text.split('\n').filter(l => l.trim()); if (lines.length < 2) continue;
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

// BUG FIX (Auto/FMCG cross-contamination): NSE Backpage.aspx returns the previous request's payload
// when called in rapid succession because their CDN keys off the cinfo body hash + session cookie state.
// We prime a per-call session cookie, embed a unique nonce in cinfo, and verify variance on the response.
async function nsePrimeSessionCookie(): Promise<string> {
  try {
    const r = await fetch('https://niftyindices.com/reports/historical-data', { headers: { 'User-Agent': UA } });
    const setCookie = r.headers.get('set-cookie') || '';
    return setCookie.split(',').map(s => s.split(';')[0].trim()).filter(Boolean).join('; ');
  } catch { return ''; }
}

async function fetchPEPB(indexName: string, fromDate: string, toDate: string) {
  // Embed a per-request nonce so NSE cannot serve a cached-from-prior-index response.
  const nonce = Math.random().toString(36).slice(2, 10);
  const cinfo = JSON.stringify({ name: indexName, startDate: fromDate, endDate: toDate, indexName, _nonce: nonce });
  const cookie = await nsePrimeSessionCookie();
  const headers = cookie ? { ...NI_HEADERS, Cookie: cookie } : NI_HEADERS;
  const r = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', { method: 'POST', headers, body: JSON.stringify({ cinfo }) });
  const j = await r.json(); const raw = JSON.parse(j.d);
  const rows = raw.map((r: any) => ({ date: parseNseDate(r.DATE), pe: parseFloat(r.pe), pb: parseFloat(r.pb) }));
  // Variance guard: if the upstream returned a series with zero PE variance over >50 rows it's a stale cache hit.
  if (rows.length > 50) {
    const pes = rows.map((x: any) => x.pe).filter((v: number) => Number.isFinite(v));
    const lo = Math.min(...pes), hi = Math.max(...pes);
    if (hi - lo < 0.001) {
      console.warn(`[fetchPEPB] flat-PE response for ${indexName} (${rows.length} rows, range ${lo}-${hi}) — likely stale cache; one retry with fresh session.`);
      const cookie2 = await nsePrimeSessionCookie();
      const headers2 = cookie2 ? { ...NI_HEADERS, Cookie: cookie2 } : NI_HEADERS;
      const cinfo2 = JSON.stringify({ name: indexName, startDate: fromDate, endDate: toDate, indexName, _nonce: nonce + '-r' });
      const r2 = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', { method: 'POST', headers: headers2, body: JSON.stringify({ cinfo: cinfo2 }) });
      const j2 = await r2.json(); const raw2 = JSON.parse(j2.d);
      return raw2.map((r: any) => ({ date: parseNseDate(r.DATE), pe: parseFloat(r.pe), pb: parseFloat(r.pb) }));
    }
  }
  return rows;
}

async function fetchHistoricalClose(indexName: string, fromDate: string, toDate: string): Promise<Map<string, number>> {
  const closeMap = new Map<string, number>();
  const cinfo = JSON.stringify({ name: indexName, startDate: fromDate, endDate: toDate, indexName });
  try {
    const r = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getHistoricaldataDBtoString', { method: 'POST', headers: NI_HEADERS, body: JSON.stringify({ cinfo }) });
    if (!r.ok) return closeMap; const j = await r.json(); const raw = JSON.parse(j.d);
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
    const p1 = Math.floor(new Date(fromISO).getTime() / 1000); const p2 = Math.floor(new Date(toISO).getTime() / 1000) + 86400;
    const r = await fetchWithRetry(`https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?period1=${p1}&period2=${p2}&interval=1d`, { headers: { 'User-Agent': UA } });
    if (!r.ok) return closeMap; const j = await r.json(); const result = j?.chart?.result?.[0]; if (!result) return closeMap;
    const timestamps = result.timestamp || []; const closes = result.indicators?.quote?.[0]?.close || [];
    for (let i = 0; i < timestamps.length; i++) { if (closes[i] != null && !isNaN(closes[i])) closeMap.set(toISODate(timestamps[i]), +closes[i].toFixed(2)); }
  } catch (_e) {}
  return closeMap;
}

// India 10Y bond yield from Investing.com pairId 24014 — year-chunked with proxy fallback.
// Phase-2: India Buffett Indicator — anchored to FRED annual ratio, rolled forward by Nifty Total Market daily delta.
async function fetchIndiaMcapGDP(fromIso: string, toIso: string): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
  const annual = await fetchFRED(INDIA_MCAP_GDP_FRED, fredKey, '2010-01-01');
  const annualDates = Array.from(annual.keys()).sort();
  if (annualDates.length === 0) return out;
  const anchorDate = annualDates[annualDates.length - 1];
  const anchorRatio = annual.get(anchorDate)!;
  // Get the proxy series from anchor date forward; need anchor close + the request range
  const series = await fetchYahooDailySeries(INDIA_MCAP_PROXY_TICKER, anchorDate, toIso);
  if (series.length === 0) return out;
  // Anchor close: nearest series row >= anchorDate
  const anchorRow = series.find(s => s.date >= anchorDate) || series[0];
  if (!anchorRow || !anchorRow.close) return out;
  for (const r of series) {
    if (r.date < fromIso) continue;
    out.set(r.date, +(anchorRatio * (r.close / anchorRow.close)).toFixed(2));
  }
  return out;
}

// Phase-2: NSE constituent weights — fetches equity-stockIndices JSON via proxy chain.
// Returns Top-N constituents by weight; persists to index_weights.
async function fetchIndexWeightsRaw(nseIndexQuery: string): Promise<{symbol: string; companyName: string; industry: string; weight: number}[]> {
  const target = `https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(nseIndexQuery)}`;
  const NSE_HDR: Record<string, string> = {
    'User-Agent': UA,
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com/market-data/live-equity-market',
    'Origin': 'https://www.nseindia.com',
    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
  };
  const routes: Array<{ name: string; url: string }> = [
    { name: 'direct', url: target },
    { name: 'corsproxy.io', url: `https://corsproxy.io/?url=${encodeURIComponent(target)}` },
    { name: 'allorigins.win', url: `https://api.allorigins.win/raw?url=${encodeURIComponent(target)}` },
  ];
  for (const r of routes) {
    try {
      const resp = await fetchWithRetry(r.url, { headers: NSE_HDR }, 2);
      if (!resp.ok) { console.warn(`[wts] ${r.name} HTTP ${resp.status}`); continue; }
      const text = await resp.text();
      if (!text || text.startsWith('<')) continue;
      let j: any;
      try { j = JSON.parse(text); } catch { continue; }
      const rows: any[] = Array.isArray(j?.data) ? j.data : [];
      const constituents = rows.filter(x => x?.priority !== 1 && (x?.symbol && x?.symbol !== nseIndexQuery));
      const out = constituents.map(x => {
        const meta = x.meta || {};
        const w = parseFloat(String(x.weightInIndex ?? x.weight ?? meta.weightInIndex ?? meta.weight ?? '0').replace(/,/g, ''));
        return {
          symbol: String(x.symbol || ''),
          companyName: String(meta.companyName || x.identifier || x.symbol || ''),
          industry: String(meta.industry || x.industry || ''),
          weight: isNaN(w) ? 0 : +w.toFixed(4),
        };
      }).filter(c => c.symbol && c.weight > 0);
      out.sort((a, b) => b.weight - a.weight);
      if (out.length > 0) { console.log(`[wts] ${r.name} ${nseIndexQuery}: ${out.length} constituents`); return out; }
    } catch (e) { console.warn(`[wts] ${r.name} error:`, e); }
  }
  return [];
}

async function fetchBY(fromDate: string, toDate: string) {
  const allData: { date: string; by: number }[] = [];
  const startYear = parseInt(fromDate.split('-')[0]);
  const endYear = parseInt(toDate.split('-')[0]);
  for (let y = startYear; y <= endYear; y++) {
    const from = y === startYear ? fromDate : `${y}-01-01`;
    const to = y === endYear ? toDate : `${y}-12-31`;
    const target = `https://api.investing.com/api/financialdata/historical/24014?start-date=${from}&end-date=${to}&time-frame=Daily&add-missing-rows=false`;
    const j = await fetchInvestingViaProxies(target);
    if (j && Array.isArray(j.data)) {
      for (const d of j.data) {
        const date = (d.rowDateTimestamp || '').split('T')[0];
        const raw = d.last_close ?? d.last_closeRaw;
        const val = parseFloat(String(raw).replace(/,/g, ''));
        if (date && !isNaN(val)) allData.push({ date, by: val });
      }
    }
  }
  // Dedupe by date (keep last occurrence)
  const byDate = new Map<string, number>();
  for (const r of allData) byDate.set(r.date, r.by);
  const out = Array.from(byDate.entries()).map(([date, by]) => ({ date, by }));
  out.sort((a, b) => a.date.localeCompare(b.date));
  return out;
}

async function fetchFRED(seriesId: string, apiKey: string, startDate: string): Promise<Map<string, number>> {
  const result = new Map<string, number>();
  try {
    const r = await fetchWithRetry(`https://api.stlouisfed.org/fred/series/observations?series_id=${seriesId}&api_key=${apiKey}&file_type=json&observation_start=${startDate}`, { headers: { 'User-Agent': UA } });
    if (!r.ok) return result; const j = await r.json();
    for (const obs of (j.observations || [])) { if (obs.value && obs.value !== '.') result.set(obs.date, parseFloat(obs.value)); }
  } catch (e) { console.error(`FRED fetch error ${seriesId}:`, e); }
  return result;
}

function forwardFill(dates: string[], sparseMap: Map<string, number>): Map<string, number> {
  const filled = new Map<string, number>(); let lastVal: number | undefined;
  for (const d of dates) { const v = sparseMap.get(d); if (v !== undefined) lastVal = v; if (lastVal !== undefined) filled.set(d, lastVal); }
  return filled;
}

function computeRSI14(closes: number[]): (number | null)[] {
  const period = 14; const rsi: (number | null)[] = new Array(closes.length).fill(null);
  if (closes.length < period + 1) return rsi;
  let avgGain = 0, avgLoss = 0;
  for (let i = 1; i <= period; i++) { const ch = closes[i] - closes[i-1]; if (ch > 0) avgGain += ch; else avgLoss -= ch; }
  avgGain /= period; avgLoss /= period;
  rsi[period] = avgLoss === 0 ? 100 : +(100 - 100/(1+avgGain/avgLoss)).toFixed(2);
  for (let i = period+1; i < closes.length; i++) {
    const ch = closes[i] - closes[i-1];
    avgGain = (avgGain*(period-1) + (ch > 0 ? ch : 0))/period;
    avgLoss = (avgLoss*(period-1) + (ch < 0 ? -ch : 0))/period;
    rsi[i] = avgLoss === 0 ? 100 : +(100 - 100/(1+avgGain/avgLoss)).toFixed(2);
  }
  return rsi;
}

async function fetchCFTCCOT(cftcTicker: string, startDate: string): Promise<Map<string, number>> {
  const result = new Map<string, number>(); const apiKey = Deno.env.get('NASDAQ_DATA_LINK_KEY') || '';
  if (!apiKey || !cftcTicker) return result;
  try {
    const r = await fetchWithRetry(`https://data.nasdaq.com/api/v3/datasets/${cftcTicker}.json?start_date=${startDate}&api_key=${apiKey}&order=asc`, { headers: { 'User-Agent': UA } });
    if (!r.ok) return result; const j = await r.json();
    const cols: string[] = (j.dataset?.column_names || []).map((c: string) => c.toLowerCase()); const rows: any[][] = j.dataset?.data || [];
    const ncLongI = cols.findIndex(c => c.includes('noncommercial') && c.includes('long') && !c.includes('spread'));
    const ncShortI = cols.findIndex(c => c.includes('noncommercial') && c.includes('short') && !c.includes('spread'));
    const oiI = cols.findIndex(c => c.includes('open interest'));
    const longI = ncLongI >= 0 ? ncLongI : cols.findIndex(c => c.includes('long') && !c.includes('spread') && !c.includes('change'));
    const shortI = ncShortI >= 0 ? ncShortI : cols.findIndex(c => c.includes('short') && !c.includes('spread') && !c.includes('change'));
    if (longI < 0 || shortI < 0) return result;
    for (const row of rows) {
      const date = row[0]; const ncLong = parseFloat(row[longI]); const ncShort = parseFloat(row[shortI]);
      if (isNaN(ncLong) || isNaN(ncShort)) continue; const net = ncLong - ncShort;
      if (oiI >= 0) { const oi = parseFloat(row[oiI]); if (!isNaN(oi) && oi > 0) result.set(date, +((net/oi)*100).toFixed(2)); }
      else result.set(date, +net.toFixed(0));
    }
  } catch (e) { console.error(`CFTC fetch error:`, e); }
  return result;
}

async function fetchFMPBreadth(exchange: string, startDate: string): Promise<Map<string, number>> {
  const result = new Map<string, number>(); const apiKey = Deno.env.get('FMP_API_KEY') || '';
  if (!apiKey || !exchange) return result;
  try {
    const r = await fetchWithRetry(`https://financialmodelingprep.com/stable/market-breadth?type=${exchange}&apikey=${apiKey}`, { headers: { 'User-Agent': UA } });
    if (!r.ok) return result; const j = await r.json();
    if (Array.isArray(j)) { for (const row of j) { const date = row.date; const pct = row.averageAbove200DMA ?? row.percentAbove200DMA ?? row.breadth ?? null; if (date && pct != null && !isNaN(pct)) { const val = pct > 1 ? pct : pct * 100; result.set(date, +val.toFixed(2)); } } }
  } catch (e) { console.error(`FMP breadth error:`, e); }
  return result;
}

async function refreshUSIndex(sb: any, idx: string, indexConfig: typeof INDICES[string], fullRefresh = false): Promise<{ merged: number; fetchedCloses: number; fetchedBY: number; fetchedWilshire: number; fetchedGDP: number; fetchedCFTC: number; fetchedBreadth: number; latestDate: string | null }> {
  const { data: latest } = await sb.from('daily_eyby_data').select('date').eq('index_id', idx).order('date', { ascending: false }).limit(1);
  const lastDate = latest?.[0]?.date || BASELINE_START_ISO;
  let fromStr: string;
  if (fullRefresh) { fromStr = BASELINE_START_ISO; } else { const fd = new Date(lastDate); fd.setDate(fd.getDate()-7); fromStr = fd.toISOString().split('T')[0]; }
  const today = new Date(); today.setDate(today.getDate()+1); const toStr = today.toISOString().split('T')[0];
  const ticker = indexConfig.yahooTicker || '';
  const closeSeries = await fetchYahooDailySeries(ticker, fromStr, toStr);
  if (closeSeries.length === 0) return { merged: 0, fetchedCloses: 0, fetchedBY: 0, fetchedWilshire: 0, fetchedGDP: 0, fetchedCFTC: 0, fetchedBreadth: 0, latestDate: lastDate };
  const tradingDates = closeSeries.map(c => c.date);
  const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
  // Phase-3: per-country bond yield series (defaults to US DGS10)
  const bondSeries = indexConfig.bondFredSeries || 'DGS10';
  const [rawBY, rawGDP, wilshireSeries, rawCFTC, rawBreadth] = await Promise.all([
    fetchFRED(bondSeries, fredKey, fromStr), fetchFRED('GDP', fredKey, BASELINE_START_ISO),
    fetchYahooDailySeries('^W5000', fromStr, toStr), fetchCFTCCOT(indexConfig.cftcTicker || '', fromStr),
    fetchFMPBreadth(indexConfig.fmpBreadthExchange || '', fromStr),
  ]);
  const rawWilshire = new Map<string, number>(); for (const w of wilshireSeries) rawWilshire.set(w.date, w.close);
  const byFilled = forwardFill(tradingDates, rawBY); const wilshireFilled = forwardFill(tradingDates, rawWilshire);
  const gdpFilled = forwardFill(tradingDates, rawGDP); const cftcFilled = forwardFill(tradingDates, rawCFTC);
  const breadthFilled = forwardFill(tradingDates, rawBreadth);
  if (byFilled.size === 0) {
    const { data: latestByRow } = await sb.from('daily_eyby_data').select('by_yield').eq('index_id', idx).not('by_yield', 'is', null).order('date', { ascending: false }).limit(1);
    if (latestByRow?.[0]?.by_yield) { const fb = latestByRow[0].by_yield; for (const d of tradingDates) byFilled.set(d, fb); }
  }
  const fwdEpsAnchor = parseFloat(Deno.env.get(indexConfig.fwdEpsEnvVar || '') || '0') || FWD_EPS_DEFAULTS[idx] || 0;
  const bvpsAnchor = parseFloat(Deno.env.get(indexConfig.bvpsEnvVar || '') || '0') || BVPS_DEFAULTS[idx] || 0;
  const rows: any[] = [];
  for (const { date, close } of closeSeries) {
    const by = byFilled.get(date); if (by === undefined) continue;
    const pe = fwdEpsAnchor > 0 ? +(close/fwdEpsAnchor).toFixed(2) : null;
    const pb = bvpsAnchor > 0 ? +(close/bvpsAnchor).toFixed(2) : null;
    const wil = wilshireFilled.get(date); const gdp = gdpFilled.get(date);
    const mcapGdp = (wil && gdp && gdp > 0) ? +((wil/gdp)*100).toFixed(2) : null;
    rows.push({ index_id: idx, date, pe, pb, by_yield: +by.toFixed(4), close_price: close, mcap_gdp: mcapGdp, cftc_net_pct: cftcFilled.get(date) ?? null, breadth_pct: breadthFilled.get(date) ?? null });
  }
  let merged = 0;
  for (let i = 0; i < rows.length; i += 500) { const chunk = rows.slice(i, i+500); const { error } = await sb.from('daily_eyby_data').upsert(chunk, { onConflict: 'index_id,date' }); if (error) throw error; merged += chunk.length; }
  return { merged, fetchedCloses: closeSeries.length, fetchedBY: rawBY.size, fetchedWilshire: rawWilshire.size, fetchedGDP: rawGDP.size, fetchedCFTC: rawCFTC.size, fetchedBreadth: rawBreadth.size, latestDate: rows.length > 0 ? rows[rows.length-1].date : lastDate };
}

async function fetchYahooDailySeries(symbol: string, fromISO: string, toISO: string): Promise<{date: string; close: number}[]> {
  const out: {date: string; close: number}[] = [];
  try {
    const p1 = Math.floor(new Date(fromISO).getTime()/1000); const p2 = Math.floor(new Date(toISO).getTime()/1000)+86400;
    const r = await fetchWithRetry(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?period1=${p1}&period2=${p2}&interval=1d&includeAdjustedClose=true`, { headers: { 'User-Agent': UA } });
    if (!r.ok) return out; const j = await r.json(); const result = j?.chart?.result?.[0]; if (!result) return out;
    const timestamps: number[] = result.timestamp || []; const closes: (number|null)[] = result.indicators?.quote?.[0]?.close || [];
    for (let i = 0; i < timestamps.length; i++) { const c = closes[i]; if (c != null && !isNaN(c) && c > 0) out.push({ date: toISODate(timestamps[i]), close: +c.toFixed(4) }); }
  } catch (_e) {}
  return out;
}

async function fetchYahooQuarterlyFundamentals(symbol: string): Promise<{ epsQ: {date: string; value: number}[]; bvps: {date: string; value: number}[]; }> {
  const epsQ: {date: string; value: number}[] = []; const bvps: {date: string; value: number}[] = [];
  try {
    const end = Math.floor(Date.now()/1000)+86400;
    const r = await fetchWithRetry(`https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/${encodeURIComponent(symbol)}?symbol=${encodeURIComponent(symbol)}&type=quarterlyDilutedEPS,quarterlyBookValuePerShare&period1=0&period2=${end}&corsDomain=finance.yahoo.com`, { headers: { 'User-Agent': UA } });
    if (!r.ok) return { epsQ, bvps }; const j = await r.json();
    for (const series of (j?.timeseries?.result || [])) {
      const typeKey = series?.meta?.type?.[0]; if (!typeKey) continue; const rows = series[typeKey]; if (!Array.isArray(rows)) continue;
      for (const row of rows) { if (!row) continue; const asOf = row.asOfDate; const raw = row.reportedValue?.raw;
        if (typeof raw === 'number' && !isNaN(raw) && typeof asOf === 'string') { if (typeKey === 'quarterlyDilutedEPS') epsQ.push({ date: asOf, value: raw }); else if (typeKey === 'quarterlyBookValuePerShare') bvps.push({ date: asOf, value: raw }); }
      }
    }
  } catch (_e) {}
  epsQ.sort((a,b) => a.date.localeCompare(b.date)); bvps.sort((a,b) => a.date.localeCompare(b.date)); return { epsQ, bvps };
}

function buildTtmEpsTimeline(epsQ: {date: string; value: number}[]): {effectiveFrom: string; ttmEps: number}[] {
  const timeline: {effectiveFrom: string; ttmEps: number}[] = [];
  for (let i = 3; i < epsQ.length; i++) { timeline.push({ effectiveFrom: epsQ[i].date, ttmEps: +(epsQ[i].value+epsQ[i-1].value+epsQ[i-2].value+epsQ[i-3].value).toFixed(4) }); }
  return timeline;
}

function pickByDate<T extends {effectiveFrom?: string; date?: string}>(timeline: T[], date: string, key: 'effectiveFrom' | 'date'): T | null {
  if (!timeline.length) return null; let lo = 0, hi = timeline.length-1, best = -1;
  while (lo <= hi) { const mid = (lo+hi)>>1; if ((timeline[mid] as any)[key] <= date) { best = mid; lo = mid+1; } else hi = mid-1; }
  return best >= 0 ? timeline[best] : null;
}

async function refreshStock(sb: any, meta: { ticker: string; yahoo_symbol: string; listing_date: string | null }): Promise<{ ticker: string; fetchedPrices: number; epsQuarters: number; bvpsQuarters: number; merged: number; firstDate: string | null; lastDate: string | null; note?: string }> {
  const effectiveStart = (meta.listing_date && meta.listing_date > BASELINE_START_ISO) ? meta.listing_date : BASELINE_START_ISO;
  const todayIso = new Date().toISOString().split('T')[0];
  const { data: latest } = await sb.from('stocks_daily').select('date').eq('ticker', meta.ticker).order('date', { ascending: false }).limit(1);
  let fromIso = effectiveStart; const lastKnown = latest?.[0]?.date || null;
  if (lastKnown) { const d = new Date(lastKnown); d.setDate(d.getDate()-7); const rolled = d.toISOString().split('T')[0]; if (rolled > effectiveStart) fromIso = rolled; }
  const prices = await fetchYahooDailySeries(meta.yahoo_symbol, fromIso, todayIso);
  if (prices.length === 0) return { ticker: meta.ticker, fetchedPrices: 0, epsQuarters: 0, bvpsQuarters: 0, merged: 0, firstDate: null, lastDate: null, note: 'no-price-data' };
  const { epsQ, bvps } = await fetchYahooQuarterlyFundamentals(meta.yahoo_symbol);
  const ttmTimeline = buildTtmEpsTimeline(epsQ); const rows: any[] = [];
  for (const p of prices) {
    const row: any = { ticker: meta.ticker, date: p.date, close: p.close };
    const ttmMatch = pickByDate(ttmTimeline as any[], p.date, 'effectiveFrom') as any;
    if (ttmMatch?.ttmEps > 0) { row.eps_ttm = +ttmMatch.ttmEps.toFixed(4); row.pe = +(p.close/ttmMatch.ttmEps).toFixed(4); row.earnings_yield = +((ttmMatch.ttmEps/p.close)*100).toFixed(4); }
    const bvpsMatch = pickByDate(bvps as any[], p.date, 'date') as any;
    if (bvpsMatch?.value > 0) { row.bvps = +bvpsMatch.value.toFixed(4); row.pb = +(p.close/bvpsMatch.value).toFixed(4); }
    rows.push(row);
  }
  let merged = 0;
  for (let i = 0; i < rows.length; i += 500) { const chunk = rows.slice(i, i+500); const { error } = await sb.from('stocks_daily').upsert(chunk, { onConflict: 'ticker,date' }); if (error) throw error; merged += chunk.length; }
  return { ticker: meta.ticker, fetchedPrices: prices.length, epsQuarters: epsQ.length, bvpsQuarters: bvps.length, merged, firstDate: prices[0].date, lastDate: prices[prices.length-1].date };
}

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });
  const url = new URL(req.url); const mode = url.searchParams.get('mode') || 'pe-fetch';
  try {
    if (mode === 'get-data') {
      const idx = url.searchParams.get('index') || 'nifty50'; const idxConfig = INDICES[idx]; const sb = getSupabase();
      const allRows: any[] = []; let from = 0; const pageSize = 1000;
      const selectCols = idxConfig?.source === 'us' ? 'date, pe, pb, by_yield, close_price, mcap_gdp, cftc_net_pct, breadth_pct' : 'date, pe, pb, by_yield, close_price';
      while (true) { const { data, error } = await sb.from('daily_eyby_data').select(selectCols).eq('index_id', idx).order('date', { ascending: true }).range(from, from+pageSize-1); if (error) throw error; if (!data || data.length === 0) break; allRows.push(...data); if (data.length < pageSize) break; from += pageSize; }
      if (idxConfig?.source === 'us' && allRows.length > 0) {
        const fwdEps = parseFloat(Deno.env.get(idxConfig.fwdEpsEnvVar || '') || '0') || FWD_EPS_DEFAULTS[idx] || 0;
        const closes = allRows.map((r: any) => r.close_price || 0); const rsiArr = computeRSI14(closes);
        const enriched = allRows.map((r: any, i: number) => {
          const close = r.close_price || 0; const gsec10 = r.by_yield || 0; const pe = r.pe || 0;
          const fwd_ey = fwdEps > 0 && close > 0 ? +((fwdEps/close)*100).toFixed(4) : 0;
          const earningsYield = pe > 0 ? +(100/pe).toFixed(4) : 0; const eyby = gsec10 > 0 ? +(earningsYield/gsec10).toFixed(4) : 0;
          const pb = r.pb || 0; const roe = (pb > 0 && pe > 0) ? +((pb/pe)*100).toFixed(2) : null;
          const rawCftc = r.cftc_net_pct; const fiiNetLong = rawCftc != null ? Math.max(0, Math.min(100, 50 + rawCftc * 1.5)) : null;
          return { date: r.date, close, pe, pb, gsec10, fwd_ey, erp: +(fwd_ey - gsec10).toFixed(4), eyby, rsi14: rsiArr[i], mcap_gdp: r.mcap_gdp || null, roe, fii_fut_net_long: fiiNetLong != null ? +fiiNetLong.toFixed(2) : null, breadth: r.breadth_pct != null ? +r.breadth_pct : null };
        });
        return new Response(JSON.stringify(enriched), { headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' } });
      }
      const compact = allRows.map((r: any) => { const roe = (r.pb > 0 && r.pe > 0) ? +((r.pb/r.pe)*100).toFixed(2) : null; return [r.date, r.pe, r.pb, r.by_yield, r.close_price, roe]; });
      return new Response(JSON.stringify(compact), { headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' } });
    }
    if (mode === 'env-check') {
      const envKeys = ['SUPABASE_URL','SUPABASE_SERVICE_ROLE_KEY','FRED_API_KEY','FMP_API_KEY','NASDAQ_DATA_LINK_KEY','SP500_FWD_EPS_ANCHOR','SP500_BVPS_ANCHOR','NASDAQ_FWD_EPS_ANCHOR','NASDAQ_BVPS_ANCHOR'];
      const report: Record<string, string> = {};
      for (const k of envKeys) { const v = Deno.env.get(k); report[k] = v ? `SET (${v.length} chars)` : 'NOT SET'; }
      for (const idx of ['sp500','nasdaq'] as const) { const cfg = INDICES[idx]; report[`${idx}_effective_fwd_eps`] = String(parseFloat(Deno.env.get(cfg.fwdEpsEnvVar||'')||'0')||FWD_EPS_DEFAULTS[idx]||0); report[`${idx}_effective_bvps`] = String(parseFloat(Deno.env.get(cfg.bvpsEnvVar||'')||'0')||BVPS_DEFAULTS[idx]||0); }
      report['deploy_version'] = DEPLOY_VERSION;
      return new Response(JSON.stringify({ ts: new Date().toISOString(), env: report }), { headers: CORS });
    }
    if (mode === 'health') {
      const sb = getSupabase(); const out: any = { ts: new Date().toISOString(), deploy_version: DEPLOY_VERSION, indices: {}, stocks: {} };
      for (const idx of Object.keys(INDICES)) { const { data } = await sb.from('daily_eyby_data').select('date, pe, pb, by_yield, close_price').eq('index_id', idx).order('date', { ascending: false }).limit(1); const row = data?.[0]; out.indices[idx] = { label: INDICES[idx].label, latestDate: row?.date || null, hasPe: row?.pe != null, hasPb: row?.pb != null, hasByYield: row?.by_yield != null, hasClose: row?.close_price != null }; }
      try { const { data: stockMeta } = await sb.from('stocks_master').select('ticker').eq('is_active', true); for (const s of (stockMeta || [])) { const { data } = await sb.from('stocks_daily').select('date, close, pe, pb').eq('ticker', s.ticker).order('date', { ascending: false }).limit(1); const r = data?.[0]; out.stocks[s.ticker] = { latestDate: r?.date || null, hasClose: r?.close != null, hasPe: r?.pe != null, hasPb: r?.pb != null }; } } catch (_e) {}
      try { const { data: audit } = await sb.from('refresh_audit').select('index_id, status, latest_date, message, created_at').order('created_at', { ascending: false }).limit(15); out.recentAudit = audit || []; } catch (_e) { out.recentAudit = []; }
      return new Response(JSON.stringify(out), { headers: CORS });
    }
    if (mode === 'stocks-list') { const sb = getSupabase(); const { data, error } = await sb.from('stocks_master').select('ticker, name, yahoo_symbol, exchange, listing_date, sector, is_active').eq('is_active', true).order('name', { ascending: true }); if (error) throw error; return new Response(JSON.stringify(data || []), { headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' } }); }
    if (mode === 'stocks-data') { const ticker = url.searchParams.get('ticker'); if (!ticker) return new Response(JSON.stringify({ error: 'ticker required' }), { status: 400, headers: CORS }); const sb = getSupabase(); const all: any[] = []; let from = 0; while (true) { const { data, error } = await sb.from('stocks_daily').select('date, close, pe, pb, eps_ttm, bvps, earnings_yield').eq('ticker', ticker).order('date', { ascending: true }).range(from, from+999); if (error) throw error; if (!data || data.length === 0) break; all.push(...data); if (data.length < 1000) break; from += 1000; } return new Response(JSON.stringify({ ticker, rows: all.map((r: any) => [r.date, r.close, r.pe, r.pb, r.eps_ttm, r.bvps, r.earnings_yield]) }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' } }); }
    if (mode === 'stocks-update') { const ticker = url.searchParams.get('ticker'); if (!ticker) return new Response(JSON.stringify({ error: 'ticker required' }), { status: 400, headers: CORS }); const sb = getSupabase(); const { data, error } = await sb.from('stocks_master').select('ticker, yahoo_symbol, listing_date, is_active').eq('ticker', ticker).limit(1); if (error) throw error; const meta = data?.[0]; if (!meta) return new Response(JSON.stringify({ error: 'unknown ticker' }), { status: 404, headers: CORS }); try { const result = await refreshStock(sb, meta); await logAudit(sb, `stock:${ticker}`, result.note ? 'warn' : 'ok', result.lastDate, `fetched=${result.fetchedPrices} merged=${result.merged}`); return new Response(JSON.stringify(result), { headers: CORS }); } catch (e) { await logAudit(sb, `stock:${ticker}`, 'error', null, String(e)); throw e; } }
    if (mode === 'stocks-update-all') { const sb = getSupabase(); const { data: list, error } = await sb.from('stocks_master').select('ticker, yahoo_symbol, listing_date, is_active').eq('is_active', true).order('ticker', { ascending: true }); if (error) throw error; const results: any[] = []; for (const meta of (list || [])) { try { const r = await refreshStock(sb, meta as any); results.push(r); } catch (e) { results.push({ ticker: meta.ticker, error: String(e) }); } } return new Response(JSON.stringify(results), { headers: CORS }); }
    if (mode === 'backfill-close') {
      const idx = url.searchParams.get('index') || 'nifty50'; const indexConfig = INDICES[idx]; if (!indexConfig) return new Response(JSON.stringify({error: `Unknown index: ${idx}`}), { headers: CORS }); const sb = getSupabase();
      const missingRows: any[] = []; let offset = 0;
      while (true) { const { data, error } = await sb.from('daily_eyby_data').select('date').eq('index_id', idx).is('close_price', null).order('date', { ascending: true }).range(offset, offset+999); if (error) throw error; if (!data || data.length === 0) break; missingRows.push(...data); if (data.length < 1000) break; offset += 1000; }
      if (missingRows.length === 0) return new Response(JSON.stringify({ success: true, message: 'No missing close prices', index: idx }), { headers: CORS });
      const firstMissing = missingRows[0].date; const lastMissing = missingRows[missingRows.length-1].date;
      const allCloses = new Map<string, number>(); const startYear = parseInt(firstMissing.split('-')[0]); const endYear = parseInt(lastMissing.split('-')[0]);
      for (let y = startYear; y <= endYear; y++) { const yearCloses = await fetchHistoricalClose(indexConfig.nseIndexName, fmtNseDate(new Date(y === startYear ? firstMissing : `${y}-01-01`)), fmtNseDate(new Date(y === endYear ? lastMissing : `${y}-12-31`))); for (const [date, close] of yearCloses) allCloses.set(date, close); }
      if (allCloses.size === 0 && indexConfig.yahooTicker) { const yahooCloses = await fetchYahooClose(indexConfig.yahooTicker, firstMissing, lastMissing); for (const [date, close] of yahooCloses) allCloses.set(date, close); }
      let updated = 0; const batch: any[] = []; for (const row of missingRows) { const close = allCloses.get(row.date); if (close !== undefined && close > 0) batch.push({ index_id: idx, date: row.date, close_price: close }); }
      for (let i = 0; i < batch.length; i += 500) { const { error } = await sb.from('daily_eyby_data').upsert(batch.slice(i, i+500), { onConflict: 'index_id,date', ignoreDuplicates: false }); if (error) throw error; updated += batch.slice(i, i+500).length; }
      return new Response(JSON.stringify({ success: true, index: idx, totalMissing: missingRows.length, updatedRows: updated }), { headers: CORS });
    }
    if (mode === 'update-data') {
      const idx = url.searchParams.get('index') || 'nifty50'; const indexConfig = INDICES[idx]; if (!indexConfig) return new Response(JSON.stringify({error: `Unknown index: ${idx}`}), { headers: CORS }); const sb = getSupabase();
      const fullRefresh = url.searchParams.get('full') === 'true';
      if (indexConfig.source === 'us') {
        try { const result = await refreshUSIndex(sb, idx, indexConfig, fullRefresh); const { count } = await sb.from('daily_eyby_data').select('*', { count: 'exact', head: true }).eq('index_id', idx); await logAudit(sb, idx, result.merged > 0 ? 'ok' : 'no-new-data', result.latestDate, `closes=${result.fetchedCloses} by=${result.fetchedBY} wil=${result.fetchedWilshire} gdp=${result.fetchedGDP} cftc=${result.fetchedCFTC} breadth=${result.fetchedBreadth} merged=${result.merged}`); return new Response(JSON.stringify({ success: true, index: idx, label: indexConfig.label, source: 'us', newLatest: result.latestDate, totalRows: count, fetchedCloses: result.fetchedCloses, fetchedBY: result.fetchedBY, fetchedWilshire: result.fetchedWilshire, fetchedGDP: result.fetchedGDP, fetchedCFTC: result.fetchedCFTC, fetchedBreadth: result.fetchedBreadth, merged: result.merged }), { headers: CORS }); } catch (e) { await logAudit(sb, idx, 'error', null, String(e)); throw e; }
      }
      try {
        const { data: latest } = await sb.from('daily_eyby_data').select('date').eq('index_id', idx).order('date', { ascending: false }).limit(1);
        const lastDate = latest?.[0]?.date || (indexConfig.source === 'csv' ? '2024-10-01' : '2016-04-01');
        const fromDate = new Date(lastDate); fromDate.setDate(fromDate.getDate()-3); const today = new Date(); today.setDate(today.getDate()+1);
        const fromStr = fromDate.toISOString().split('T')[0]; const toStr = today.toISOString().split('T')[0];
        let pepbData: {date: string; pe: number; pb: number}[]; let closeMap = new Map<string, number>();
        if (indexConfig.source === 'csv') { const csvData = await fetchPEPBCloseFromCSV(indexConfig.nseIndexName, fromStr, toStr); pepbData = csvData.map(d => ({ date: d.date, pe: d.pe, pb: d.pb })); for (const d of csvData) { if (d.close > 0) closeMap.set(d.date, d.close); } }
        else { const nseFrom = fmtNseDate(fromDate); const nseTo = fmtNseDate(today); pepbData = await fetchPEPB(indexConfig.nseIndexName, nseFrom, nseTo); closeMap = await fetchCloseFromCSV(indexConfig.nseIndexName.toLowerCase() === 'nifty 50' ? 'Nifty 50' : indexConfig.nseIndexName, fromStr, toStr); }
        const byData = await fetchBY(fromStr, toStr); const byMap = new Map(byData.map((r: any) => [r.date, r.by]));
        const byDates = Array.from(byMap.keys()).sort();
        function getByForDate(date: string): number | undefined { const exact = byMap.get(date); if (exact !== undefined) return exact; let lo = 0, hi = byDates.length-1, best = -1; while (lo <= hi) { const mid = (lo+hi)>>1; if (byDates[mid] <= date) { best = mid; lo = mid+1; } else hi = mid-1; } return best >= 0 ? byMap.get(byDates[best]) : undefined; }
        let dbFallbackBy: number | undefined;
        if (byData.length === 0) { const { data: latestByRow } = await sb.from('daily_eyby_data').select('by_yield').eq('index_id', idx).not('by_yield', 'is', null).order('date', { ascending: false }).limit(1); if (latestByRow?.[0]?.by_yield) dbFallbackBy = latestByRow[0].by_yield; }
        // Phase-2: pull live India Mcap/GDP map and forward-fill across PE dates.
        const mcapMap = await fetchIndiaMcapGDP(fromStr, toStr);
        const mcapDates = Array.from(mcapMap.keys()).sort();
        function getMcapForDate(date: string): number | undefined { const exact = mcapMap.get(date); if (exact !== undefined) return exact; let lo = 0, hi = mcapDates.length-1, best = -1; while (lo <= hi) { const mid = (lo+hi)>>1; if (mcapDates[mid] <= date) { best = mid; lo = mid+1; } else hi = mid-1; } return best >= 0 ? mcapMap.get(mcapDates[best]) : undefined; }
        const merged: any[] = [];
        for (const r of pepbData) { const by = getByForDate(r.date) ?? dbFallbackBy; if (by !== undefined && !isNaN(r.pe) && !isNaN(r.pb) && !isNaN(by)) { const row: any = { index_id: idx, date: r.date, pe: r.pe, pb: r.pb, by_yield: by }; const close = closeMap.get(r.date); if (close !== undefined && close > 0) row.close_price = close; const mc = getMcapForDate(r.date); if (mc != null && mc > 0) row.mcap_gdp = mc; merged.push(row); } }
        if (merged.length > 0) { const { error } = await sb.from('daily_eyby_data').upsert(merged, { onConflict: 'index_id,date' }); if (error) throw error; }
        const { data: newLatest } = await sb.from('daily_eyby_data').select('date').eq('index_id', idx).order('date', { ascending: false }).limit(1);
        const { count } = await sb.from('daily_eyby_data').select('*', { count: 'exact', head: true }).eq('index_id', idx);
        const newLatestDate = newLatest?.[0]?.date || null;
        await logAudit(sb, idx, merged.length > 0 ? 'ok' : 'no-new-data', newLatestDate, `merged=${merged.length} fetchedPE=${pepbData.length} fetchedBY=${byData.length} deploy=${DEPLOY_VERSION}`);
        return new Response(JSON.stringify({ success: true, index: idx, label: indexConfig.label, source: indexConfig.source, newLatest: newLatestDate, totalRows: count, fetchedPE: pepbData.length, fetchedBY: byData.length, fetchedClose: closeMap.size, merged: merged.length, deploy: DEPLOY_VERSION }), { headers: CORS });
      } catch (e) { await logAudit(sb, idx, 'error', null, String(e)); throw e; }
    }
    if (mode === 'update-all') {
      // SEQUENTIAL with 350ms gap between calls — NSE's Backpage.aspx serves cached payloads on
      // rapid back-to-back requests which previously corrupted nifty-auto with nifty-fmcg's PE/PB.
      // Phase-5: refresh array now covers ALL Indian sectors + US + Global indices in one pass.
      const indices = Object.keys(INDICES);
      const out: any[] = [];
      for (const idx of indices) {
        try {
          const r = await fetch(`${url.origin}${url.pathname}?mode=update-data&index=${idx}`);
          out.push(await r.json());
        } catch (e) { out.push({ index: idx, error: String(e) }); }
        await new Promise(res => setTimeout(res, 350));
      }
      return new Response(JSON.stringify(out), { headers: CORS });
    }
    if (mode === 'india-macro') {
      // Phase-5: India GVA + Expenditure proxy bundle — pulled from FRED with graceful nulls.
      // GVA = Agri (low-freq, latest annual) + Industry (INDPROINDMISMEI monthly IIP) + Services (proxy via Nifty Services Sector close as activity ROC).
      // GDP-Expenditure = C (NAEXKP02INQ189S) + I (INDGFCFQDSMEI) + G (n/a, expose null) + (X-M) (n/a).
      const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
      const startISO = '2018-01-01';
      const todayISO = new Date().toISOString().split('T')[0];
      const [iip, hh, gfcf, agri, services, cpi, repo] = await Promise.all([
        fetchFRED('INDPROINDMISMEI', fredKey, startISO),     // Industrial production (Industry / Investment proxy)
        fetchFRED('NAEXKP02INQ189S', fredKey, startISO),     // Real Household Final Consumption (C)
        fetchFRED('INDGFCFQDSMEI', fredKey, startISO),       // Gross Fixed Capital Formation (I)
        fetchFRED('NYGDPMKTPKDZGIND', fredKey, startISO),    // Real GDP growth (Agri proxy via residual)
        fetchYahooDailySeries('%5ECNXSERVICE', startISO, todayISO).then(s => new Map(s.map(p => [p.date, p.close]))),
        fetchFRED('INDCPIALLMINMEI', fredKey, startISO),     // CPI inflation
        fetchFRED('INDIRSTCB01STM', fredKey, startISO),      // Short-term interest rate (RBI proxy)
      ]);
      function latestAndYoY(map: Map<string, number>, lagSteps: number) {
        const dates = Array.from(map.keys()).sort();
        if (dates.length < lagSteps + 1) return { latest: null, yoy: null, asOf: null };
        const latestDate = dates[dates.length - 1];
        const latest = map.get(latestDate) ?? null;
        const lagDate = dates[dates.length - 1 - lagSteps];
        const lag = map.get(lagDate) ?? null;
        const yoy = (latest != null && lag != null && lag !== 0) ? +(((latest / lag) - 1) * 100).toFixed(2) : null;
        return { latest: latest != null ? +latest.toFixed(2) : null, yoy, asOf: latestDate };
      }
      const out = {
        ts: new Date().toISOString(),
        gva: {
          agri:     { ...latestAndYoY(agri, 1),     label: 'Agriculture (Real GDP growth proxy)', source: 'FRED NYGDPMKTPKDZGIND (annual)' },
          industry: { ...latestAndYoY(iip, 12),     label: 'Industry (IIP YoY)',                  source: 'FRED INDPROINDMISMEI (monthly)' },
          services: { ...latestAndYoY(services, 252), label: 'Services (Nifty Services close YoY)', source: 'Yahoo CNXSERVICE (daily)' },
        },
        expenditure: {
          C:  { ...latestAndYoY(hh, 4),   label: 'Private Consumption (HH Final Cons. YoY)', source: 'FRED NAEXKP02INQ189S (quarterly)' },
          I:  { ...latestAndYoY(gfcf, 4), label: 'Gross Fixed Capital Formation YoY',         source: 'FRED INDGFCFQDSMEI (quarterly)' },
          G:  { latest: null, yoy: null, asOf: null, label: 'Government Spending', source: 'Not available on FRED — graceful fallback' },
          NX: { latest: null, yoy: null, asOf: null, label: 'Net Exports',         source: 'Not available on FRED — graceful fallback' },
        },
        monetary: {
          cpi:  { ...latestAndYoY(cpi, 12), label: 'CPI Inflation YoY', source: 'FRED INDCPIALLMINMEI (monthly)' },
          repo: { ...latestAndYoY(repo, 12), label: 'Short-term Rate (RBI proxy)', source: 'FRED INDIRSTCB01STM (monthly)' },
        },
      };
      return new Response(JSON.stringify(out), { headers: { ...CORS, 'Cache-Control': 'public, max-age=21600, s-maxage=21600' } });
    }
    if (mode === 'list-indices') { return new Response(JSON.stringify(INDICES), { headers: CORS }); }
    if (mode === 'weights') {
      // Read top constituents for one index_id from index_weights.
      const idx = url.searchParams.get('index') || url.searchParams.get('index_id') || 'nifty50';
      const limit = Math.min(50, parseInt(url.searchParams.get('limit') || '15') || 15);
      const sb = getSupabase();
      const { data, error } = await sb.from('index_weights').select('symbol, company_name, industry, weight, rank, fetched_at').eq('index_id', idx).order('weight', { ascending: false }).limit(limit);
      if (error) return new Response(JSON.stringify({ error: String(error) }), { status: 500, headers: CORS });
      return new Response(JSON.stringify({ index_id: idx, count: data?.length || 0, rows: data || [] }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=3600, s-maxage=3600' } });
    }
    if (mode === 'weights-refresh') {
      // Bulk refresh weights for all known indices (or one if &index= passed).
      // Designed to run monthly (1st of month) via GitHub Actions, but idempotent if hit daily.
      const sb = getSupabase();
      const single = url.searchParams.get('index');
      const targets = single ? [single] : Object.keys(NSE_WEIGHTS_QUERY);
      const out: any[] = [];
      for (const idx of targets) {
        const q = NSE_WEIGHTS_QUERY[idx];
        if (!q) { out.push({ index_id: idx, error: 'no nse query mapping' }); continue; }
        try {
          const list = await fetchIndexWeightsRaw(q);
          if (list.length === 0) { out.push({ index_id: idx, fetched: 0, note: 'empty (NSE blocked or no data)' }); continue; }
          const now = new Date().toISOString();
          const rows = list.map((c, i) => ({ index_id: idx, symbol: c.symbol, company_name: c.companyName, industry: c.industry, weight: c.weight, rank: i + 1, fetched_at: now }));
          // Wipe stale rows for this index_id then insert fresh — weights shift monthly.
          await sb.from('index_weights').delete().eq('index_id', idx);
          for (let i = 0; i < rows.length; i += 200) {
            const chunk = rows.slice(i, i + 200);
            const { error } = await sb.from('index_weights').upsert(chunk, { onConflict: 'index_id,symbol' });
            if (error) { out.push({ index_id: idx, error: String(error) }); break; }
          }
          out.push({ index_id: idx, fetched: rows.length, top1: rows[0]?.symbol, top1_weight: rows[0]?.weight });
        } catch (e) { out.push({ index_id: idx, error: String(e) }); }
      }
      await logAudit(sb, 'weights-refresh', 'ok', new Date().toISOString().split('T')[0], `targets=${targets.length} ${JSON.stringify(out).slice(0, 200)}`);
      return new Response(JSON.stringify({ ts: new Date().toISOString(), results: out }), { headers: CORS });
    }
    if (mode === 'mcap-gdp-refresh') {
      // Backfill / refresh mcap_gdp on daily_eyby_data for Indian indices over the requested range.
      const sb = getSupabase();
      const fromStr = url.searchParams.get('from') || BASELINE_START_ISO;
      const toStr = url.searchParams.get('to') || new Date().toISOString().split('T')[0];
      const idxParam = url.searchParams.get('index');
      const targets = idxParam ? [idxParam] : Object.keys(INDICES).filter(k => INDICES[k].source !== 'us');
      const mcapMap = await fetchIndiaMcapGDP(fromStr, toStr);
      const mcapDates = Array.from(mcapMap.keys()).sort();
      const getMc = (d: string): number | null => { let lo = 0, hi = mcapDates.length-1, best = -1; while (lo <= hi) { const mid = (lo+hi)>>1; if (mcapDates[mid] <= d) { best = mid; lo = mid+1; } else hi = mid-1; } return best >= 0 ? (mcapMap.get(mcapDates[best]) ?? null) : null; };
      const summary: any[] = [];
      for (const idx of targets) {
        const { data: existing } = await sb.from('daily_eyby_data').select('date').eq('index_id', idx).gte('date', fromStr).lte('date', toStr).order('date', { ascending: true });
        const rows = (existing || []).map(r => ({ index_id: idx, date: r.date, mcap_gdp: getMc(r.date) })).filter(r => r.mcap_gdp != null);
        let updated = 0;
        for (let i = 0; i < rows.length; i += 500) {
          const chunk = rows.slice(i, i + 500);
          const { error } = await sb.from('daily_eyby_data').upsert(chunk, { onConflict: 'index_id,date' });
          if (!error) updated += chunk.length;
        }
        summary.push({ index_id: idx, candidates: existing?.length || 0, updated });
      }
      return new Response(JSON.stringify({ ts: new Date().toISOString(), anchor_ratio_source: INDIA_MCAP_GDP_FRED, proxy: INDIA_MCAP_PROXY_TICKER, summary }), { headers: CORS });
    }
    if (mode === 'macro-history') {
      // Phase-4: Full 10Y daily series for DXY (12M ROC), USD/JPY (12M ROC), US 10Y-2Y spread.
      const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
      const today = new Date();
      const tenYearsAgo = new Date(); tenYearsAgo.setFullYear(tenYearsAgo.getFullYear() - 10);
      const fromIso = tenYearsAgo.toISOString().split('T')[0]; const toIso = today.toISOString().split('T')[0];
      const [dxy, usdjpy, t10y2y] = await Promise.all([
        fetchYahooDailySeries('DX-Y.NYB', fromIso, toIso),
        fetchYahooDailySeries('JPY=X', fromIso, toIso),
        fetchFRED('T10Y2Y', fredKey, fromIso),
      ]);
      // Compute trailing 12M ROC at each daily point — uses nearest trading day from 1y prior.
      function rocSeries(s: { date: string; close: number }[]) {
        const out: { date: string; value: number; roc12m: number | null }[] = [];
        const dates = s.map(p => p.date);
        for (let i = 0; i < s.length; i++) {
          const d = new Date(s[i].date); const ya = new Date(d); ya.setFullYear(ya.getFullYear() - 1);
          const yaIso = ya.toISOString().split('T')[0];
          let lo = 0, hi = i, best = -1;
          while (lo <= hi) { const mid = (lo + hi) >> 1; if (dates[mid] <= yaIso) { best = mid; lo = mid + 1; } else hi = mid - 1; }
          const yaClose = best >= 0 ? s[best].close : null;
          out.push({ date: s[i].date, value: s[i].close, roc12m: yaClose ? +(((s[i].close / yaClose) - 1) * 100).toFixed(2) : null });
        }
        return out;
      }
      const curveArr = Array.from(t10y2y.entries()).sort().map(([date, value]) => ({ date, value }));
      return new Response(JSON.stringify({
        ts: new Date().toISOString(),
        dxy: rocSeries(dxy),
        usdjpy: rocSeries(usdjpy),
        yieldCurve: curveArr,
      }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=3600, s-maxage=3600' } });
    }
    if (mode === 'macro-ribbon') {
      // Phase-3: Sticky macro status bar — DXY 12M ROC, USD/JPY 12M ROC, US 10Y-2Y spread.
      // Output is a compact JSON of {value, roc12m, signal: green|neutral|red} per metric.
      const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
      const today = new Date(); const oneYearAgo = new Date(); oneYearAgo.setFullYear(oneYearAgo.getFullYear()-1);
      const fromIso = oneYearAgo.toISOString().split('T')[0]; const toIso = today.toISOString().split('T')[0];
      const [dxy, usdjpy, t10y2y] = await Promise.all([
        fetchYahooDailySeries('DX-Y.NYB', fromIso, toIso),
        fetchYahooDailySeries('JPY=X', fromIso, toIso),
        fetchFRED('T10Y2Y', fredKey, fromIso),
      ]);
      const rocPct = (s: {date:string;close:number}[]) => s.length > 1 ? +(((s[s.length-1].close / s[0].close) - 1) * 100).toFixed(2) : null;
      const dxyRoc = rocPct(dxy);
      const jpyRoc = rocPct(usdjpy);
      const spreadDates = Array.from(t10y2y.keys()).sort();
      const spreadLatest = spreadDates.length ? (t10y2y.get(spreadDates[spreadDates.length-1]) ?? null) : null;
      // Direction: strong DXY (>+3%) and strong JPY (USD/JPY < -3%) = RED risk-off; weak DXY = GREEN risk-on.
      // Inverted yield curve (negative spread) = RED; steepening (>+0.5) = GREEN.
      const sigDxy = dxyRoc == null ? 'neutral' : dxyRoc > 3 ? 'red' : dxyRoc < -3 ? 'green' : 'neutral';
      const sigJpy = jpyRoc == null ? 'neutral' : jpyRoc < -3 ? 'red' : jpyRoc > 3 ? 'green' : 'neutral';
      const sigCurve = spreadLatest == null ? 'neutral' : spreadLatest < 0 ? 'red' : spreadLatest > 0.5 ? 'green' : 'neutral';
      return new Response(JSON.stringify({
        ts: new Date().toISOString(),
        dxy: { value: dxy.length ? dxy[dxy.length-1].close : null, roc12m: dxyRoc, signal: sigDxy, label: 'DXY 12M ROC', help: 'US Dollar Index. Strong dollar = global risk-off. Inverse signal.' },
        usdjpy: { value: usdjpy.length ? usdjpy[usdjpy.length-1].close : null, roc12m: jpyRoc, signal: sigJpy, label: 'USD/JPY 12M ROC', help: 'Yen weakness (USD/JPY rising) = inflation/risk-off. Inverse signal.' },
        yieldCurve: { spread: spreadLatest, signal: sigCurve, label: 'US 10Y-2Y Spread', help: 'Inverted (<0) = recession risk. Steepening = expansion.' },
      }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=3600, s-maxage=3600' } });
    }
    if (mode === 'gdp-factors') {
      // Phase-3: India GDP Expenditure Method — pull C, I (X-M deferred until FRED publishes), compute YoY growth.
      // FRED India series: NAEXKP02INQ189S = Real HH Final Consumption (quarterly seasonally adj),
      //                    INDGFCFQDSMEI  = Gross Fixed Capital Formation (quarterly).
      const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
      const [c, i] = await Promise.all([
        fetchFRED('NAEXKP02INQ189S', fredKey, '2020-01-01'),
        fetchFRED('INDGFCFQDSMEI', fredKey, '2020-01-01'),
      ]);
      function yoy(map: Map<string, number>) {
        const dates = Array.from(map.keys()).sort();
        if (dates.length < 5) return null;
        const latest = map.get(dates[dates.length - 1]);
        const yearAgo = map.get(dates[dates.length - 5]);
        if (latest == null || !yearAgo) return null;
        return +(((latest / yearAgo) - 1) * 100).toFixed(2);
      }
      const cYoy = yoy(c), iYoy = yoy(i);
      const factors = [
        { factor: 'C',   label: 'Private Consumption',           yoy: cYoy, sectors: ['FMCG', 'Auto'] },
        { factor: 'I',   label: 'Gross Fixed Capital Formation', yoy: iYoy, sectors: ['Infrastructure', 'Capital Markets'] },
        { factor: 'X-M', label: 'Net Exports',                   yoy: null, sectors: ['IT', 'Pharma'] },
      ];
      const valid = factors.filter(f => f.yoy != null);
      valid.sort((a, b) => (b.yoy as number) - (a.yoy as number));
      const leading = valid[0] || null;
      return new Response(JSON.stringify({
        ts: new Date().toISOString(),
        factors,
        leading,
        note: 'Net Exports series not yet on FRED; C and I drive the implied overweight.',
      }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=86400, s-maxage=86400' } });
    }
    if (mode === 'test-us') { const diag: any = { ts: new Date().toISOString(), tests: {} }; const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK; try { const p1 = Math.floor(Date.now()/1000)-86400*10; const p2 = Math.floor(Date.now()/1000)+86400; const yr = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent('^GSPC')}?period1=${p1}&period2=${p2}&interval=1d`, { headers: { 'User-Agent': UA } }); const yText = await yr.text(); diag.tests.yahoo = { status: yr.status, ok: yr.ok, bodyLen: yText.length }; } catch (e) { diag.tests.yahoo = { error: String(e) }; } try { const fr = await fetch(`https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key=${fredKey}&file_type=json&observation_start=2026-04-01`, { headers: { 'User-Agent': UA } }); const fText = await fr.text(); diag.tests.fred = { status: fr.status, ok: fr.ok, bodyLen: fText.length }; } catch (e) { diag.tests.fred = { error: String(e) }; } return new Response(JSON.stringify(diag), { headers: CORS }); }
    if (mode === 'test-by') {
      // Diagnostic: exercise each proxy in the chain for Investing.com pairId 24014 so we can see which one is working.
      const today = new Date(); const from = new Date(); from.setDate(from.getDate()-10);
      const fromStr = from.toISOString().split('T')[0]; const toStr = today.toISOString().split('T')[0];
      const target = `https://api.investing.com/api/financialdata/historical/24014?start-date=${fromStr}&end-date=${toStr}&time-frame=Daily&add-missing-rows=false`;
      const diag: any = { ts: new Date().toISOString(), target, attempts: [] };
      const routes: Array<{name: string; url: string}> = [
        { name: 'direct', url: target },
        { name: 'corsproxy.io', url: `https://corsproxy.io/?url=${encodeURIComponent(target)}` },
        { name: 'allorigins.win', url: `https://api.allorigins.win/raw?url=${encodeURIComponent(target)}` },
      ];
      for (const route of routes) {
        try {
          const r = await fetch(route.url, { headers: INV_HEADERS });
          const text = await r.text();
          let rows = 0; let firstDate: string | null = null; let lastDate: string | null = null;
          try { const j = JSON.parse(text); if (j && Array.isArray(j.data)) { rows = j.data.length; firstDate = j.data[0]?.rowDateTimestamp?.split('T')[0] || null; lastDate = j.data[rows-1]?.rowDateTimestamp?.split('T')[0] || null; } } catch (_e) {}
          diag.attempts.push({ route: route.name, status: r.status, ok: r.ok, bodyLen: text.length, rows, firstDate, lastDate, preview: text.slice(0, 120) });
        } catch (e) { diag.attempts.push({ route: route.name, error: String(e) }); }
      }
      return new Response(JSON.stringify(diag, null, 2), { headers: CORS });
    }
    if (mode === 'pe-fetch') { const from = url.searchParams.get('from') || '01-Apr-2016'; const to = url.searchParams.get('to') || '01-Apr-2026'; const indexName = url.searchParams.get('indexName') || 'NIFTY 50'; const cinfo = JSON.stringify({name: indexName, startDate: from, endDate: to, indexName}); const r = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', { method: 'POST', headers: NI_HEADERS, body: JSON.stringify({ cinfo }) }); const j = await r.json(); return new Response(j.d, { headers: CORS }); }
    if (mode === 'by-fetch-full') {
      // Full 10Y India bond yield dump via proxy chain.
      const rows = await fetchBY('2016-04-01', new Date().toISOString().split('T')[0]);
      return new Response(JSON.stringify(rows.map(r => ({ date: r.date, yield: r.by }))), { headers: CORS });
    }
    return new Response(JSON.stringify({error:'unknown mode'}), { headers: CORS });
  } catch (e) { return new Response(JSON.stringify({ error: String(e) }), { status: 500, headers: CORS }); }
});
