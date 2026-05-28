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
  'Connection': 'keep-alive', 'Accept': 'application/json, text/javascript, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest',
  'User-Agent': UA, 'Content-Type': 'application/json; charset=UTF-8',
  'Origin': 'https://niftyindices.com', 'Referer': 'https://niftyindices.com/reports/historical-data',
};
const INV_HEADERS: Record<string, string> = {
  'User-Agent': UA, 'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9',
  'Referer': 'https://www.investing.com/rates-bonds/india-10-year-bond-yield-historical-data',
  'Origin': 'https://www.investing.com', 'domain-id': 'in',
  'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"', 'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"',
};

const INDICES: Record<string, { nseIndexName: string; label: string; source: 'api' | 'csv' | 'us';
  yahooTicker?: string; fwdEpsEnvVar?: string; bvpsEnvVar?: string; cftcTicker?: string; fmpBreadthExchange?: string;
  bondFredSeries?: string; worldMcapPct?: number; }> = {
  'nifty50':                 { nseIndexName: 'NIFTY 50',                label: 'Nifty 50',                source: 'api', yahooTicker: '%5ENSEI' },
  'nifty-next-50':           { nseIndexName: 'Nifty Next 50',           label: 'Nifty Next 50',           source: 'csv' },
  'nifty-100':               { nseIndexName: 'Nifty 100',               label: 'Nifty 100',               source: 'csv' },
  'nifty-200':               { nseIndexName: 'Nifty 200',               label: 'Nifty 200',               source: 'csv' },
  'nifty-500':               { nseIndexName: 'Nifty 500',               label: 'Nifty 500',               source: 'csv' },
  'nifty-midcap-100':        { nseIndexName: 'Nifty Midcap 100',        label: 'Nifty Midcap 100',        source: 'csv' },
  'nifty-midcap-150':        { nseIndexName: 'Nifty Midcap 150',        label: 'Nifty Midcap 150',        source: 'csv' },
  'nifty-smallcap-100':      { nseIndexName: 'Nifty Smallcap 100',      label: 'Nifty Smallcap 100',      source: 'csv' },
  'nifty-smallcap-250':      { nseIndexName: 'Nifty Smallcap 250',      label: 'Nifty Smallcap 250',      source: 'csv' },
  'nifty-microcap-250':      { nseIndexName: 'NIFTY MICROCAP 250',      label: 'Nifty Microcap 250',      source: 'csv' },
  'nifty-capital-markets':   { nseIndexName: 'Nifty Capital Markets',   label: 'Nifty Capital Markets',   source: 'csv' },
  'nifty-bank':              { nseIndexName: 'Nifty Bank',              label: 'Nifty Bank',              source: 'csv' },
  'nifty-private-bank':      { nseIndexName: 'Nifty Private Bank',      label: 'Nifty Private Bank',      source: 'csv' },
  'nifty-psu-bank':          { nseIndexName: 'Nifty PSU Bank',          label: 'Nifty PSU Bank',          source: 'csv' },
  'nifty-fin-service':       { nseIndexName: 'NIFTY FIN SERVICE',       label: 'Nifty Financial Services', source: 'api', yahooTicker: 'NIFTY_FIN_SERVICE.NS' },
  'nifty-it':                { nseIndexName: 'NIFTY IT',                label: 'Nifty IT',                source: 'api', yahooTicker: '%5ECNXIT' },
  'nifty-auto':              { nseIndexName: 'NIFTY AUTO',              label: 'Nifty Auto',              source: 'api', yahooTicker: '%5ECNXAUTO' },
  'nifty-fmcg':              { nseIndexName: 'NIFTY FMCG',              label: 'Nifty FMCG',              source: 'api', yahooTicker: '%5ECNXFMCG' },
  'nifty-pharma':            { nseIndexName: 'NIFTY PHARMA',            label: 'Nifty Pharma',            source: 'api', yahooTicker: '%5ECNXPHARMA' },
  'nifty-healthcare':        { nseIndexName: 'Nifty Healthcare Index',  label: 'Nifty Healthcare',        source: 'csv' },
  'nifty-infra':             { nseIndexName: 'NIFTY INFRA',             label: 'Nifty Infrastructure',    source: 'api', yahooTicker: '%5ECNXINFRA' },
  'nifty-energy':            { nseIndexName: 'NIFTY ENERGY',            label: 'Nifty Energy',            source: 'api', yahooTicker: '%5ECNXENERGY' },
  'nifty-oil-gas':           { nseIndexName: 'Nifty Oil & Gas',         label: 'Nifty Oil & Gas',         source: 'csv' },
  'nifty-metal':             { nseIndexName: 'Nifty Metal',             label: 'Nifty Metal',             source: 'csv' },
  'nifty-media':             { nseIndexName: 'Nifty Media',             label: 'Nifty Media',             source: 'csv' },
  'nifty-consumer-durables': { nseIndexName: 'Nifty Consumer Durables', label: 'Nifty Consumer Durables', source: 'csv' },
  'nifty-chemicals':         { nseIndexName: 'Nifty Chemicals',         label: 'Nifty Chemicals',         source: 'csv' },
  'nifty-cement':            { nseIndexName: 'Nifty Cement',            label: 'Nifty Cement',             source: 'csv' },
  'nifty-realty':            { nseIndexName: 'NIFTY REALTY',            label: 'Nifty Realty',            source: 'api', yahooTicker: '%5ECNXREALTY' },
  'nifty-commodities':       { nseIndexName: 'NIFTY COMMODITIES',       label: 'Nifty Commodities',       source: 'api', yahooTicker: '%5ECNXCMDT' },
  'nifty-services-sector':   { nseIndexName: 'NIFTY SERV SECTOR',       label: 'Nifty Services Sector',   source: 'api', yahooTicker: '%5ECNXSERVICE' },
  'nifty-consumption':       { nseIndexName: 'Nifty India Consumption', label: 'Nifty India Consumption', source: 'csv' },
  'nifty-mnc':               { nseIndexName: 'Nifty MNC',               label: 'Nifty MNC',               source: 'csv' },
  'nifty-pse':               { nseIndexName: 'Nifty PSE',               label: 'Nifty PSE',               source: 'csv' },
  'nifty-cpse':              { nseIndexName: 'Nifty CPSE',              label: 'Nifty CPSE',              source: 'csv' },
  'sp500':    { nseIndexName: '', label: 'S&P 500',  source: 'us', yahooTicker: '^GSPC',  fwdEpsEnvVar: 'SP500_FWD_EPS_ANCHOR',    bvpsEnvVar: 'SP500_BVPS_ANCHOR',    cftcTicker: 'CFTC/13874A_FO_ALL',  fmpBreadthExchange: 'sp500',  bondFredSeries: 'DGS10',           worldMcapPct: 62.0 },
  'nasdaq':   { nseIndexName: '', label: 'NASDAQ',   source: 'us', yahooTicker: '^NDX',   fwdEpsEnvVar: 'NASDAQ_FWD_EPS_ANCHOR',   bvpsEnvVar: 'NASDAQ_BVPS_ANCHOR',   cftcTicker: 'CFTC/209742_FO_ALL', fmpBreadthExchange: 'nasdaq', bondFredSeries: 'DGS10',           worldMcapPct: 22.0 },
  'kospi200': { nseIndexName: '', label: 'KOSPI 200', source: 'us', yahooTicker: '^KS200', fwdEpsEnvVar: 'KOSPI200_FWD_EPS_ANCHOR', bvpsEnvVar: 'KOSPI200_BVPS_ANCHOR', bondFredSeries: 'IRLTLT01KRM156N', worldMcapPct: 1.7 },
  'bovespa':  { nseIndexName: '', label: 'Bovespa',   source: 'us', yahooTicker: '^BVSP',  fwdEpsEnvVar: 'BOVESPA_FWD_EPS_ANCHOR',  bvpsEnvVar: 'BOVESPA_BVPS_ANCHOR',  bondFredSeries: 'INTGSTBRM193N',   worldMcapPct: 0.5 },
  'twse':     { nseIndexName: '', label: 'TWSE',      source: 'us', yahooTicker: '^TWII',  fwdEpsEnvVar: 'TWSE_FWD_EPS_ANCHOR',     bvpsEnvVar: 'TWSE_BVPS_ANCHOR',     bondFredSeries: 'DGS10',           worldMcapPct: 1.6 },
  'nikkei':   { nseIndexName: '', label: 'Nikkei 225', source: 'us', yahooTicker: '^N225',  fwdEpsEnvVar: 'NIKKEI_FWD_EPS_ANCHOR',   bvpsEnvVar: 'NIKKEI_BVPS_ANCHOR',   bondFredSeries: 'IRLTLT01JPM156N', worldMcapPct: 5.5 },
  'stoxx600': { nseIndexName: '', label: 'STOXX 600', source: 'us', yahooTicker: '^STOXX', fwdEpsEnvVar: 'STOXX600_FWD_EPS_ANCHOR', bvpsEnvVar: 'STOXX600_BVPS_ANCHOR', bondFredSeries: 'IRLTLT01EZM156N', worldMcapPct: 11.0 },
  'hangseng': { nseIndexName: '', label: 'Hang Seng', source: 'us', yahooTicker: '^HSI',   fwdEpsEnvVar: 'HANGSENG_FWD_EPS_ANCHOR', bvpsEnvVar: 'HANGSENG_BVPS_ANCHOR', bondFredSeries: 'DGS10',           worldMcapPct: 3.5 },
};

const FRED_KEY_FALLBACK = 'd6d6deeb62090decbe4f9f2f684b539b';
const FWD_EPS_DEFAULTS: Record<string, number> = { sp500: 285, nasdaq: 1050, kospi200: 37, bovespa: 13900, twse: 1250, nikkei: 2100, stoxx600: 36, hangseng: 1900 };
const BVPS_DEFAULTS: Record<string, number> = { sp500: 1210, nasdaq: 3530, kospi200: 370, bovespa: 73500, twse: 7300, nikkei: 27000, stoxx600: 255, hangseng: 17000 };
const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MONTH_MAP: Record<string,string> = {Jan:'01',Feb:'02',Mar:'03',Apr:'04',May:'05',Jun:'06',Jul:'07',Aug:'08',Sep:'09',Oct:'10',Nov:'11',Dec:'12'};
const BASELINE_START_ISO = '2016-04-01';
const DEPLOY_VERSION = 'v65-phase9.3';
const INDIA_MCAP_GDP_FRED = 'DDDM01INA156NWDB';
const INDIA_MCAP_PROXY_TICKER = '^CRSLDX';
const FRED_URL = (sid: string) => `https://fred.stlouisfed.org/series/${sid}`;

function getSupabase() { return createClient(Deno.env.get('SUPABASE_URL')!, Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!); }

async function fetchWithRetry(url: string, init?: RequestInit, attempts = 3): Promise<Response> {
  let lastErr: any;
  for (let i = 0; i < attempts; i++) {
    try { const resp = await fetch(url, init); if (resp.ok || resp.status === 404) return resp; lastErr = new Error(`HTTP ${resp.status}`); }
    catch (e) { lastErr = e; }
    if (i < attempts - 1) await new Promise(r => setTimeout(r, 500 * Math.pow(2, i)));
  }
  throw lastErr;
}

async function fetchInvestingViaProxies(targetUrl: string): Promise<{ data: any[] } | null> {
  const attempts = [
    { name: 'direct', url: targetUrl },
    { name: 'corsproxy.io', url: `https://corsproxy.io/?url=${encodeURIComponent(targetUrl)}` },
    { name: 'allorigins.win', url: `https://api.allorigins.win/raw?url=${encodeURIComponent(targetUrl)}` },
  ];
  for (const a of attempts) {
    try {
      const r = await fetchWithRetry(a.url, { headers: INV_HEADERS }, 2);
      if (!r.ok) continue;
      const text = await r.text(); if (!text || text.startsWith('<')) continue;
      let parsed: any; try { parsed = JSON.parse(text); } catch { continue; }
      if (parsed && Array.isArray(parsed.data)) return parsed;
    } catch (e) { console.warn(`[inv] ${a.name} error:`, e); }
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
      const resp = await fetchWithRetry(url, { headers: { 'User-Agent': UA } }); if (!resp.ok) continue;
      const text = await resp.text(); if (text.startsWith('<!DOCTYPE') || text.includes('<html')) continue;
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
  const closeMap = new Map<string, number>(); const start = new Date(fromDate); const end = new Date(toDate);
  for (let d = new Date(start); d <= end; d.setDate(d.getDate() + 1)) {
    if (d.getDay() === 0 || d.getDay() === 6) continue;
    const dd = String(d.getDate()).padStart(2,'0'); const mm = String(d.getMonth()+1).padStart(2,'0'); const yyyy = d.getFullYear();
    const isoDate = `${yyyy}-${mm}-${dd}`;
    try {
      const url = `https://www.niftyindices.com/Daily_Snapshot/ind_close_all_${dd}${mm}${yyyy}.csv`;
      const resp = await fetchWithRetry(url, { headers: { 'User-Agent': UA } }); if (!resp.ok) continue;
      const text = await resp.text(); if (text.startsWith('<!DOCTYPE') || text.includes('<html')) continue;
      const lines = text.split('\n').filter(l => l.trim()); if (lines.length < 2) continue;
      const hdr = parseCsvLine(lines[0]); const clI = hdr.findIndex(h => h.toLowerCase().includes('closing index value'));
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

async function nsePrimeSessionCookie(): Promise<string> {
  try {
    const r = await fetch('https://niftyindices.com/reports/historical-data', { headers: { 'User-Agent': UA } });
    const setCookie = r.headers.get('set-cookie') || '';
    return setCookie.split(',').map(s => s.split(';')[0].trim()).filter(Boolean).join('; ');
  } catch { return ''; }
}

async function fetchPEPB(indexName: string, fromDate: string, toDate: string) {
  const nonce = Math.random().toString(36).slice(2, 10);
  const cinfo = JSON.stringify({ name: indexName, startDate: fromDate, endDate: toDate, indexName, _nonce: nonce });
  const cookie = await nsePrimeSessionCookie();
  const headers = cookie ? { ...NI_HEADERS, Cookie: cookie } : NI_HEADERS;
  const r = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', { method: 'POST', headers, body: JSON.stringify({ cinfo }) });
  const j = await r.json(); const raw = JSON.parse(j.d);
  const rows = raw.map((r: any) => ({ date: parseNseDate(r.DATE), pe: parseFloat(r.pe), pb: parseFloat(r.pb) }));
  if (rows.length > 50) {
    const pes = rows.map((x: any) => x.pe).filter((v: number) => Number.isFinite(v));
    const lo = Math.min(...pes), hi = Math.max(...pes);
    if (hi - lo < 0.001) {
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

async function fetchIndiaMcapGDP(fromIso: string, toIso: string): Promise<Map<string, number>> {
  const out = new Map<string, number>();
  const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
  const annual = await fetchFRED(INDIA_MCAP_GDP_FRED, fredKey, '2010-01-01');
  const annualDates = Array.from(annual.keys()).sort();
  if (annualDates.length === 0) return out;
  const anchorDate = annualDates[annualDates.length - 1];
  const anchorRatio = annual.get(anchorDate)!;
  const series = await fetchYahooDailySeries(INDIA_MCAP_PROXY_TICKER, anchorDate, toIso);
  if (series.length === 0) return out;
  const anchorRow = series.find(s => s.date >= anchorDate) || series[0];
  if (!anchorRow || !anchorRow.close) return out;
  for (const r of series) { if (r.date < fromIso) continue; out.set(r.date, +(anchorRatio * (r.close / anchorRow.close)).toFixed(2)); }
  return out;
}

async function fetchBY(fromDate: string, toDate: string) {
  const allData: { date: string; by: number }[] = [];
  const startYear = parseInt(fromDate.split('-')[0]); const endYear = parseInt(toDate.split('-')[0]);
  for (let y = startYear; y <= endYear; y++) {
    const from = y === startYear ? fromDate : `${y}-01-01`; const to = y === endYear ? toDate : `${y}-12-31`;
    const target = `https://api.investing.com/api/financialdata/historical/24014?start-date=${from}&end-date=${to}&time-frame=Daily&add-missing-rows=false`;
    const j = await fetchInvestingViaProxies(target);
    if (j && Array.isArray(j.data)) { for (const d of j.data) { const date = (d.rowDateTimestamp || '').split('T')[0]; const raw = d.last_close ?? d.last_closeRaw; const val = parseFloat(String(raw).replace(/,/g, '')); if (date && !isNaN(val)) allData.push({ date, by: val }); } }
  }
  const byDate = new Map<string, number>(); for (const r of allData) byDate.set(r.date, r.by);
  const out = Array.from(byDate.entries()).map(([date, by]) => ({ date, by })); out.sort((a, b) => a.date.localeCompare(b.date));
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

function forwardFill(dates: string[], sparseMap: Map<string, number>): Map<string, number> { const filled = new Map<string, number>(); let lastVal: number | undefined; for (const d of dates) { const v = sparseMap.get(d); if (v !== undefined) lastVal = v; if (lastVal !== undefined) filled.set(d, lastVal); } return filled; }
function computeRSI14(closes: number[]): (number | null)[] { const period = 14; const rsi: (number | null)[] = new Array(closes.length).fill(null); if (closes.length < period + 1) return rsi; let avgGain = 0, avgLoss = 0; for (let i = 1; i <= period; i++) { const ch = closes[i] - closes[i-1]; if (ch > 0) avgGain += ch; else avgLoss -= ch; } avgGain /= period; avgLoss /= period; rsi[period] = avgLoss === 0 ? 100 : +(100 - 100/(1+avgGain/avgLoss)).toFixed(2); for (let i = period+1; i < closes.length; i++) { const ch = closes[i] - closes[i-1]; avgGain = (avgGain*(period-1) + (ch > 0 ? ch : 0))/period; avgLoss = (avgLoss*(period-1) + (ch < 0 ? -ch : 0))/period; rsi[i] = avgLoss === 0 ? 100 : +(100 - 100/(1+avgGain/avgLoss)).toFixed(2); } return rsi; }

async function fetchCFTCCOT(cftcTicker: string, startDate: string): Promise<Map<string, number>> { const result = new Map<string, number>(); const apiKey = Deno.env.get('NASDAQ_DATA_LINK_KEY') || ''; if (!apiKey || !cftcTicker) return result; try { const r = await fetchWithRetry(`https://data.nasdaq.com/api/v3/datasets/${cftcTicker}.json?start_date=${startDate}&api_key=${apiKey}&order=asc`, { headers: { 'User-Agent': UA } }); if (!r.ok) return result; const j = await r.json(); const cols: string[] = (j.dataset?.column_names || []).map((c: string) => c.toLowerCase()); const rows: any[][] = j.dataset?.data || []; const ncLongI = cols.findIndex(c => c.includes('noncommercial') && c.includes('long') && !c.includes('spread')); const ncShortI = cols.findIndex(c => c.includes('noncommercial') && c.includes('short') && !c.includes('spread')); const oiI = cols.findIndex(c => c.includes('open interest')); const longI = ncLongI >= 0 ? ncLongI : cols.findIndex(c => c.includes('long') && !c.includes('spread') && !c.includes('change')); const shortI = ncShortI >= 0 ? ncShortI : cols.findIndex(c => c.includes('short') && !c.includes('spread') && !c.includes('change')); if (longI < 0 || shortI < 0) return result; for (const row of rows) { const date = row[0]; const ncLong = parseFloat(row[longI]); const ncShort = parseFloat(row[shortI]); if (isNaN(ncLong) || isNaN(ncShort)) continue; const net = ncLong - ncShort; if (oiI >= 0) { const oi = parseFloat(row[oiI]); if (!isNaN(oi) && oi > 0) result.set(date, +((net/oi)*100).toFixed(2)); } else result.set(date, +net.toFixed(0)); } } catch (e) { console.error(`CFTC fetch error:`, e); } return result; }

async function fetchFMPBreadth(exchange: string, startDate: string): Promise<Map<string, number>> { const result = new Map<string, number>(); const apiKey = Deno.env.get('FMP_API_KEY') || ''; if (!apiKey || !exchange) return result; try { const r = await fetchWithRetry(`https://financialmodelingprep.com/stable/market-breadth?type=${exchange}&apikey=${apiKey}`, { headers: { 'User-Agent': UA } }); if (!r.ok) return result; const j = await r.json(); if (Array.isArray(j)) { for (const row of j) { const date = row.date; const pct = row.averageAbove200DMA ?? row.percentAbove200DMA ?? row.breadth ?? null; if (date && pct != null && !isNaN(pct)) { const val = pct > 1 ? pct : pct * 100; result.set(date, +val.toFixed(2)); } } } } catch (e) { console.error(`FMP breadth error:`, e); } return result; }

async function refreshUSIndex(sb: any, idx: string, indexConfig: typeof INDICES[string], fullRefresh = false) {
  const { data: latest } = await sb.from('daily_eyby_data').select('date').eq('index_id', idx).order('date', { ascending: false }).limit(1);
  const lastDate = latest?.[0]?.date || BASELINE_START_ISO;
  let fromStr: string; if (fullRefresh) { fromStr = BASELINE_START_ISO; } else { const fd = new Date(lastDate); fd.setDate(fd.getDate()-7); fromStr = fd.toISOString().split('T')[0]; }
  const today = new Date(); today.setDate(today.getDate()+1); const toStr = today.toISOString().split('T')[0];
  const ticker = indexConfig.yahooTicker || '';
  const closeSeries = await fetchYahooDailySeries(ticker, fromStr, toStr);
  if (closeSeries.length === 0) return { merged: 0, fetchedCloses: 0, fetchedBY: 0, latestDate: lastDate };
  const tradingDates = closeSeries.map(c => c.date);
  const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
  const bondSeries = indexConfig.bondFredSeries || 'DGS10';
  const [rawBY, rawGDP, wilshireSeries, rawCFTC, rawBreadth] = await Promise.all([ fetchFRED(bondSeries, fredKey, fromStr), fetchFRED('GDP', fredKey, BASELINE_START_ISO), fetchYahooDailySeries('^W5000', fromStr, toStr), fetchCFTCCOT(indexConfig.cftcTicker || '', fromStr), fetchFMPBreadth(indexConfig.fmpBreadthExchange || '', fromStr) ]);
  const rawWilshire = new Map<string, number>(); for (const w of wilshireSeries) rawWilshire.set(w.date, w.close);
  const byFilled = forwardFill(tradingDates, rawBY); const wilshireFilled = forwardFill(tradingDates, rawWilshire);
  const gdpFilled = forwardFill(tradingDates, rawGDP); const cftcFilled = forwardFill(tradingDates, rawCFTC); const breadthFilled = forwardFill(tradingDates, rawBreadth);
  if (byFilled.size === 0) { const { data: latestByRow } = await sb.from('daily_eyby_data').select('by_yield').eq('index_id', idx).not('by_yield', 'is', null).order('date', { ascending: false }).limit(1); if (latestByRow?.[0]?.by_yield) { const fb = latestByRow[0].by_yield; for (const d of tradingDates) byFilled.set(d, fb); } }
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
  return { merged, fetchedCloses: closeSeries.length, fetchedBY: rawBY.size, latestDate: rows.length > 0 ? rows[rows.length-1].date : lastDate };
}

function buildMacroCell(map: Map<string, number>, lagSteps: number, label: string, source: string, sourceUrl: string | null, opts: { yoyKind?: 'pct' | 'diff' | 'level'; provenance?: 'primary' | 'fallback'; seriesLen?: number } = {}) {
  const yoyKind = opts.yoyKind || 'pct';
  const provenance = opts.provenance || 'primary';
  if (!map || map.size === 0) return { value: null, date: null, yoy: null, status: 'unavailable', label, source, sourceUrl, provenance, series: [] };
  const dates = Array.from(map.keys()).sort();
  const latestDate = dates[dates.length - 1]; const latest = map.get(latestDate) ?? null;
  let yoy: number | null = null;
  if (yoyKind === 'level') yoy = null;
  else if (dates.length >= lagSteps + 1 && lagSteps > 0) {
    const lagDate = dates[dates.length - 1 - lagSteps];
    const lag = map.get(lagDate) ?? null;
    if (latest != null && lag != null) {
      if (yoyKind === 'pct' && lag !== 0) yoy = +(((latest / lag) - 1) * 100).toFixed(2);
      if (yoyKind === 'diff') yoy = +(latest - lag).toFixed(2);
    }
  }
  // Phase 9.2 — Attach trailing series for inline Cell sparklines. Default 120 most-recent
  // observations (10y monthly, ~5mo daily). Frontend slices by period selector.
  const N = opts.seriesLen ?? 120;
  const start = Math.max(0, dates.length - N);
  const series: { date: string; value: number }[] = [];
  for (let i = start; i < dates.length; i++) { const v = map.get(dates[i]); if (v != null) series.push({ date: dates[i], value: +v.toFixed(4) }); }
  return { value: latest != null ? +latest.toFixed(2) : null, date: latestDate, yoy, status: 'ok', label, source, sourceUrl, provenance, series };
}

// Phase 9.2 — Cascading fallback fetcher.
// Tries primaryId first; if empty (FRED ID retired/delayed), falls back to fallbackId.
// Optional yahooTicker fallback supplies a daily price-based proxy when both FRED IDs fail.
// Returns { map, source, sourceUrl, sourceLabel, provenance } so callers can flag the cell.
async function fetchWithFallback(opts: {
  primaryId: string;
  primaryLabel: string;
  fallbackId?: string;
  fallbackLabel?: string;
  yahooTicker?: string;
  yahooLabel?: string;
  fredKey: string;
  startISO: string;
}): Promise<{ map: Map<string, number>; provenance: 'primary' | 'fallback'; source: string; sourceUrl: string | null }> {
  const { primaryId, primaryLabel, fallbackId, fallbackLabel, yahooTicker, yahooLabel, fredKey, startISO } = opts;
  // 1. Primary FRED
  const primary = await fetchFRED(primaryId, fredKey, startISO);
  if (primary.size > 0) return { map: primary, provenance: 'primary', source: primaryLabel, sourceUrl: FRED_URL(primaryId) };
  // 2. Fallback FRED
  if (fallbackId) {
    const fallback = await fetchFRED(fallbackId, fredKey, startISO);
    if (fallback.size > 0) return { map: fallback, provenance: 'fallback', source: fallbackLabel || `FRED ${fallbackId} (fallback)`, sourceUrl: FRED_URL(fallbackId) };
  }
  // 3. Yahoo proxy
  if (yahooTicker) {
    const series = await fetchYahooDailySeries(yahooTicker, startISO, new Date().toISOString().split('T')[0]);
    if (series.length > 0) {
      const m = new Map<string, number>(); for (const p of series) m.set(p.date, p.close);
      return { map: m, provenance: 'fallback', source: yahooLabel || `Yahoo ${yahooTicker} (proxy)`, sourceUrl: `https://finance.yahoo.com/quote/${encodeURIComponent(yahooTicker)}` };
    }
  }
  return { map: new Map(), provenance: 'primary', source: primaryLabel, sourceUrl: FRED_URL(primaryId) };
}

function buildYieldSpreadSeries(map10: Map<string, number>, map3m: Map<string, number>): { date: string; value: number }[] {
  if (!map10 || !map3m || map10.size === 0 || map3m.size === 0) return [];
  const dates = new Set<string>([...map10.keys(), ...map3m.keys()]);
  const sorted = Array.from(dates).sort();
  const f10 = forwardFill(sorted, map10); const f3m = forwardFill(sorted, map3m);
  const out: { date: string; value: number }[] = [];
  for (const d of sorted) { const a = f10.get(d); const b = f3m.get(d); if (a != null && b != null && isFinite(a) && isFinite(b)) out.push({ date: d, value: +(a - b).toFixed(4) }); }
  return out;
}

function unavailableCell(label: string, source: string, sourceUrl: string | null) {
  return { value: null, date: null, yoy: null, status: 'unavailable', label, source, sourceUrl };
}

// Phase 9.2 — Fetch monthly-anchor close series for a set of sector index_ids over the last N years.
// Returns { [index_id]: [{date, close}, ...] } — one observation per month, last trading-day close.
// Used by the dynamic backtest engine to compute live N-year CAGR of the active regime's overweight basket.
async function fetchSectorMonthlyCloses(sb: any, indexIds: string[], years = 10): Promise<Record<string, { date: string; close: number }[]>> {
  const out: Record<string, { date: string; close: number }[]> = {};
  const fromDate = new Date(); fromDate.setFullYear(fromDate.getFullYear() - years);
  const fromISO = fromDate.toISOString().split('T')[0];
  for (const idx of indexIds) {
    try {
      const all: { date: string; close: number }[] = [];
      let from = 0;
      while (true) {
        const { data, error } = await sb.from('daily_eyby_data').select('date, close_price').eq('index_id', idx).gte('date', fromISO).not('close_price', 'is', null).order('date', { ascending: true }).range(from, from + 999);
        if (error || !data || data.length === 0) break;
        for (const r of data) all.push({ date: r.date, close: +r.close_price });
        if (data.length < 1000) break; from += 1000;
      }
      // Down-sample to monthly: keep the last observation per YYYY-MM
      const byMonth = new Map<string, { date: string; close: number }>();
      for (const p of all) { const ym = p.date.slice(0, 7); byMonth.set(ym, p); }
      out[idx] = Array.from(byMonth.values()).sort((a, b) => a.date.localeCompare(b.date));
    } catch (_e) { out[idx] = []; }
  }
  return out;
}

// Phase 9.1 — Derive System ROE from latest Nifty 50 daily_eyby_data (PE & PB).
// ROE ≈ (1/PE) / (1/PB) × 100 = (PB / PE) × 100. This proxies the aggregate ROE for the broad Indian equity market.
// Phase 9.3 — Monthly ROE − 10Y G-Sec spread series derived from daily_eyby_data (nifty50).
// ROE proxy = (PB / PE) × 100. Spread = ROE − by_yield. Last observation of each calendar month.
async function fetchRoeGsecSpreadSeries(sb: any, years = 10): Promise<{ date: string; value: number }[]> {
  const out: { date: string; value: number }[] = [];
  try {
    const fromDate = new Date(); fromDate.setFullYear(fromDate.getFullYear() - years);
    const fromISO = fromDate.toISOString().split('T')[0];
    const all: { date: string; pe: number; pb: number; by: number }[] = [];
    let from = 0;
    while (true) {
      const { data, error } = await sb.from('daily_eyby_data').select('date, pe, pb, by_yield').eq('index_id', 'nifty50').gte('date', fromISO).not('pe', 'is', null).not('pb', 'is', null).not('by_yield', 'is', null).order('date', { ascending: true }).range(from, from + 999);
      if (error || !data || data.length === 0) break;
      for (const r of data) { const pe = +r.pe, pb = +r.pb, by = +r.by_yield; if (pe > 0 && pb > 0 && by > 0) all.push({ date: r.date, pe, pb, by }); }
      if (data.length < 1000) break; from += 1000;
    }
    const byMonth = new Map<string, { date: string; value: number }>();
    for (const p of all) { const roe = (p.pb / p.pe) * 100; const spread = roe - p.by; const ym = p.date.slice(0, 7); byMonth.set(ym, { date: p.date, value: +spread.toFixed(2) }); }
    return Array.from(byMonth.values()).sort((a, b) => a.date.localeCompare(b.date));
  } catch (_e) { return out; }
}

async function buildSystemROE(sb: any): Promise<{ roe: number | null; pb: number | null; pe: number | null; date: string | null }> {
  try {
    const { data } = await sb.from('daily_eyby_data').select('pe, pb, date').eq('index_id', 'nifty50').not('pe', 'is', null).not('pb', 'is', null).order('date', { ascending: false }).limit(1);
    const row = data?.[0]; if (!row || !row.pe || !row.pb || row.pe <= 0) return { roe: null, pb: null, pe: null, date: null };
    return { roe: +((row.pb / row.pe) * 100).toFixed(2), pb: +(+row.pb).toFixed(2), pe: +(+row.pe).toFixed(2), date: row.date };
  } catch { return { roe: null, pb: null, pe: null, date: null }; }
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
        const enriched = allRows.map((r: any, i: number) => { const close = r.close_price || 0; const gsec10 = r.by_yield || 0; const pe = r.pe || 0; const fwd_ey = fwdEps > 0 && close > 0 ? +((fwdEps/close)*100).toFixed(4) : 0; const earningsYield = pe > 0 ? +(100/pe).toFixed(4) : 0; const eyby = gsec10 > 0 ? +(earningsYield/gsec10).toFixed(4) : 0; const pb = r.pb || 0; const roe = (pb > 0 && pe > 0) ? +((pb/pe)*100).toFixed(2) : null; const rawCftc = r.cftc_net_pct; const fiiNetLong = rawCftc != null ? Math.max(0, Math.min(100, 50 + rawCftc * 1.5)) : null; return { date: r.date, close, pe, pb, gsec10, fwd_ey, erp: +(fwd_ey - gsec10).toFixed(4), eyby, rsi14: rsiArr[i], mcap_gdp: r.mcap_gdp || null, roe, fii_fut_net_long: fiiNetLong != null ? +fiiNetLong.toFixed(2) : null, breadth: r.breadth_pct != null ? +r.breadth_pct : null }; });
        return new Response(JSON.stringify(enriched), { headers: { ...CORS, 'Cache-Control': 'public, max-age=60, s-maxage=60' } });
      }
      const compact = allRows.map((r: any) => { const roe = (r.pb > 0 && r.pe > 0) ? +((r.pb/r.pe)*100).toFixed(2) : null; return [r.date, r.pe, r.pb, r.by_yield, r.close_price, roe]; });
      return new Response(JSON.stringify(compact), { headers: { ...CORS, 'Cache-Control': 'public, max-age=60, s-maxage=60' } });
    }
    if (mode === 'env-check') { const envKeys = ['SUPABASE_URL','SUPABASE_SERVICE_ROLE_KEY','FRED_API_KEY','FMP_API_KEY','NASDAQ_DATA_LINK_KEY']; const report: Record<string, string> = {}; for (const k of envKeys) { const v = Deno.env.get(k); report[k] = v ? `SET (${v.length} chars)` : 'NOT SET'; } report['deploy_version'] = DEPLOY_VERSION; report['indices_count'] = String(Object.keys(INDICES).length); return new Response(JSON.stringify({ ts: new Date().toISOString(), env: report }), { headers: CORS }); }
    if (mode === 'health') { const sb = getSupabase(); const out: any = { ts: new Date().toISOString(), deploy_version: DEPLOY_VERSION, indices: {} }; for (const idx of Object.keys(INDICES)) { const { data } = await sb.from('daily_eyby_data').select('date, close_price, pe, pb').eq('index_id', idx).order('date', { ascending: false }).limit(1); const row = data?.[0]; out.indices[idx] = { label: INDICES[idx].label, latestDate: row?.date || null, latestPE: row?.pe, latestPB: row?.pb }; } return new Response(JSON.stringify(out), { headers: CORS }); }
    if (mode === 'list-indices') { return new Response(JSON.stringify(INDICES), { headers: CORS }); }
    if (mode === 'update-data') {
      const idx = url.searchParams.get('index') || 'nifty50'; const indexConfig = INDICES[idx]; if (!indexConfig) return new Response(JSON.stringify({error: `Unknown index: ${idx}`}), { headers: CORS }); const sb = getSupabase();
      const fullRefresh = url.searchParams.get('full') === 'true';
      if (indexConfig.source === 'us') { try { const result = await refreshUSIndex(sb, idx, indexConfig, fullRefresh); const { count } = await sb.from('daily_eyby_data').select('*', { count: 'exact', head: true }).eq('index_id', idx); await logAudit(sb, idx, result.merged > 0 ? 'ok' : 'no-new-data', result.latestDate, `closes=${result.fetchedCloses} by=${result.fetchedBY} merged=${result.merged}`); return new Response(JSON.stringify({ success: true, index: idx, label: indexConfig.label, source: 'us', newLatest: result.latestDate, totalRows: count, fetchedCloses: result.fetchedCloses, fetchedBY: result.fetchedBY, merged: result.merged }), { headers: CORS }); } catch (e) { await logAudit(sb, idx, 'error', null, String(e)); throw e; } }
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
        const mcapMap = await fetchIndiaMcapGDP(fromStr, toStr);
        const mcapDates = Array.from(mcapMap.keys()).sort();
        function getMcapForDate(date: string): number | undefined { const exact = mcapMap.get(date); if (exact !== undefined) return exact; let lo = 0, hi = mcapDates.length-1, best = -1; while (lo <= hi) { const mid = (lo+hi)>>1; if (mcapDates[mid] <= date) { best = mid; lo = mid+1; } else hi = mid-1; } return best >= 0 ? mcapMap.get(mcapDates[best]) : undefined; }
        const merged: any[] = [];
        for (const r of pepbData) { const by = getByForDate(r.date) ?? dbFallbackBy; if (by !== undefined && !isNaN(r.pe) && !isNaN(r.pb) && !isNaN(by)) { const row: any = { index_id: idx, date: r.date, pe: r.pe, pb: r.pb, by_yield: by }; const close = closeMap.get(r.date); if (close !== undefined && close > 0) row.close_price = close; const mc = getMcapForDate(r.date); if (mc != null && mc > 0) row.mcap_gdp = mc; merged.push(row); } }
        if (merged.length > 0) { const { error } = await sb.from('daily_eyby_data').upsert(merged, { onConflict: 'index_id,date' }); if (error) throw error; }
        const { count } = await sb.from('daily_eyby_data').select('*', { count: 'exact', head: true }).eq('index_id', idx);
        await logAudit(sb, idx, merged.length > 0 ? 'ok' : 'no-new-data', null, `merged=${merged.length} fetchedPE=${pepbData.length} deploy=${DEPLOY_VERSION}`);
        return new Response(JSON.stringify({ success: true, index: idx, label: indexConfig.label, source: indexConfig.source, totalRows: count, fetchedPE: pepbData.length, fetchedBY: byData.length, merged: merged.length, deploy: DEPLOY_VERSION }), { headers: CORS });
      } catch (e) { await logAudit(sb, idx, 'error', null, String(e)); throw e; }
    }
    if (mode === 'update-all') {
      const indices = Object.keys(INDICES); const out: any[] = [];
      for (const idx of indices) {
        try { const r = await fetch(`${url.origin}${url.pathname}?mode=update-data&index=${idx}`); out.push(await r.json()); }
        catch (e) { out.push({ index: idx, error: String(e) }); }
        await new Promise(res => setTimeout(res, 350));
      }
      return new Response(JSON.stringify(out), { headers: CORS });
    }
    if (mode === 'pe-fetch') { const from = url.searchParams.get('from') || '01-Apr-2016'; const to = url.searchParams.get('to') || '01-Apr-2026'; const indexName = url.searchParams.get('indexName') || 'NIFTY 50'; const cinfo = JSON.stringify({name: indexName, startDate: from, endDate: to, indexName}); const r = await fetchWithRetry('https://niftyindices.com/Backpage.aspx/getpepbHistoricaldataDBtoString', { method: 'POST', headers: NI_HEADERS, body: JSON.stringify({ cinfo }) }); const j = await r.json(); return new Response(j.d, { headers: CORS }); }
    if (mode === 'weights') { const idx = url.searchParams.get('index') || 'nifty50'; const limit = Math.min(50, parseInt(url.searchParams.get('limit') || '15') || 15); const sb = getSupabase(); const { data, error } = await sb.from('index_weights').select('symbol, company_name, industry, weight, rank, fetched_at').eq('index_id', idx).order('weight', { ascending: false }).limit(limit); if (error) return new Response(JSON.stringify({ error: String(error) }), { status: 500, headers: CORS }); return new Response(JSON.stringify({ index_id: idx, count: data?.length || 0, rows: data || [] }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=3600, s-maxage=3600' } }); }
    // Phase 9.3 — Stocks module endpoints. When stocks_master / stocks_daily are empty (backfill pending),
    // these return well-formed empty payloads instead of "unknown mode" so the frontend renders its fallback UI
    // rather than white-screening.
    if (mode === 'stocks-universe') {
      const sb = getSupabase();
      try {
        const { data: master } = await sb.from('stocks_master').select('ticker, name, yahoo_symbol, sector, exchange').eq('is_active', true).order('name', { ascending: true });
        if (!master || master.length === 0) {
          return new Response(JSON.stringify({ tree: {}, indexOrder: [], total: 0, message: 'stocks_master table is empty — universe awaits backfill' }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' } });
        }
        // Group by sector. Future enhancement: join against index_weights for proper per-index baskets.
        const tree: Record<string, any[]> = {};
        for (const row of master) { const sec = row.sector || 'Other'; if (!tree[sec]) tree[sec] = []; tree[sec].push({ ticker: row.ticker, name: row.name, yahoo_symbol: row.yahoo_symbol, exchange: row.exchange }); }
        return new Response(JSON.stringify({ tree, indexOrder: Object.keys(tree).sort(), total: master.length }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=600, s-maxage=600' } });
      } catch (e) { return new Response(JSON.stringify({ tree: {}, error: String(e) }), { status: 200, headers: CORS }); }
    }
    if (mode === 'stocks-list') {
      const sb = getSupabase();
      try {
        const { data, error } = await sb.from('stocks_master').select('ticker, name, yahoo_symbol, exchange, sector, listing_date, is_active').eq('is_active', true).order('name', { ascending: true });
        if (error) throw error;
        return new Response(JSON.stringify(data || []), { headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' } });
      } catch (e) { return new Response(JSON.stringify([]), { status: 200, headers: CORS }); }
    }
    if (mode === 'stocks-data') {
      const ticker = url.searchParams.get('ticker');
      if (!ticker) return new Response(JSON.stringify({ ticker: '', rows: [], error: 'ticker required' }), { headers: CORS });
      const sb = getSupabase();
      try {
        const all: any[] = []; let from = 0;
        while (true) {
          const { data, error } = await sb.from('stocks_daily').select('date, close, pe, pb, eps_ttm, bvps, earnings_yield').eq('ticker', ticker).order('date', { ascending: true }).range(from, from + 999);
          if (error || !data || data.length === 0) break;
          all.push(...data); if (data.length < 1000) break; from += 1000;
        }
        return new Response(JSON.stringify({ ticker, rows: all.map(r => [r.date, r.close, r.pe, r.pb, r.eps_ttm, r.bvps, r.earnings_yield]) }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=300, s-maxage=300' } });
      } catch (e) { return new Response(JSON.stringify({ ticker, rows: [], error: String(e) }), { status: 200, headers: CORS }); }
    }
    if (mode === 'stocks-quote') {
      const ticker = url.searchParams.get('ticker');
      if (!ticker) return new Response(JSON.stringify({ ticker: '', fundamentals: null, error: 'ticker required' }), { headers: CORS });
      // Yahoo quoteSummary fetcher with graceful failure — UI degrades to N/A rather than crashing on null.
      try {
        const sb = getSupabase();
        const { data: master } = await sb.from('stocks_master').select('yahoo_symbol').eq('ticker', ticker).limit(1);
        const ySym = master?.[0]?.yahoo_symbol || ticker;
        const r = await fetchWithRetry(`https://query1.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(ySym)}?modules=defaultKeyStatistics,financialData,summaryDetail`, { headers: { 'User-Agent': UA } });
        if (!r.ok) return new Response(JSON.stringify({ ticker, fundamentals: null, error: 'Yahoo HTTP ' + r.status }), { headers: CORS });
        const j = await r.json();
        const res = j?.quoteSummary?.result?.[0];
        if (!res) return new Response(JSON.stringify({ ticker, fundamentals: null, error: 'Yahoo empty response' }), { headers: CORS });
        const fd = res.financialData || {}; const ks = res.defaultKeyStatistics || {}; const sd = res.summaryDetail || {};
        const fundamentals = {
          roe:        fd.returnOnEquity?.raw != null ? +(fd.returnOnEquity.raw * 100).toFixed(2) : null,
          pb:         ks.priceToBook?.raw ?? null,
          trailingPE: sd.trailingPE?.raw ?? ks.trailingPE?.raw ?? null,
          forwardPE:  sd.forwardPE?.raw ?? null,
          eps:        ks.trailingEps?.raw ?? null,
          marketCap:  sd.marketCap?.raw ?? null,
        };
        return new Response(JSON.stringify({ ticker, fundamentals }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=600, s-maxage=600' } });
      } catch (e) { return new Response(JSON.stringify({ ticker, fundamentals: null, error: String(e) }), { status: 200, headers: CORS }); }
    }
    if (mode === 'macro-history') {
      const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
      const today = new Date(); const tenYearsAgo = new Date(); tenYearsAgo.setFullYear(tenYearsAgo.getFullYear() - 10);
      const fromIso = tenYearsAgo.toISOString().split('T')[0]; const toIso = today.toISOString().split('T')[0];
      const [dxy, usdjpy, t10y2y] = await Promise.all([ fetchYahooDailySeries('DX-Y.NYB', fromIso, toIso), fetchYahooDailySeries('JPY=X', fromIso, toIso), fetchFRED('T10Y2Y', fredKey, fromIso) ]);
      function rocSeries(s: { date: string; close: number }[]) { const out: { date: string; value: number; roc12m: number | null }[] = []; const dates = s.map(p => p.date); for (let i = 0; i < s.length; i++) { const d = new Date(s[i].date); const ya = new Date(d); ya.setFullYear(ya.getFullYear() - 1); const yaIso = ya.toISOString().split('T')[0]; let lo = 0, hi = i, best = -1; while (lo <= hi) { const mid = (lo + hi) >> 1; if (dates[mid] <= yaIso) { best = mid; lo = mid + 1; } else hi = mid - 1; } const yaClose = best >= 0 ? s[best].close : null; out.push({ date: s[i].date, value: s[i].close, roc12m: yaClose ? +(((s[i].close / yaClose) - 1) * 100).toFixed(2) : null }); } return out; }
      const curveArr = Array.from(t10y2y.entries()).sort().map(([date, value]) => ({ date, value }));
      return new Response(JSON.stringify({ ts: new Date().toISOString(), dxy: rocSeries(dxy), usdjpy: rocSeries(usdjpy), yieldCurve: curveArr }), { headers: { ...CORS, 'Cache-Control': 'public, max-age=3600, s-maxage=3600' } });
    }
    if (mode === 'india-macro') {
      // Phase 9.1 — Adds businessCycle (Capacity Util, Net D/E, System ROE, ROE-GSec spread),
      // highFreq (Auto sales PV/2W/CV/Tractors, Fuel cons.), freight (Port cargo, Railway freight, E-way bills),
      // and capitalFlows (FII Equity, FII Debt, DII/MF). Series w/o public free feeds gracefully return status='unavailable'.
      const fredKey = Deno.env.get('FRED_API_KEY') || FRED_KEY_FALLBACK;
      const sb = getSupabase();
      const startISO = '2018-01-01'; const longStartISO = '2010-01-01';
      const todayISO = new Date().toISOString().split('T')[0];
      // Phase 9.2 — Sector basket index_ids for dynamic backtest. Covers Banks/Fin/Auto/FMCG/IT/Pharma/Energy/Metal/Realty/Commodities/Services/Infra.
      const SECTOR_BASKET_IDS = ['nifty-bank','nifty-fin-service','nifty-auto','nifty-fmcg','nifty-it','nifty-pharma','nifty-energy','nifty-metal','nifty-realty','nifty-commodities','nifty-services-sector','nifty-infra'];
      // Phase 9.3 — Panorama additions (labor, credit quality, FDI). Most have no free FRED feed; we attempt
      // fetchWithFallback cascades against likely IDs, otherwise the cell degrades to status='unavailable'.
      const [gdp, pfce, gfcf, iipRes, cpi, wpi, repo, gsec10, tbill3m, bankCreditRes, depositsRes, tradeBalanceRes, fxReservesRes, brentSeries, niftySeries, niftyAutoSeries, systemRoe, sectorBenchmarks, roeGsecSpreadSeries, unemploymentRes, lfprRes, fdiNetRes] = await Promise.all([
        fetchFRED('NYGDPMKTPKDZGIND', fredKey, longStartISO),
        fetchFRED('NAEXKP02INQ189S',  fredKey, startISO),
        fetchFRED('INDGFCFQDSMEI',    fredKey, startISO),
        // Phase 9.2 — Cascading fallback: IIP → manufacturing index → Nifty (price-based pulse).
        fetchWithFallback({ primaryId: 'INDPROINDMISMEI', primaryLabel: 'FRED INDPROINDMISMEI (monthly)', fallbackId: 'INDPRMNTO01IXOBSAM', fallbackLabel: 'FRED INDPRMNTO01IXOBSAM (mfg index, fallback)', yahooTicker: '%5ENSEI', yahooLabel: 'Yahoo NSEI close (price proxy, fallback)', fredKey, startISO }),
        fetchFRED('INDCPIALLMINMEI', fredKey, longStartISO),
        fetchFRED('INDPPIALLMINMEI', fredKey, longStartISO),
        fetchFRED('INDIRSTCB01STM',  fredKey, longStartISO),
        fetchFRED('INDIRLTLT01STM',  fredKey, longStartISO),
        fetchFRED('INDIR3TIB01STM',  fredKey, longStartISO),
        // Phase 9.2 — Bank Credit fallback to M3 money supply.
        fetchWithFallback({ primaryId: 'INDMABMM301IDM', primaryLabel: 'FRED INDMABMM301IDM (RBI proxy, monthly)', fallbackId: 'MABMM301INM189S', fallbackLabel: 'FRED MABMM301INM189S (M3, fallback)', fredKey, startISO: longStartISO }),
        // Phase 9.2 — Deposits primary M3; fallback to RBI proxy.
        fetchWithFallback({ primaryId: 'MABMM301INM189S', primaryLabel: 'FRED MABMM301INM189S (M3 proxy, monthly)', fallbackId: 'INDMABMM301IDM', fallbackLabel: 'FRED INDMABMM301IDM (RBI proxy, fallback)', fredKey, startISO: longStartISO }),
        // Phase 9.2 — Trade Balance fallback to INR/USD FX (external sector proxy).
        fetchWithFallback({ primaryId: 'INDXTBAL', primaryLabel: 'FRED INDXTBAL (monthly)', fallbackId: 'XTNTVA01INM664S', fallbackLabel: 'FRED XTNTVA01INM664S (trade value, fallback)', yahooTicker: 'INR=X', yahooLabel: 'Yahoo INR=X (FX volatility proxy, fallback)', fredKey, startISO: longStartISO }),
        // Phase 9.2 — FX Reserves fallback to INR/USD FX proxy.
        fetchWithFallback({ primaryId: 'TRESEGINM052N', primaryLabel: 'FRED TRESEGINM052N (monthly)', yahooTicker: 'INR=X', yahooLabel: 'Yahoo INR=X (FX volatility proxy, fallback)', fredKey, startISO: longStartISO }),
        fetchYahooDailySeries('BZ=F', startISO, todayISO),
        fetchYahooDailySeries('%5ENSEI', startISO, todayISO),
        fetchYahooDailySeries('%5ECNXAUTO', startISO, todayISO),
        buildSystemROE(sb),
        fetchSectorMonthlyCloses(sb, SECTOR_BASKET_IDS, 10),
        // Phase 9.3 — Monthly ROE − 10Y G-Sec spread for the new MacroBandChart.
        fetchRoeGsecSpreadSeries(sb, 10),
        // Phase 9.3 — Unemployment: try ILO India estimate (FRED), fallback to OECD India unemployment, then Yahoo INR=X as a last-resort proxy.
        fetchWithFallback({ primaryId: 'SLEMUEMUITITREQ156S', primaryLabel: 'FRED SLEMUEMUITITREQ156S (ILO India unemployment %, quarterly)', fallbackId: 'LRUN64TTINQ156S', fallbackLabel: 'FRED LRUN64TTINQ156S (OECD India harmonised UR, fallback)', fredKey, startISO: longStartISO }),
        // Phase 9.3 — Labour Force Participation Rate: try ILO India estimate, fallback to OECD index.
        fetchWithFallback({ primaryId: 'LRACTTTTINQ156S', primaryLabel: 'FRED LRACTTTTINQ156S (OECD India LFPR, quarterly)', fallbackId: 'SLTLFTOTLSPZSIND', fallbackLabel: 'FRED SLTLFTOTLSPZSIND (World Bank LFPR India, fallback)', fredKey, startISO: longStartISO }),
        // Phase 9.3 — FDI Net Inflows: FRED BMFAFFINMNOCURMEI; fallback to gross FDI series.
        fetchWithFallback({ primaryId: 'BMFAFFINMNOCURMEI', primaryLabel: 'FRED BMFAFFINMNOCURMEI (FDI net inflows USD, monthly)', fallbackId: 'BNFINFINW01STSAM', fallbackLabel: 'FRED BNFINFINW01STSAM (FDI gross flows, fallback)', fredKey, startISO: longStartISO }),
      ]);
      const iip = iipRes.map; const bankCredit = bankCreditRes.map; const deposits = depositsRes.map;
      const tradeBalance = tradeBalanceRes.map; const fxReserves = fxReservesRes.map;
      const yieldSpreadSeries = buildYieldSpreadSeries(gsec10, tbill3m);
      const latestSpread = yieldSpreadSeries.length ? yieldSpreadSeries[yieldSpreadSeries.length - 1] : null;
      const brentMap = new Map<string, number>(); for (const p of brentSeries) brentMap.set(p.date, p.close);

      // Phase 9.1 helpers — auto-sales-as-proxy and ROE/GSec spread
      function lastYoYFromDaily(series: { date: string; close: number }[], lookbackTradingDays = 252): { value: number | null; yoy: number | null; date: string | null } {
        if (!series || series.length < lookbackTradingDays + 1) return { value: series && series.length ? +series[series.length-1].close.toFixed(2) : null, yoy: null, date: series && series.length ? series[series.length-1].date : null };
        const last = series[series.length - 1]; const lag = series[series.length - 1 - lookbackTradingDays];
        const yoy = lag.close > 0 ? +(((last.close / lag.close) - 1) * 100).toFixed(2) : null;
        return { value: +last.close.toFixed(2), yoy, date: last.date };
      }
      const autoLatest = lastYoYFromDaily(niftyAutoSeries, 252);
      const latestGSec = (() => { const dates = Array.from(gsec10.keys()).sort(); return dates.length ? gsec10.get(dates[dates.length-1])! : null; })();
      const roeGsecSpreadVal = (systemRoe.roe != null && latestGSec != null) ? +(systemRoe.roe - latestGSec).toFixed(2) : null;

      const out = {
        ts: new Date().toISOString(),
        deploy_version: DEPLOY_VERSION,
        growth: {
          gdp:  buildMacroCell(gdp,  1, 'Real GDP Growth (annual %)',     'FRED NYGDPMKTPKDZGIND (annual)',     FRED_URL('NYGDPMKTPKDZGIND'), { yoyKind: 'level' }),
          pfce: buildMacroCell(pfce, 4, 'Private Consumption (PFCE YoY)', 'FRED NAEXKP02INQ189S (quarterly)',  FRED_URL('NAEXKP02INQ189S')),
          gfcf: buildMacroCell(gfcf, 4, 'Investment (GFCF YoY)',          'FRED INDGFCFQDSMEI (quarterly)',     FRED_URL('INDGFCFQDSMEI')),
        },
        industrial: {
          iip:     buildMacroCell(iip, 12, 'Index of Industrial Production (YoY)', iipRes.source, iipRes.sourceUrl, { provenance: iipRes.provenance }),
          coreInd: buildMacroCell(iip, 12, '8 Core Industries (IIP proxy YoY)',    iipRes.source + ' (proxy)', iipRes.sourceUrl, { provenance: iipRes.provenance }),
          capUtil: unavailableCell('Capacity Utilisation', 'RBI OBICUS quarterly — no free feed', 'https://dbie.rbi.org.in/'),
          pmiMfg:  unavailableCell('Manufacturing PMI',    'HSBC India — no free feed', 'https://www.pmi.spglobal.com/'),
          pmiSvc:  unavailableCell('Services PMI',         'HSBC India — no free feed', 'https://www.pmi.spglobal.com/'),
        },
        inflation: {
          cpi:         buildMacroCell(cpi,  12, 'CPI Inflation (Headline YoY)',  'FRED INDCPIALLMINMEI (monthly)', FRED_URL('INDCPIALLMINMEI')),
          coreCpi:     unavailableCell('Core CPI (ex food & fuel)', 'MoSPI series — no free FRED feed', 'https://www.mospi.gov.in/'),
          wpi:         buildMacroCell(wpi,  12, 'WPI Inflation (YoY)',           'FRED INDPPIALLMINMEI (PPI proxy, monthly)', FRED_URL('INDPPIALLMINMEI')),
          repo:        buildMacroCell(repo, 12, 'RBI Repo Rate (short-term)',    'FRED INDIRSTCB01STM (monthly)', FRED_URL('INDIRSTCB01STM'), { yoyKind: 'diff' }),
          yieldSpread: {
            value:  latestSpread ? +latestSpread.value.toFixed(2) : null,
            date:   latestSpread ? latestSpread.date : null,
            yoy:    null,
            status: yieldSpreadSeries.length > 0 ? 'ok' : 'unavailable',
            label:  '10Y G-Sec − 3M T-Bill Yield Spread',
            source: 'FRED INDIRLTLT01STM − INDIR3TIB01STM (monthly)',
            sourceUrl: FRED_URL('INDIRLTLT01STM'),
            series: yieldSpreadSeries,
          },
          corp5y:      unavailableCell('5Y AAA Corporate Spread', 'CRISIL/CCIL — no free feed', 'https://www.ccilindia.com/'),
        },
        banking: {
          credit:   buildMacroCell(bankCredit, 12, 'Systemic Bank Credit (YoY)',     bankCreditRes.source, bankCreditRes.sourceUrl, { provenance: bankCreditRes.provenance }),
          deposits: buildMacroCell(deposits,   12, 'Aggregate Deposit Growth (YoY)', depositsRes.source,   depositsRes.sourceUrl,   { provenance: depositsRes.provenance }),
        },
        external: {
          tradeBalance: buildMacroCell(tradeBalance, 12, 'Merchandise Trade Balance (USD)', tradeBalanceRes.source, tradeBalanceRes.sourceUrl, { provenance: tradeBalanceRes.provenance }),
          fxReserves:   buildMacroCell(fxReserves,   12, 'FX Reserves ex Gold (USD)',        fxReservesRes.source,   fxReservesRes.sourceUrl,   { provenance: fxReservesRes.provenance }),
          fiiFlows:     unavailableCell('FII / FPI Net Equity Flows', 'NSDL Static Reports — no free real-time feed', 'https://www.fpi.nsdl.co.in/'),
          brent: (() => {
            const dates = Array.from(brentMap.keys()).sort();
            if (dates.length === 0) return unavailableCell('Brent Crude (USD/bbl)', 'Yahoo BZ=F', 'https://finance.yahoo.com/quote/BZ%3DF');
            const latestDate = dates[dates.length - 1]; const latest = brentMap.get(latestDate)!;
            const yaDate = (() => { const d = new Date(latestDate); d.setFullYear(d.getFullYear()-1); const iso = d.toISOString().split('T')[0]; let lo=0, hi=dates.length-1, best=-1; while (lo<=hi){ const m=(lo+hi)>>1; if (dates[m] <= iso) { best=m; lo=m+1; } else hi=m-1; } return best >= 0 ? dates[best] : null; })();
            const ya = yaDate ? brentMap.get(yaDate)! : null;
            const yoy = (ya && ya > 0) ? +(((latest/ya)-1)*100).toFixed(2) : null;
            return { value: +latest.toFixed(2), date: latestDate, yoy, status: 'ok', label: 'Brent Crude (USD/bbl)', source: 'Yahoo BZ=F (ICE Brent front-month, daily)', sourceUrl: 'https://finance.yahoo.com/quote/BZ%3DF' };
          })(),
        },
        // Phase 9.1 — Business Cycle & Leverage
        businessCycle: {
          capUtil:        unavailableCell('Capacity Utilisation (RBI OBICUS)', 'RBI OBICUS Quarterly Survey — no free feed', 'https://dbie.rbi.org.in/DBIE/dbie.rbi?site=publications#!12'),
          netDebtEquity:  unavailableCell('Systemic Net Debt-to-Equity', 'CMIE Prowess / RBI FSR — no free feed', 'https://rbi.org.in/Scripts/PublicationsView.aspx?id=22272'),
          systemRoe:      systemRoe.roe != null ? { value: systemRoe.roe, date: systemRoe.date, yoy: null, status: 'ok', label: 'Aggregate System ROE (Nifty 50 derived)', source: `Derived: PB ${systemRoe.pb} / PE ${systemRoe.pe} × 100, latest Nifty 50 daily`, sourceUrl: 'https://www.niftyindices.com/reports/historical-data' } : unavailableCell('Aggregate System ROE', 'Derived from Nifty 50 PB/PE — awaiting fresh data', 'https://www.niftyindices.com/'),
          roeGsecSpread:  roeGsecSpreadVal != null ? { value: roeGsecSpreadVal, date: systemRoe.date, yoy: null, status: 'ok', label: 'ROE − 10Y G-Sec Spread (Capital-deployment edge)', source: `System ROE ${systemRoe.roe ?? '—'}% − G-Sec ${latestGSec != null ? latestGSec.toFixed(2) : '—'}%`, sourceUrl: FRED_URL('INDIRLTLT01STM'), series: roeGsecSpreadSeries } : { value: null, date: null, yoy: null, status: 'unavailable', label: 'ROE − G-Sec Spread', source: 'Awaiting ROE and bond-yield data', sourceUrl: null, series: roeGsecSpreadSeries },
        },
        // Phase 9.3 — Labor & Employment Health
        labor: {
          unemployment:   buildMacroCell(unemploymentRes.map, 4, 'Unemployment Rate', unemploymentRes.source, unemploymentRes.sourceUrl, { provenance: unemploymentRes.provenance, yoyKind: 'level' }),
          lfpr:           buildMacroCell(lfprRes.map,         4, 'Labour Force Participation Rate', lfprRes.source, lfprRes.sourceUrl, { provenance: lfprRes.provenance, yoyKind: 'level' }),
          hiringActivity: unavailableCell('Naukri JobSpeak Index (Hiring Activity)', 'Naukri JobSpeak monthly report — no public API', 'https://www.naukri.com/jobspeak/index'),
        },
        // Phase 9.3 — Credit Quality (sectoral deployment): Gold Loans + MSME Credit
        creditQuality: {
          goldLoanGrowth:    unavailableCell('Gold Loan Growth YoY (RBI sectoral deployment)', 'RBI Sectoral Deployment of Bank Credit — no free API', 'https://rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx'),
          msmeCreditGrowth:  unavailableCell('MSME Credit Growth YoY (RBI sectoral deployment)', 'RBI Sectoral Deployment of Bank Credit — no free API', 'https://rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx'),
        },
        // Phase 9.3 — FDI Dynamics (gross vs net)
        fdi: {
          netFdi:           buildMacroCell(fdiNetRes.map, 12, 'Net FDI Inflows (USD, YoY)', fdiNetRes.source, fdiNetRes.sourceUrl, { provenance: fdiNetRes.provenance }),
        },
        // Phase 9.1 — High-Frequency Consumption & Logistics
        highFreq: {
          autoSalesPV:      unavailableCell('PV (Passenger Vehicles) Sales YoY', 'SIAM monthly — no free feed', 'https://www.siam.in/statistics.aspx'),
          autoSales2W:      unavailableCell('2-Wheeler Sales YoY', 'SIAM monthly — no free feed', 'https://www.siam.in/statistics.aspx'),
          autoSalesCV:      unavailableCell('CV (Commercial Vehicle) Sales YoY', 'SIAM monthly — no free feed', 'https://www.siam.in/statistics.aspx'),
          autoSalesTractors:unavailableCell('Tractor Sales YoY', 'TMA monthly — no free feed', 'https://tractormanufacturersassociation.in/'),
          autoIndexYoY:     autoLatest.value != null ? { value: autoLatest.value, date: autoLatest.date, yoy: autoLatest.yoy, status: 'ok', label: 'Nifty Auto YoY (price-based proxy for sector demand)', source: 'Yahoo %5ECNXAUTO (daily close, 252d lag)', sourceUrl: 'https://finance.yahoo.com/quote/%5ECNXAUTO' } : unavailableCell('Nifty Auto YoY', 'Yahoo CNXAUTO — fetch failed', 'https://finance.yahoo.com/'),
          fuelConsumption:  unavailableCell('Petroleum Product Consumption YoY', 'PPAC monthly — no free feed', 'https://ppac.gov.in/consumption-of-petroleum-products'),
        },
        // Phase 9.1 — Freight & Logistics
        freight: {
          portCargo:        unavailableCell('Major Port Cargo Traffic YoY', 'IPA monthly — no free feed', 'https://ipa.nic.in/'),
          railwayFreight:   unavailableCell('Railway Freight (NTKMs) YoY', 'Indian Railways monthly — no free feed', 'https://indianrailways.gov.in/'),
          ewayBills:        unavailableCell('E-Way Bills Generated YoY', 'GSTN monthly — no free feed', 'https://ewaybillgst.gov.in/'),
        },
        // Phase 9.1 — Institutional Flows
        capitalFlows: {
          fiiEquity:        unavailableCell('FII / FPI Net Equity Flows', 'NSDL Static Reports — no free real-time feed', 'https://www.fpi.nsdl.co.in/web/Reports/Yearwise.aspx'),
          fiiDebt:          unavailableCell('FII / FPI Net Debt Flows', 'NSDL Static Reports — no free real-time feed', 'https://www.fpi.nsdl.co.in/web/Reports/Yearwise.aspx'),
          diiInflows:       unavailableCell('DII / Mutual Fund Net Equity Flows', 'AMFI monthly — no free feed', 'https://www.amfiindia.com/research-information/aum-data/categorization-of-mutual-fund-schemes'),
        },
        niftyClose: niftySeries.map(p => ({ date: p.date, value: p.close })),
        // Phase 9.2 — Per-sector monthly close benchmarks (last 10y) for dynamic CAGR backtest of the overweight basket.
        sectorBenchmarks: sectorBenchmarks,
      };
      return new Response(JSON.stringify(out), { headers: { ...CORS, 'Cache-Control': 'public, max-age=21600, s-maxage=21600' } });
    }
    return new Response(JSON.stringify({error:'unknown mode'}), { headers: CORS });
  } catch (e) { return new Response(JSON.stringify({ error: String(e) }), { status: 500, headers: CORS }); }
});
