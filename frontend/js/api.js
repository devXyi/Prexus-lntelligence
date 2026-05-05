/**
 * js/api.js
 * Prexus Intelligence — API Layer
 * THE GREAT FILE · Phase 1
 *
 * Every backend call goes through here. No module makes raw fetch() calls.
 * Handles: base URL, JWT headers, error normalisation, timeout.
 *
 * Go Gateway (auth, assets):    window.API_BASE  (default: /api or Render URL)
 * Python Engine (risk, AI):     proxied through Go gateway
 */

import { store } from './store.js';

/* ── Configuration ───────────────────────────────────────── */

function getBase() {
  return (
    window.API_BASE ||
    'https://prexus-intelligence-.onrender.com'
  );
}

const TIMEOUT_MS     = 55_000;   // 55s — Render free tier cold start
const TIMEOUT_SHORT  = 15_000;   // 15s — health checks, fast endpoints

/* ── Core fetch wrapper ──────────────────────────────────── */

/**
 * Make an authenticated API request.
 * @param {string} path       - e.g. '/login' or '/assets'
 * @param {object} options    - fetch options (method, body, etc.)
 * @param {number} timeoutMs  - request timeout
 * @returns {Promise<any>}    - parsed JSON response
 * @throws {ApiError}
 */
async function request(path, options = {}, timeoutMs = TIMEOUT_MS) {
  const token = store.get('token');

  const headers = {
    'Content-Type': 'application/json',
    ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
    ...(options.headers || {}),
  };

  const controller = new AbortController();
  const timeoutId  = setTimeout(() => controller.abort(), timeoutMs);

  let response;
  try {
    response = await fetch(getBase() + path, {
      ...options,
      headers,
      signal: controller.signal,
    });
  } catch (err) {
    clearTimeout(timeoutId);
    if (err.name === 'AbortError') {
      throw new ApiError('Request timed out — server may be waking up.', 0, 'TIMEOUT');
    }
    throw new ApiError('Cannot reach server. Check your connection.', 0, 'NETWORK');
  } finally {
    clearTimeout(timeoutId);
  }

  // Parse JSON body (even for error responses)
  let data;
  try {
    data = await response.json();
  } catch {
    data = {};
  }

  if (!response.ok) {
    const message = data?.error || data?.detail || `Server error ${response.status}`;
    throw new ApiError(message, response.status, 'HTTP');
  }

  return data;
}

/** Shorthand helpers */
const get  = (path, timeout) => request(path, { method: 'GET' }, timeout);
const post = (path, body, timeout) => request(path, { method: 'POST', body: JSON.stringify(body) }, timeout);
const put  = (path, body) => request(path, { method: 'PUT',  body: JSON.stringify(body) });
const del  = (path)       => request(path, { method: 'DELETE' });

/* ── Custom error class ──────────────────────────────────── */

export class ApiError extends Error {
  constructor(message, status = 0, type = 'UNKNOWN') {
    super(message);
    this.name   = 'ApiError';
    this.status = status;
    this.type   = type;
  }

  get isTimeout()  { return this.type === 'TIMEOUT'; }
  get isNetwork()  { return this.type === 'NETWORK'; }
  get isUnauth()   { return this.status === 401; }
  get isConflict() { return this.status === 409; }
}

/* ══════════════════════════════════════════════════════════
   AUTH ENDPOINTS — Go Gateway
══════════════════════════════════════════════════════════ */

/**
 * Register a new account.
 * Returns { token, user }
 */
export async function register(email, password, fullName = '', orgName = '') {
  return post('/register', { email, password, full_name: fullName, org_name: orgName });
}

/**
 * Log in with email + password.
 * Returns { token, user }
 */
export async function login(email, password) {
  return post('/login', { email, password });
}

/**
 * Get current authenticated user.
 * Returns UserDTO
 */
export async function getMe() {
  return get('/me', TIMEOUT_SHORT);
}

/**
 * Update user profile.
 */
export async function updateMe(fullName, orgName) {
  return put('/me', { full_name: fullName, org_name: orgName });
}

/* ══════════════════════════════════════════════════════════
   ASSET ENDPOINTS — Go Gateway
══════════════════════════════════════════════════════════ */

/** List all assets for the authenticated user. */
export async function getAssets() {
  return get('/assets', TIMEOUT_SHORT);
}

/** Create a new asset. Returns the created Asset. */
export async function createAsset(data) {
  return post('/assets', data);
}

/** Update an existing asset. Returns the updated Asset. */
export async function updateAsset(id, data) {
  return put(`/assets/${id}`, data);
}

/** Delete an asset. */
export async function deleteAsset(id) {
  return del(`/assets/${id}`);
}

/* ══════════════════════════════════════════════════════════
   RISK ENDPOINTS — Python Engine (proxied via Go)
══════════════════════════════════════════════════════════ */

/**
 * Score a single asset.
 * Returns full AssetRiskResult including intelligence packet.
 */
export async function scoreAsset({
  assetId,
  lat,
  lon,
  countryCode = 'IND',
  valueMm     = 10,
  assetType   = 'infrastructure',
  scenario    = 'baseline',
  horizonDays = 365,
  useCache    = true,
  useSatellite = false,
}) {
  return post('/risk/asset', {
    asset_id:     assetId,
    lat,
    lon,
    country_code: countryCode,
    value_mm:     valueMm,
    asset_type:   assetType,
    scenario,
    horizon_days: horizonDays,
    use_cache:    useCache,
    use_satellite: useSatellite,
  });
}

/**
 * Score a portfolio of assets with correlated Monte Carlo.
 */
export async function scorePortfolio(assets, scenario = 'baseline') {
  return post('/risk/portfolio', {
    assets: assets.map(a => ({
      asset_id:     a.id,
      lat:          a.lat   || 0,
      lon:          a.lon   || 0,
      country_code: a.cc    || 'IND',
      value_mm:     a.value_mm || 10,
      asset_type:   a.type  || 'infrastructure',
      horizon_days: 30,
    })),
    scenario,
    horizon_days: 30,
  });
}

/**
 * Run SSP scenario stress test for a single asset.
 */
export async function stressTest(assetId, lat, lon, countryCode, valueMm, assetType) {
  return post('/risk/stress-test', {
    asset_id:     assetId,
    lat, lon,
    country_code: countryCode,
    value_mm:     valueMm,
    asset_type:   assetType,
    scenario:     'baseline',
    horizon_days: 365,
  });
}

/**
 * Get Monte Carlo loss histogram for visualization.
 */
export async function getHistogram(assetId, lat, lon, countryCode, valueMm, assetType, scenario) {
  return post('/risk/histogram', {
    asset_id:     assetId,
    lat, lon,
    country_code: countryCode,
    value_mm:     valueMm,
    asset_type:   assetType,
    scenario:     scenario || 'baseline',
    horizon_days: 365,
  });
}

/* ══════════════════════════════════════════════════════════
   INTELLIGENCE ENDPOINTS — Python Engine
══════════════════════════════════════════════════════════ */

/**
 * AI analysis — single prompt.
 * Returns { result, model }
 */
export async function analyzeAI(prompt, model = 'gemini') {
  return post('/analyze', { prompt, model });
}

/**
 * AI chat — multi-turn conversation.
 * messages: [{ role: 'user'|'assistant', content: string }]
 * Returns { result, model }
 */
export async function chatAI(messages, model = 'gemini') {
  return post('/chat', { messages, model });
}

/* ══════════════════════════════════════════════════════════
   SYSTEM HEALTH — Python Engine
══════════════════════════════════════════════════════════ */

/** Full pipeline health (Layer 0–6 + Redis + Rust). */
export async function riskHealth() {
  return get('/risk/health', TIMEOUT_SHORT);
}

/** Go gateway health check. */
export async function gatewayHealth() {
  return get('/health', TIMEOUT_SHORT);
}

/** Layer 0 source registry. */
export async function listSources() {
  return get('/sources', TIMEOUT_SHORT);
}

/** Layer 2 data lake stats. */
export async function lakeStats() {
  return get('/lake/stats', TIMEOUT_SHORT);
}

/** Redis Streams queue stats. */
export async function queueStats() {
  return get('/queue/stats', TIMEOUT_SHORT);
}

/* ══════════════════════════════════════════════════════════
   WARM-UP UTILITY
══════════════════════════════════════════════════════════ */

/**
 * Ping the gateway to warm up Render free tier cold start.
 * Fire and forget — called on app load.
 */
export function warmUp() {
  fetch(getBase() + '/health', {
    method: 'GET',
    signal: AbortSignal.timeout(55_000),
  }).catch(() => {});
}

