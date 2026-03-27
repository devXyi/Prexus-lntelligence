/**
 * js/prefetch.js
 * Prexus Intelligence — Silent AI Pre-Inference Engine
 *
 * When a user selects an asset, this module silently fires an AI brief
 * request in the background — so by the time they run analysis,
 * the narrative is already waiting.
 *
 * The user never sees it loading. It just appears.
 */

import { store, subscribe } from './store.js';
import { analyzeAI }        from './api.js';
import { computeTrajectory } from './predict.js';
import { fPct, fUsd }        from './utils.js';

/* ── Config ─────────────────────────────────────────────── */
const DEFAULT_MODEL    = 'gemini';
const PREFETCH_DELAY   = 1200;   // ms after selection before firing (debounce)
const MAX_QUEUE        = 3;      // max concurrent prefetch requests

let _queue       = 0;
let _debounce    = null;
let _unsub       = null;
let _initialized = false;

/* ══════════════════════════════════════════════════════════
   INIT — call once when Meteorium mounts
══════════════════════════════════════════════════════════ */

export function initPrefetch() {
  if (_initialized) return;
  _initialized = true;

  // Subscribe to asset selection changes
  _unsub = subscribe('selectedAsset', (asset) => {
    if (!asset) return;
    _schedulePrefetch(asset);
  });

  // Also prefetch when assets first load (prefetch top-risk asset)
  subscribe('assets', (assets) => {
    if (!assets?.length) return;
    const top = [...assets].sort((a, b) => (b.cr ?? 0) - (a.cr ?? 0))[0];
    if (top && !store.getAI(_aiKey(top, 'baseline'))) {
      setTimeout(() => _prefetchAsset(top), 3000);
    }
  });
}

export function destroyPrefetch() {
  if (_unsub) { _unsub(); _unsub = null; }
  if (_debounce) { clearTimeout(_debounce); _debounce = null; }
  _initialized = false;
  _queue = 0;
}

/* ══════════════════════════════════════════════════════════
   PREFETCH LOGIC
══════════════════════════════════════════════════════════ */

function _schedulePrefetch(asset) {
  if (_debounce) clearTimeout(_debounce);
  _debounce = setTimeout(() => _prefetchAsset(asset), PREFETCH_DELAY);
}

async function _prefetchAsset(asset, scenario = 'baseline') {
  const key = _aiKey(asset, scenario);

  // Already cached? Skip.
  if (store.getAI(key)) return;

  // Queue full? Skip (don't spam).
  if (_queue >= MAX_QUEUE) return;

  const token = store.get('token');
  if (!token) return;

  _queue++;

  try {
    const traj = computeTrajectory(asset);
    const prompt = _buildPrompt(asset, traj, scenario);
    const result = await analyzeAI(prompt, DEFAULT_MODEL);

    if (result?.result) {
      store.cacheAI(key, result.result);
      // Signal that a prefetch arrived for this key
      store.set('prefetchReady', key);
    }
  } catch {
    // Silent — prefetch failures are invisible to the user
  } finally {
    _queue--;
  }
}

/* ══════════════════════════════════════════════════════════
   PROMPT BUILDER
   Builds a rich, context-aware prompt using live trajectory data
══════════════════════════════════════════════════════════ */

function _buildPrompt(asset, traj, scenario) {
  const scen = {
    baseline:   'SSP1-1.9 · Paris-aligned ~1.5°C',
    disorderly: 'SSP2-4.5 · Disorderly transition ~2.7°C',
    failed:     'SSP5-8.5 · Failed transition ~4.4°C',
  }[scenario] || 'SSP2-4.5 · Baseline';

  const crossingNote = traj.nextCrossing
    ? `ALERT: Asset projected to cross ${traj.nextCrossing.threshold} threshold in ${traj.nextCrossing.days} days (${Math.round(traj.nextCrossing.confidence * 100)}% confidence).`
    : 'No imminent threshold breach in 30-day horizon.';

  const d7 = traj.projection.find(p => p.day === 7);
  const d30 = traj.projection.find(p => p.day === 30);

  return `You are a senior climate risk analyst at a sovereign intelligence platform.
Generate a concise executive intelligence brief for the following asset.
Be direct, data-driven, and specific. Maximum 180 words.

ASSET: ${asset.name} (${asset.id})
COUNTRY: ${asset.country || 'Unknown'} [${asset.cc || '?'}]
TYPE: ${asset.type || 'Infrastructure'}
VALUE: ${fUsd(asset.value_mm ?? 0)}
SCENARIO: ${scen}

CURRENT RISK SCORES:
- Composite: ${fPct(asset.cr ?? 0)} [${traj.currentBand}]
- Physical:  ${fPct(asset.pr ?? 0)}
- Transition: ${fPct(asset.tr ?? 0)}

PREDICTIVE TRAJECTORY (30-day Monte Carlo):
- Trend: ${traj.trend}
- 7-day P50 projection: ${d7 ? fPct(d7.p50) : 'N/A'} (range ${d7 ? fPct(d7.p10) : '?'}–${d7 ? fPct(d7.p90) : '?'})
- 30-day P50 projection: ${d30 ? fPct(d30.p50) : 'N/A'} (range ${d30 ? fPct(d30.p10) : '?'}–${d30 ? fPct(d30.p90) : '?'})
- Daily drift rate: ${(traj.drift * 100).toFixed(4)}% · Volatility: ${(traj.volatility * 100).toFixed(3)}%

${crossingNote}

SOURCES: Open-Meteo ECMWF · NASA FIRMS VIIRS · Carbon Monitor · IPCC AR6

Provide: (1) Primary risk drivers for this asset's geography and type, (2) Immediate recommended actions, (3) 30-day strategic outlook with specific projections. Be direct.`;
}

/* ══════════════════════════════════════════════════════════
   PUBLIC: Retrieve cached brief (or null if not ready)
══════════════════════════════════════════════════════════ */

export function getCachedBrief(asset, scenario = 'baseline') {
  return store.getAI(_aiKey(asset, scenario)) || null;
}

export function prefetchNow(asset, scenario = 'baseline') {
  return _prefetchAsset(asset, scenario);
}

function _aiKey(asset, scenario) {
  return `${asset.id}:${scenario}`;
}

