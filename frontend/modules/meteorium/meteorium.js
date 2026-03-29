/**
 * modules/meteorium/meteorium.js
 * Prexus Intelligence — Meteorium Module Orchestrator (ULTIMATE FINAL)
 */

import { store } from '../../js/store.js';
import { initPrefetch, destroyPrefetch } from '../../js/prefetch.js';
import { startPredictionScheduler, stopPredictionScheduler } from '../../js/predict.js';
import { logout } from '../../js/auth.js';
import { navigate } from '../../js/router.js';
import { startClock } from '../../js/utils.js';

/* ── Lazy module imports ─────────────────────────────────── */
const MODULES = {
  dashboard: () => import('./dashboard.js'),
  portfolio: () => import('./portfolio.js'),
  analysis:  () => import('./analysis.js'),
  signals:   () => import('./signals.js'),
  sources:   () => import('./sources.js'),
  pipeline:  () => import('./pipeline.js'),
  ai:        () => import('./ai.js'),
};

const NAV = [
  { id:'dashboard', ico:'fa-chart-line',     label:'Overview' },
  { id:'portfolio', ico:'fa-layer-group',    label:'Asset Portfolio' },
  { id:'analysis',  ico:'fa-bolt',           label:'Risk Analysis' },
  { id:'signals',   ico:'fa-wave-square',    label:'Signals' },
  { id:'sources',   ico:'fa-satellite-dish', label:'Data Sources' },
  { id:'pipeline',  ico:'fa-sitemap',        label:'Pipeline' },
  { id:'ai',        ico:'fa-microchip',      label:'AI Intelligence', badge:'AI' },
];

const VIEW_TITLES = {
  dashboard: 'Overview',
  portfolio: 'Asset Portfolio',
  analysis:  'Risk Analysis',
  signals:   'Signals',
  sources:   'Data Sources',
  pipeline:  'Pipeline',
  ai:        'AI Intelligence',
};

let _activeModule = null;
let _activeId     = null;
let _clockStop    = null;
let _mountToken   = 0;

/* ══════════════════════════════════════════════════════════
   PUBLIC API
══════════════════════════════════════════════════════════ */

export async function initMeteorium() {
  _renderShell();
  _startClock();

  startPredictionScheduler(); // singleton start
  initPrefetch();

  await _mount('dashboard');
}

export function destroyMeteorium() {
  _unmountCurrent();

  stopPredictionScheduler();
  destroyPrefetch();

  _clockStop?.();
  _clockStop = null;
}

export function updateTopbar(opts = {}) {
  if (opts.title) {
    const el = document.getElementById('met-topbar-title');
    if (el) el.textContent = opts.title;
  }

  if (opts.signalCount !== undefined) {
    const el = document.getElementById('met-topbar-alerts');
    if (el) {
      el.textContent = opts.signalCount;
      el.style.display = opts.signalCount > 0 ? 'flex' : 'none';
    }
  }
}

/* ══════════════════════════════════════════════════════════
   SHELL
══════════════════════════════════════════════════════════ */

function _renderShell() {
  const page = document.getElementById('page-meteorium');
  if (!page) return;

  const user = store.get('user');
  const org  = store.get('org');

  page.innerHTML = `
  <div class="met-cls">UNCLASSIFIED // FOUO // METEORIUM v3.1.0</div>

  <div class="met-shell">

    <div class="met-sidebar">
      <div class="met-sidebar-logo">
        <div class="met-sidebar-icon">
          <i class="fa-solid fa-cloud-bolt"></i>
        </div>
        <div>
          <div class="met-sidebar-name">METEORIUM</div>
          <div class="met-sidebar-sub">by PREXUS</div>
        </div>
      </div>

      <div style="flex:1;overflow:auto">
        <div class="met-nav-section">Navigation</div>

        ${NAV.map(n => `
          <div class="met-nav-item ${n.id==='dashboard'?'active':''}" data-nav="${n.id}">
            <span class="ico"><i class="fa-solid ${n.ico}"></i></span>
            ${n.label}
            ${n.badge ? `<span class="met-nav-badge">${n.badge}</span>`:''}
          </div>
        `).join('')}

        <div class="met-nav-section">System</div>

        <div class="met-nav-item" data-back>
          <span class="ico"><i class="fa-solid fa-grid-2"></i></span>
          Back to Hub
        </div>
      </div>

      <div class="met-sidebar-footer">
        <div class="status-dot live pulse"></div>
        <div class="met-user-line">ORG: ${org?.orgName || '—'}</div>
        <div class="met-user-line">USER: ${user?.email || '—'}</div>

        <button id="met-logout">Sign Out</button>
      </div>
    </div>

    <div class="met-main">
      <div class="met-topbar">
        <span id="met-topbar-title">Overview</span>

        <div>
          <span id="met-loading-indicator" style="display:none">Loading…</span>
          <span id="met-utc-clock"></span>

          <div id="met-topbar-alerts" style="display:none">0</div>
        </div>
      </div>

      <div id="met-workspace"></div>
    </div>
  </div>

  <div class="met-cls">PREXUS INC.</div>
  `;

  page.addEventListener('click', (e) => {
    const nav = e.target.closest('[data-nav]');
    if (nav) return _navTo(nav.dataset.nav);

    if (e.target.closest('[data-back]')) {
      navigate('hub');
    }

    if (e.target.closest('#met-logout')) {
      logout();
    }
  });
}

/* ══════════════════════════════════════════════════════════
   MODULE SYSTEM
══════════════════════════════════════════════════════════ */

async function _mount(id) {
  if (_activeId === id) return;

  const token = ++_mountToken;

  _setLoading(true);
  _unmountCurrent();
  _setNavActive(id);

  const workspace = document.getElementById('met-workspace');
  if (!workspace) return;

  workspace.innerHTML = `
    <div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-muted);font-family:var(--font-data);font-size:9px;gap:8px">
      <span class="spinner"></span>
      Loading ${VIEW_TITLES[id]}…
    </div>
  `;

  const loader = MODULES[id];

  if (!loader) {
    workspace.textContent = `Module not found: ${id}`;
    return;
  }

  try {
    const mod = await loader();

    if (token !== _mountToken) return;

    _activeModule = mod;
    _activeId = id;
    store.set('module', id);

    workspace.innerHTML = '';
    await mod.init(workspace);

  } catch (err) {
    console.error('[meteorium] load error:', err);

    workspace.innerHTML = '';
    const errorEl = document.createElement('div');
    errorEl.style.color = 'var(--red)';
    errorEl.textContent = err?.message || 'Unknown error';
    workspace.appendChild(errorEl);

  } finally {
    _setLoading(false);
  }
}

function _unmountCurrent() {
  if (_activeModule?.destroy) {
    try {
      _activeModule.destroy();
    } catch (err) {
      console.warn('[meteorium] destroy failed:', err);
    }
  }
  _activeModule = null;
  _activeId = null;
}

function _navTo(id) {
  _mount(id);
}

function _setNavActive(id) {
  const page = document.getElementById('page-meteorium');
  if (!page) return;

  NAV.forEach(n => {
    const el = page.querySelector(`[data-nav="${n.id}"]`);
    if (el) el.classList.toggle('active', n.id === id);
  });
}

/* ══════════════════════════════════════════════════════════
   UTILITIES
══════════════════════════════════════════════════════════ */

function _setLoading(state) {
  const el = document.getElementById('met-loading-indicator');
  if (el) el.style.display = state ? 'inline' : 'none';
}

function _startClock() {
  _clockStop?.();
  _clockStop = startClock(({ time }) => {
    const el = document.getElementById('met-utc-clock');
    if (el) el.textContent = time;
  });
}