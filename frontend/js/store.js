/**
 * js/store.js
 * Prexus Intelligence — Reactive Global State Store
 *
 * Upgraded from polling-based to subscription-driven.
 * Every mutation emits typed events. No setInterval page watchers.
 * All modules subscribe declaratively — store is the single source of truth.
 */

/* ══════════════════════════════════════════════════════════
   STATE SHAPE
══════════════════════════════════════════════════════════ */
const _state = {
  token:         null,
  user:          null,
  org:           null,
  page:          'auth',
  module:        'dashboard',
  assets:        [],
  selectedAsset: null,
  riskCache:     {},    // assetId → { scores, ts, trajectory }
  aiCache:       {},    // assetId+scenario → { brief, ts }
  loading:       new Set(),
  errors:        new Map(),
};

/* ══════════════════════════════════════════════════════════
   SUBSCRIBER REGISTRY
══════════════════════════════════════════════════════════ */
const _subscribers = new Map();   // key → Set<fn>
const _once        = new Map();   // key → Set<fn>  (fire once then remove)

function _notify(key, value) {
  _subscribers.get(key)?.forEach(cb => {
    try { cb(value, key); } catch(e) { console.error(`[store] subscriber error "${key}":`, e); }
  });
  _once.get(key)?.forEach(cb => {
    try { cb(value, key); } catch(e) {}
  });
  _once.delete(key);

  // Always fire '*' wildcard subscribers
  _subscribers.get('*')?.forEach(cb => {
    try { cb(value, key); } catch(e) {}
  });
}

/* ══════════════════════════════════════════════════════════
   PUBLIC API
══════════════════════════════════════════════════════════ */
export const store = {

  get(key) {
    return _state[key];
  },

  set(key, value) {
    const prev = _state[key];
    _state[key] = value;
    // Only notify if value actually changed (shallow compare)
    if (prev !== value) _notify(key, value);
  },

  setMany(updates) {
    for (const [key, value] of Object.entries(updates)) {
      const prev = _state[key];
      _state[key] = value;
      if (prev !== value) _notify(key, value);
    }
  },

  /** Immutably update a nested key */
  patch(key, updates) {
    const prev = _state[key];
    _state[key] = { ...(prev || {}), ...updates };
    _notify(key, _state[key]);
  },

  isLoading(key) { return _state.loading.has(key); },

  startLoading(key) {
    _state.loading.add(key);
    _notify('loading', _state.loading);
  },

  stopLoading(key) {
    _state.loading.delete(key);
    _notify('loading', _state.loading);
  },

  setError(key, message) {
    _state.errors.set(key, message);
    _notify('errors', _state.errors);
  },

  clearError(key) {
    _state.errors.delete(key);
    _notify('errors', _state.errors);
  },

  getError(key) {
    return _state.errors.get(key) || null;
  },

  /** Cache risk scores for an asset */
  cacheRisk(assetId, scores) {
    _state.riskCache[assetId] = { ...scores, ts: Date.now() };
    _notify('riskCache', _state.riskCache);
  },

  getRisk(assetId) {
    const c = _state.riskCache[assetId];
    if (!c) return null;
    // Expire after 5 minutes
    if (Date.now() - c.ts > 5 * 60 * 1000) { delete _state.riskCache[assetId]; return null; }
    return c;
  },

  /** Cache AI brief */
  cacheAI(key, brief) {
    _state.aiCache[key] = { brief, ts: Date.now() };
    _notify('aiCache', _state.aiCache);
  },

  getAI(key) {
    const c = _state.aiCache[key];
    if (!c) return null;
    if (Date.now() - c.ts > 10 * 60 * 1000) { delete _state.aiCache[key]; return null; }
    return c.brief;
  },
};

/* ══════════════════════════════════════════════════════════
   SUBSCRIPTION HELPERS
══════════════════════════════════════════════════════════ */

/**
 * Subscribe to state key changes.
 * Returns unsubscribe function.
 *
 * @example
 *   const off = subscribe('assets', (assets) => renderList(assets));
 *   // later: off()
 */
export function subscribe(key, callback) {
  if (!_subscribers.has(key)) _subscribers.set(key, new Set());
  _subscribers.get(key).add(callback);
  return () => _subscribers.get(key)?.delete(callback);
}

/**
 * Subscribe once — fires on next change then auto-removes.
 */
export function subscribeOnce(key, callback) {
  if (!_once.has(key)) _once.set(key, new Set());
  _once.get(key).add(callback);
  return () => _once.get(key)?.delete(callback);
}

/**
 * Subscribe to any key change ('*' wildcard).
 * callback receives (value, key).
 */
export function subscribeAll(callback) {
  return subscribe('*', callback);
}

/**
 * Navigate reactively — replaces setInterval polling in app.html.
 * Emits 'page' change which hub.js and meteorium.js subscribe to.
 */
export function navigateTo(pageId) {
  store.set('page', pageId);
}

/* ══════════════════════════════════════════════════════════
   PERSISTENCE
══════════════════════════════════════════════════════════ */

export function persistSession(token, user) {
  try {
    localStorage.setItem('prx_token', token);
    localStorage.setItem('prx_user', JSON.stringify(user));
  } catch(e) { console.warn('[store] localStorage write failed:', e); }
}

export function restoreSession() {
  try {
    const token  = localStorage.getItem('prx_token');
    const rawUser = localStorage.getItem('prx_user');
    if (!token || !rawUser) return false;

    const user = JSON.parse(rawUser);
    if (!user?.email) return false;

    store.setMany({ token, user });

    const orgRaw = localStorage.getItem(`prexus_org_${user.email}`);
    if (orgRaw) store.set('org', JSON.parse(orgRaw));

    return true;
  } catch(e) {
    console.warn('[store] session restore failed:', e);
    return false;
  }
}

export function persistOrg(orgData) {
  try {
    const email = store.get('user')?.email;
    if (!email) return;
    localStorage.setItem(`prexus_org_${email}`, JSON.stringify(orgData));
    store.set('org', orgData);
  } catch(e) { console.warn('[store] org persist failed:', e); }
}

export function clearSession() {
  try {
    localStorage.removeItem('prx_token');
    localStorage.removeItem('prx_user');
  } catch(e) {}
  store.setMany({ token: null, user: null, org: null, assets: [], selectedAsset: null });
  store.get('loading').clear();
  store.get('errors').clear();
}
