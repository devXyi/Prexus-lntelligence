/**
 * js/auth.js
 * Prexus Intelligence — Auth Module
 * THE GREAT FILE · Phase 1
 *
 * Handles:
 *   - Sign in / Register (tab-toggled card)
 *   - Cold-start counter (Render free tier warm-up)
 *   - Session restore on load
 *   - Org onboarding form
 *   - Terminal animation after org creation
 *   - Routing to Hub after full onboarding
 */

import { store, persistSession, persistOrg, restoreSession } from './store.js';
import { login, register, getAssets, warmUp, ApiError } from './api.js';
import { navigate } from './router.js';
import { $, sleep, generateDeviceId, isValidEmail, isValidPassword } from './utils.js';

/* ══════════════════════════════════════════════════════════
   AUTH STATE
══════════════════════════════════════════════════════════ */

let authMode = 'login';   // 'login' | 'register'
let wakeInterval = null;

/* ══════════════════════════════════════════════════════════
   INITIALISE
══════════════════════════════════════════════════════════ */

/**
 * Called by index.html on DOMContentLoaded.
 * Checks for existing session and routes accordingly.
 */
export function initAuth() {
  // Warm up backend (fire-and-forget)
  warmUp();

  // Cookie banner
  _initCookieBanner();

  // ── Cookie bypass ──────────────────────────────────────────────────────
  // If a valid session cookie exists, skip auth entirely.
  // The user sees the hub immediately — no login page flash.
  if (restoreSession()) {
    const token = store.get('token');
    const user  = store.get('user');
    const org   = store.get('org');

    if (token && user) {
      if (org) {
        // Full session: go straight to hub, no auth page shown at all
        _setHubOrgName(org.orgName || user.org_name || user.email);
        _preloadAssetCount();
        navigate('hub');
        return;
      } else {
        // Account exists, org setup incomplete — resume org onboarding
        navigate('org');
        _bindOrgForm();
        return;
      }
    }
  }

  // ── No valid session: show auth page ──────────────────────────────────
  navigate('auth');
  _bindAuthForm();
  _bindOrgForm();
}

/* ══════════════════════════════════════════════════════════
   AUTH FORM
══════════════════════════════════════════════════════════ */

function _bindAuthForm() {
  const form     = $('#authForm');
  const tabLogin = $('#tabLogin');
  const tabReg   = $('#tabRegister');

  if (!form) return;

  // Tab toggle
  tabLogin?.addEventListener('click', () => setAuthMode('login'));
  tabReg?.addEventListener('click',   () => setAuthMode('register'));

  // Submit
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    _handleAuthSubmit();
  });
}

/** Switch between Sign In and Register. */
export function setAuthMode(mode) {
  authMode = mode;

  const tabLogin  = $('#tabLogin');
  const tabReg    = $('#tabRegister');
  const nameField = $('#nameField');
  const btn       = $('#authBtn');
  const errEl     = $('#authError');

  const ACTIVE_STYLE   = 'background:rgba(14,165,233,.15);color:#7DCFF8;';
  const INACTIVE_STYLE = 'background:transparent;color:var(--text-muted);';

  if (mode === 'login') {
    if (tabLogin)  tabLogin.style.cssText  = ACTIVE_STYLE;
    if (tabReg)    tabReg.style.cssText    = INACTIVE_STYLE;
    if (nameField) nameField.style.display = 'none';
    if (btn)       btn.textContent         = 'Sign In';
  } else {
    if (tabReg)    tabReg.style.cssText    = ACTIVE_STYLE;
    if (tabLogin)  tabLogin.style.cssText  = INACTIVE_STYLE;
    if (nameField) nameField.style.display = 'block';
    if (btn)       btn.textContent         = 'Create Account';
  }

  // Clear error on mode switch
  if (errEl) {
    errEl.textContent = '';
    errEl.classList.remove('visible');
  }
}

async function _handleAuthSubmit() {
  const email    = $('#authEmail')?.value?.trim()    || '';
  const password = $('#authPass')?.value              || '';
  const fullName = $('#displayName')?.value?.trim()  || '';

  const errEl    = $('#authError');
  const btn      = $('#authBtn');
  const iconI    = $('#authIconI');
  const iconWrap = $('#authIcon');

  // Client-side validation
  if (!isValidEmail(email)) {
    return _showAuthError('Enter a valid email address.');
  }
  if (!isValidPassword(password)) {
    return _showAuthError('Password must be at least 6 characters.');
  }
  if (authMode === 'register' && !fullName) {
    return _showAuthError('Full name is required.');
  }

  // Reset error
  if (errEl) {
    errEl.textContent = '';
    errEl.classList.remove('visible');
  }

  // Loading state
  _setAuthLoading(true, btn, iconI, iconWrap);

  // Wake-up counter for Render cold start
  let secs = 0;
  wakeInterval = setInterval(() => {
    secs++;
    if (btn && !btn.disabled) return;
    if (secs >= 8 && btn) {
      btn.innerHTML = `<span class="spinner"></span>&nbsp;Server waking… ${secs}s`;
    }
  }, 1000);

  try {
    let data;
    if (authMode === 'login') {
      data = await login(email, password);
    } else {
      data = await register(email, password, fullName);
    }

    if (!data?.token) {
      throw new ApiError('Authentication failed — no token returned.');
    }

    // Persist session
    persistSession(data.token, data.user);
    store.setMany({ token: data.token, user: data.user });

    // Success animation
    _setAuthSuccess(btn, iconI, iconWrap);
    await sleep(700);

    // Pre-fill org form name if registering
    if (authMode === 'register' && fullName) {
      const orgName = $('#orgFullName');
      if (orgName) orgName.value = fullName;
    }

    // Route based on org existence
    const org = (() => {
      try {
        const raw = localStorage.getItem(`prexus_org_${email}`);
        return raw ? JSON.parse(raw) : null;
      } catch { return null; }
    })();

    if (org) {
      store.set('org', org);
      _setHubOrgName(org.orgName || data.user?.org_name || email);
      _preloadAssetCount();
      navigate('hub');
    } else {
      navigate('org');
    }

  } catch (err) {
    let msg;
    if (err instanceof ApiError) {
      if (err.isTimeout) msg = '⏱ Server is waking up — please retry in a moment.';
      else if (err.isNetwork) msg = '⚠ Cannot reach server. Check connection and retry.';
      else if (err.isConflict) msg = '✕ Email already registered. Try signing in.';
      else if (err.isUnauth) msg = '✕ Invalid email or password.';
      else msg = `✕ ${err.message}`;
    } else {
      msg = '✕ Unexpected error. Please try again.';
    }
    _showAuthError(msg);
  } finally {
    clearInterval(wakeInterval);
    _setAuthLoading(false, btn, iconI, iconWrap);
  }
}

/* ── Auth UI helpers ─────────────────────────────────────── */

function _setAuthLoading(loading, btn, iconI, iconWrap) {
  if (!btn) return;
  btn.disabled = loading;
  if (loading) {
    btn.innerHTML = `<span class="spinner"></span>&nbsp;Authenticating…`;
    if (iconI) {
      iconI.className = 'fa-solid fa-circle-notch anim-spin';
      iconI.style.animation = '';
    }
  } else {
    btn.innerHTML = authMode === 'login' ? 'Sign In' : 'Create Account';
    if (iconI) {
      iconI.className = 'fa-solid fa-lock';
      iconI.style.animation = '';
      iconI.style.color = 'var(--cobalt)';
    }
    if (iconWrap) {
      iconWrap.classList.remove('unlocked');
    }
  }
}

function _setAuthSuccess(btn, iconI, iconWrap) {
  if (btn) {
    btn.innerHTML = '<i class="fa-solid fa-circle-check"></i>&nbsp;Verified — Redirecting';
    btn.classList.add('success');
  }
  if (iconWrap) iconWrap.classList.add('unlocked');
  if (iconI) {
    iconI.className = 'fa-solid fa-unlock';
    iconI.style.color = '';
  }
}

function _showAuthError(message) {
  const errEl = $('#authError');
  if (!errEl) return;
  errEl.textContent = message;
  errEl.classList.add('visible');
}

/* ══════════════════════════════════════════════════════════
   ORG ONBOARDING FORM
══════════════════════════════════════════════════════════ */

const TERM_LINES = [
  '[SYSTEM]     Initializing TLS 1.3 secure handshake...',
  '[ENTITY]     Provisioning Organization Object · ORG_PENDING',
  '[ENTITY]     Provisioning Administrator · USR_ADMIN_001',
  '[RBAC]       Assigning policy ORG_ADMIN → USR_ADMIN_001',
  '[NETWORK]    Binding email domain to Organization Workspace',
  '[COMPLIANCE] Validating against OFAC, EU & UN sanctions registries...',
  '[COMPLIANCE] Validating against EAR/ITAR export control lists...',
  '[IDENTITY]   Creating cryptographic identity anchor...',
  '[STATE]      Organization status → PENDING_VERIFICATION (B2G §3.2)',
  '[AUDIT]      EVENT_LOGGED · ORG_CREATED · auth_rep',
  '[SYSTEM]     Establishing Prexus Ecosystem Environment...',
  '[SYSTEM]  ✓  Access Granted. Welcome to Prexus.',
];

function _bindOrgForm() {
  const form = $('#orgForm');
  if (!form) return;

  form.addEventListener('submit', (e) => {
    e.preventDefault();
    _handleOrgSubmit();
  });
}

function _handleOrgSubmit() {
  const user = store.get('user');

  const orgData = {
    orgName:    $('#orgName')?.value      || '',
    orgType:    $('#orgType')?.value      || '',
    country:    $('#orgCountry')?.value   || '',
    domain:     $('#orgDomain')?.value    || '',
    fullName:   $('#orgFullName')?.value  || '',
    jobTitle:   $('#orgJobTitle')?.value  || '',
    department: $('#orgDept')?.value      || '',
    email:      user?.email              || '',
    createdAt:  new Date().toISOString(),
  };

  // Persist org
  persistOrg(orgData);

  // Update user org_name locally
  if (user) {
    const updated = { ...user, org_name: orgData.orgName };
    store.set('user', updated);
    try { localStorage.setItem('prx_user', JSON.stringify(updated)); } catch {}
  }

  // Run terminal animation
  _runTerminalAnimation(orgData);
}

async function _runTerminalAnimation(orgData) {
  const overlay = $('#termOverlay');
  const lines   = $('#termLines');
  if (!overlay || !lines) {
    // Fallback: skip animation
    _completeOrgOnboarding(orgData);
    return;
  }

  overlay.style.display = 'flex';
  lines.innerHTML = '';

  for (const line of TERM_LINES) {
    await sleep(380);
    const d = document.createElement('div');
    d.className = 'fi';
    d.style.cssText = 'color:hsl(160,68%,60%);display:flex;gap:10px;margin-bottom:8px';
    d.innerHTML = `<span style="color:hsl(215,15%,25%);user-select:none;flex-shrink:0">›</span><span style="font-size:11px;font-family:var(--font-data)">${line}</span>`;
    lines.appendChild(d);
    // Auto-scroll terminal
    const body = lines.parentElement;
    if (body) body.scrollTop = body.scrollHeight;
  }

  await sleep(900);
  overlay.style.display = 'none';
  _completeOrgOnboarding(orgData);
}

function _completeOrgOnboarding(orgData) {
  _setHubOrgName(orgData.orgName);
  navigate('hub');
}

/* ══════════════════════════════════════════════════════════
   LOGOUT
══════════════════════════════════════════════════════════ */

/**
 * Log out the current user.
 * Clears session, returns to auth page.
 */
export function logout() {
  // Unmount React if Meteorium was mounted
  if (window._meteoriumRoot) {
    try { window._meteoriumRoot.unmount(); } catch {}
    window._meteoriumRoot = null;
  }
  const root = document.getElementById('meteorium-root');
  if (root) root.innerHTML = '';

  // Clear localStorage
  try {
    localStorage.removeItem('prx_token');
    localStorage.removeItem('prx_user');
  } catch {}

  store.setMany({
    token: null,
    user:  null,
    org:   null,
    assets: [],
    selectedAsset: null,
  });

  navigate('auth');

  // Reset auth form state
  setTimeout(() => {
    setAuthMode('login');
    const errEl = $('#authError');
    if (errEl) { errEl.textContent = ''; errEl.classList.remove('visible'); }
  }, 300);
}

/* ══════════════════════════════════════════════════════════
   COOKIE BANNER
══════════════════════════════════════════════════════════ */

function _initCookieBanner() {
  const banner = $('#cookieBanner');
  if (!banner) return;

  if (localStorage.getItem('prexus_cookies')) {
    banner.classList.add('hidden');
    return;
  }

  const acceptBtn  = $('#cookieAccept');
  const declineBtn = $('#cookieDecline');

  acceptBtn?.addEventListener('click',  () => _dismissCookies(true));
  declineBtn?.addEventListener('click', () => _dismissCookies(false));
}

function _dismissCookies(accepted) {
  const banner = $('#cookieBanner');
  if (banner) banner.classList.add('hidden');
  localStorage.setItem('prexus_cookies', accepted ? 'accepted' : 'declined');
  if (accepted && !localStorage.getItem('prexus_device_id')) {
    localStorage.setItem('prexus_device_id', generateDeviceId());
  }
}

/* ══════════════════════════════════════════════════════════
   HUB HELPERS
══════════════════════════════════════════════════════════ */

function _setHubOrgName(name) {
  const el = document.getElementById('hubOrgName');
  if (el) el.textContent = name || '—';
}

async function _preloadAssetCount() {
  try {
    const assets = await getAssets();
    if (Array.isArray(assets)) {
      store.set('assets', assets);
      const el = document.getElementById('hubAssetCount');
      if (el) el.textContent = assets.length;

      // Set last run display
      const lastRun = document.getElementById('hubLastRun');
      if (lastRun && assets.length > 0) {
        lastRun.textContent = new Date().toISOString().slice(11,16) + 'Z';
      }
    }
  } catch {
    // Silent — hub still shows, just with 0 count
  }
}

