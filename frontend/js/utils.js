/**
 * js/utils.js
 * Prexus Intelligence — Utility Functions
 * THE GREAT FILE · Phase 1
 *
 * Pure functions only. No side effects. No DOM access.
 * Formatters, risk color/label logic, math helpers.
 */

/* ── Number formatters ───────────────────────────────────── */

/** Format as percentage: 0.71 → "71.0%" */
export function fPct(v) {
  return `${((v || 0) * 100).toFixed(1)}%`;
}

/** Format as USD: 450 → "$450M", 1500 → "$1.5B" */
export function fUsd(v) {
  if (!v) return '$0M';
  return v >= 1000
    ? `$${(v / 1000).toFixed(1)}B`
    : `$${v.toFixed(0)}M`;
}

/** Format as USD millions: 42300000 → "$42.3M" */
export function fUsdM(v) {
  return `$${((v || 0) / 1e6).toFixed(1)}M`;
}

/** Round to N decimal places */
export function round(v, n = 2) {
  return Math.round((v || 0) * 10 ** n) / 10 ** n;
}

/* ── Risk color / label / class ──────────────────────────── */

/**
 * Returns CSS color string for a risk score 0–1.
 * Maps to the 5-tier risk system from Layer 5.
 */
export function riskColor(score) {
  if (score >= 0.85) return 'var(--red)';
  if (score >= 0.65) return 'var(--amber)';
  if (score >= 0.45) return '#F97316';     /* orange */
  if (score >= 0.25) return 'var(--cobalt)';
  return 'var(--green)';
}

/**
 * Returns text label for risk score.
 */
export function riskLabel(score) {
  if (score >= 0.85) return 'CRITICAL';
  if (score >= 0.65) return 'HIGH';
  if (score >= 0.45) return 'ELEVATED';
  if (score >= 0.25) return 'MODERATE';
  return 'LOW';
}

/**
 * Returns CSS class suffix for risk score.
 * Used as: `decision ${riskClass(score)}`
 */
export function riskClass(score) {
  if (score >= 0.85) return 'critical';
  if (score >= 0.65) return 'high';
  if (score >= 0.45) return 'elevated';
  return 'nominal';
}

/**
 * Returns tag CSS class for severity string.
 */
export function severityTagClass(sev) {
  switch (sev?.toUpperCase()) {
    case 'CRITICAL': return 'tag-red';
    case 'HIGH':     return 'tag-amber';
    case 'ELEVATED': return 'tag-cobalt';
    default:         return 'tag-green';
  }
}

/* ── DOM helpers ─────────────────────────────────────────── */

/**
 * Query selector shorthand.
 */
export function $(selector, parent = document) {
  return parent.querySelector(selector);
}

export function $$(selector, parent = document) {
  return Array.from(parent.querySelectorAll(selector));
}

/**
 * Create an element with attributes and text.
 */
export function el(tag, attrs = {}, text = '') {
  const element = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') element.className = v;
    else if (k === 'style') element.style.cssText = v;
    else element.setAttribute(k, v);
  }
  if (text) element.textContent = text;
  return element;
}

/* ── Time ────────────────────────────────────────────────── */

/**
 * Returns current UTC time string: "14:32:07 UTC"
 */
export function utcTime() {
  return new Date().toISOString().slice(11, 19) + ' UTC';
}

/**
 * Returns current UTC date string: "2026·03·16"
 */
export function utcDate() {
  return new Date().toISOString().slice(0, 10).replace(/-/g, '·');
}

/**
 * Start a live clock. Returns cleanup function.
 * @param {Function} onTick - called every second with { time, date }
 */
export function startClock(onTick) {
  const tick = () => onTick({ time: utcTime(), date: utcDate() });
  tick();
  const id = setInterval(tick, 1000);
  return () => clearInterval(id);
}

/* ── Validation ──────────────────────────────────────────── */

export function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

export function isValidPassword(password) {
  return typeof password === 'string' && password.length >= 6;
}

/* ── Misc ────────────────────────────────────────────────── */

/**
 * Debounce a function.
 */
export function debounce(fn, ms = 300) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}

/**
 * Sleep for ms milliseconds.
 */
export function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Generate a device ID.
 */
export function generateDeviceId() {
  return 'DEV-' + crypto.randomUUID();
}

