/**
 * modules/meteorium/dashboard.js
 * Prexus Intelligence — Global Intelligence Dashboard
 * THE GREAT FILE · Phase 2
 *
 * FIXES:
 *  - XSS: sanitizeHTML() on asset fields in _miniCard(), _compoundCard(), _bindMapClicks()
 *  - normalizeAsset() + clamp() — prevents crash on bad API data
 *  - _worldMap() now uses normalized assets, not SIM_ASSETS (map was lying)
 *  - Priority Decisions sorted by assetPriority() score, not raw cr
 */

import { store } from '../../js/store.js';
import { scorePortfolio } from '../../js/api.js';
import { fPct, fUsd, riskColor, riskLabel, riskClass, sanitizeHTML } from '../../js/utils.js';
import { updateTopbar } from './meteorium.js';

const SIM_ASSETS = [
  { id:'MUM-INF-001', name:'Mumbai Port Terminal',     lat:18.93,  lon:72.83,   cr:0.71, pr:0.68, tr:0.44 },
  { id:'DEL-ENE-002', name:'Delhi Power Grid',          lat:28.61,  lon:77.20,   cr:0.58, pr:0.55, tr:0.51 },
  { id:'MUM-FIN-003', name:'BKC Financial Complex',     lat:19.06,  lon:72.87,   cr:0.42, pr:0.38, tr:0.56 },
  { id:'CHE-MFG-004', name:'Chennai Auto Cluster',      lat:13.08,  lon:80.27,   cr:0.65, pr:0.71, tr:0.38 },
  { id:'SGP-TRN-005', name:'Singapore PSA Terminal',    lat:1.27,   lon:103.82,  cr:0.33, pr:0.28, tr:0.42 },
  { id:'LON-FIN-006', name:'Canary Wharf Finance Hub',  lat:51.51,  lon:-0.02,   cr:0.28, pr:0.24, tr:0.38 },
  { id:'SHA-MFG-007', name:'Shanghai Industrial Zone',  lat:31.23,  lon:121.47,  cr:0.61, pr:0.66, tr:0.48 },
  { id:'SAO-AGR-008', name:'São Paulo Agri Hub',        lat:-23.55, lon:-46.63,  cr:0.87, pr:0.91, tr:0.35 },
];

const SIM_COMPOUNDS = [
  {
    type:'fire_climate_compound', sev:'CRITICAL', location:'São Paulo Basin, Brazil',
    desc:'Co-occurring wildfire risk (87%), drought (0.82), and heat stress (74%). Satellite-confirmed vegetation collapse NDVI 0.18.',
    amp:3.1,
    chain:[
      {label:'Temp Anomaly',    val:0.74, color:'#F59E0B'},
      {label:'Drought Index',   val:0.82, color:'#F97316'},
      {label:'Fire Probability',val:0.87, color:'#EF4444'},
    ],
  },
  {
    type:'drought_heat_nexus', sev:'HIGH', location:'Deccan Plateau, India',
    desc:'Drought index 0.67 and heat stress 0.58 across manufacturing zones. Monsoon deficit −31% vs ERA5 baseline.',
    amp:1.9,
    chain:[
      {label:'Heat Stress',  val:0.58, color:'#F59E0B'},
      {label:'Drought Index',val:0.67, color:'#F97316'},
    ],
  },
];

// ── Data normalization ────────────────────────────────────────────────────────
function clamp(v) { v = Number(v); return isNaN(v) ? 0 : Math.min(Math.max(v, 0), 1); }

function normalizeAsset(a) {
  return {
    id:   a.id   || crypto.randomUUID(),
    name: a.name || 'Unknown Asset',
    lat:  Number(a.lat) || 0,
    lon:  Number(a.lon) || 0,
    cr:   clamp(a.cr),
    pr:   clamp(a.pr),
    tr:   clamp(a.tr),
    value_mm: Number(a.value_mm) || 0,
    cc:   a.cc || '',
    type: a.type || '',
    alerts: Number(a.alerts) || 0,
  };
}

// Weighted priority score — drives Priority Decisions ranking
function assetPriority(a) {
  return (a.cr * 0.6) + (a.pr * 0.25) + (a.tr * 0.15);
}

const SIM_PORTFOLIO = { composite_risk:0.52, var_95:0.19, cvar_95:0.28, loss_expected_mm:141.6, total_value_mm:6070 };

export async function init(container) {
  const assets     = (store.get('assets') || SIM_ASSETS).map(normalizeAsset);
  const alertCount = assets.filter(a => a.cr >= 0.65).length;
  updateTopbar({ alertCount, signalCount: 12, portfolioRisk: SIM_PORTFOLIO.composite_risk });

  // Priority list — algorithmic score, not raw cr
  const prioritized = [...assets]
    .map(a => ({ ...a, _score: assetPriority(a) }))
    .sort((a, b) => b._score - a._score);

  container.innerHTML = `
    <div class="met-kpi-grid">
      ${_kpi('Portfolio Exposure', fUsd(SIM_PORTFOLIO.total_value_mm), `${assets.length} assets`, 'cobalt')}
      ${_kpi('Composite Risk', fPct(SIM_PORTFOLIO.composite_risk), 'Value-weighted · IPCC AR6', 'amber')}
      ${_kpi('Active Alerts', String(alertCount), 'Critical decisions required', alertCount > 0 ? 'red' : 'cobalt')}
      ${_kpi('Compound Events', String(SIM_COMPOUNDS.length), 'Multi-hazard chains active', 'amber')}
    </div>

    <div style="display:grid;grid-template-columns:1fr 280px;gap:12px;margin-bottom:12px">
      <div class="panel" style="overflow:hidden">
        <div class="panel-head">
          <span class="panel-title">Geospatial Risk Distribution · WGS-84</span>
          <div style="margin-left:auto;display:flex;gap:10px;align-items:center">
            ${[['var(--red)','Critical'],['var(--amber)','High'],['var(--cobalt)','Elevated'],['var(--green)','Low']]
              .map(([c,l])=>`<div style="display:flex;align-items:center;gap:3px"><div style="width:5px;height:5px;border-radius:50%;background:${c}"></div><span style="font-size:7.5px;color:var(--text-muted)">${l}</span></div>`).join('')}
          </div>
        </div>
        ${_worldMap(assets)}
        <div id="dash-detail" style="display:none"></div>
      </div>

      <div class="panel" style="display:flex;flex-direction:column;overflow:hidden">
        <div class="panel-head">
          <span class="panel-title">Priority Decisions</span>
          <div style="margin-left:auto"><span class="tag tag-red">${alertCount} HIGH+</span></div>
        </div>
        <div style="flex:1;overflow-y:auto">
          ${prioritized.filter(a => a.cr >= 0.45).slice(0, 6).map(_miniCard).join('')}
        </div>
      </div>
    </div>

    <div style="margin-bottom:12px">
      <div style="font-size:8px;letter-spacing:.18em;text-transform:uppercase;color:var(--text-muted);margin-bottom:8px">
        ⚡ Active Compound Events — Risk Chain Visualization
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
        ${SIM_COMPOUNDS.map(_compoundCard).join('')}
      </div>
    </div>

    <div class="panel">
      <div class="panel-head">
        <span class="panel-title">Portfolio Risk Metrics · Monte Carlo N=5,000</span>
        <div style="margin-left:auto"><span class="tag tag-green">SSP2-4.5 BASELINE</span></div>
      </div>
      <div id="dash-metrics" style="padding:12px 14px">
        <div style="display:flex;align-items:center;gap:6px;color:var(--text-muted);font-size:10px">
          <span class="spinner"></span> Computing…
        </div>
      </div>
    </div>`;

  _bindMapClicks(container, assets);
  _loadPortfolio();
}

export function destroy() {}

function _worldMap(assets) {
  const W=800, H=360;
  const proj=(lat,lon)=>[((lon+180)/360)*W, ((90-lat)/180)*H];
  const continents=[
    'M60,50 L160,50 L180,80 L170,120 L140,150 L120,180 L90,170 L60,130 L50,90 Z',
    'M120,185 L155,185 L165,220 L160,270 L140,310 L120,310 L108,260 L110,220 Z',
    'M290,40 L350,35 L370,55 L355,75 L330,80 L295,70 Z',
    'M290,90 L360,85 L380,120 L385,180 L370,240 L340,280 L300,275 L280,230 L275,160 L280,110 Z',
    'M365,30 L560,25 L590,60 L600,100 L570,130 L510,140 L450,135 L410,120 L370,100 L355,65 Z',
    'M510,230 L580,225 L590,260 L570,290 L530,295 L505,265 Z',
  ];
  const grads = assets.map(a=>{
    const raw=riskColor(a.cr);
    const c=raw.replace('var(--red)','#EF4444').replace('var(--amber)','#F59E0B').replace('var(--cobalt)','#0EA5E9').replace('var(--green)','#10B981').replace('#F97316','#F97316');
    return `<radialGradient id="dg-${a.id}"><stop offset="0%" stop-color="${c}" stop-opacity=".7"/><stop offset="100%" stop-color="${c}" stop-opacity="0"/></radialGradient>`;
  }).join('');
  const dots = assets.map(a=>{
    const [x,y]=proj(a.lat,a.lon);
    const c=riskColor(a.cr).replace('var(--red)','#EF4444').replace('var(--amber)','#F59E0B').replace('var(--cobalt)','#0EA5E9').replace('var(--green)','#10B981').replace('#F97316','#F97316');
    return `<g style="cursor:pointer" data-asset="${a.id}" class="dash-asset-dot">
      <circle cx="${x}" cy="${y}" r="10" fill="url(#dg-${a.id})" opacity=".4"/>
      <circle cx="${x}" cy="${y}" r="4.5" fill="${c}" stroke="rgba(0,0,0,.5)" stroke-width=".8" style="filter:drop-shadow(0 0 5px ${c})"/>
    </g>`;
  }).join('');
  const latLines=[-60,-30,0,30,60].map(lat=>{const[,y]=proj(lat,0);return`<line x1="0" y1="${y}" x2="${W}" y2="${y}" stroke="rgba(14,165,233,.05)" stroke-width=".5"/>`;}).join('');
  const lonLines=[-120,-60,0,60,120].map(lon=>{const[x]=proj(0,lon);return`<line x1="${x}" y1="0" x2="${x}" y2="${H}" stroke="rgba(14,165,233,.05)" stroke-width=".5"/>`;}).join('');
  const eq=()=>{const[,y]=proj(0,0);return`<line x1="0" y1="${y}" x2="${W}" y2="${y}" stroke="rgba(14,165,233,.12)" stroke-width=".8" stroke-dasharray="4 6"/>`;};
  return `<div style="position:relative;height:260px;background:rgba(0,0,0,.3)">
    <svg viewBox="0 0 ${W} ${H}" style="width:100%;height:100%" id="dash-map">
      <defs>${grads}</defs>
      ${latLines}${lonLines}${eq()}
      ${continents.map(d=>`<path d="${d}" fill="rgba(14,165,233,.055)" stroke="rgba(14,165,233,.13)" stroke-width=".8"/>`).join('')}
      ${dots}
    </svg>
  </div>`;
}

function _miniCard(a) {
  const c=riskColor(a.cr), sev=riskLabel(a.cr);
  const bg    = sev==='CRITICAL'?'rgba(239,68,68,.04)':sev==='HIGH'?'rgba(245,158,11,.04)':'var(--cobalt-lo)';
  const bdg_bg= sev==='CRITICAL'?'rgba(239,68,68,.2)':sev==='HIGH'?'rgba(245,158,11,.2)':'var(--cobalt-mid)';
  const bdg_tx= sev==='CRITICAL'?'#fca5a5':sev==='HIGH'?'#fcd34d':'var(--cobalt)';
  const bdg_bd= sev==='CRITICAL'?'rgba(239,68,68,.3)':sev==='HIGH'?'rgba(245,158,11,.3)':'var(--cobalt-hi)';
  // FIX: sanitize asset fields
  const safeId   = sanitizeHTML(a.id   || '');
  const safeName = sanitizeHTML(a.name || '');
  return `<div style="padding:8px 12px;border-bottom:1px solid var(--border);border-left:2px solid ${c};background:${bg}">
    <div style="display:flex;align-items:center;gap:5px;margin-bottom:3px">
      <span style="font-size:7.5px;font-weight:700;letter-spacing:.12em;padding:1px 5px;border-radius:2px;text-transform:uppercase;background:${bdg_bg};color:${bdg_tx};border:1px solid ${bdg_bd}">${sev}</span>
      <span style="font-size:9px;color:var(--text-primary)">${safeId}</span>
      <span style="margin-left:auto;font-family:var(--font-display);font-size:15px;color:${c}">${fPct(a.cr)}</span>
    </div>
    <div style="font-size:9px;color:var(--text-secondary);line-height:1.4">${safeName}</div>
    <div style="margin-top:4px"><div class="met-risk-bar"><div class="met-risk-fill" style="width:${a.cr*100}%;background:${c}"></div></div></div>
  </div>`;
}

function _compoundCard(ev) {
  const isCrit  = ev.sev==='CRITICAL';
  const borderC = isCrit?'rgba(239,68,68,.2)':'rgba(245,158,11,.2)';
  const bgC     = isCrit?'rgba(239,68,68,.05)':'rgba(245,158,11,.05)';
  const titleC  = isCrit?'var(--red)':'var(--amber)';
  const badgeBg = isCrit?'rgba(239,68,68,.2)':'rgba(245,158,11,.2)';
  const badgeTx = isCrit?'#fca5a5':'#fcd34d';
  const badgeBd = isCrit?'rgba(239,68,68,.3)':'rgba(245,158,11,.3)';
  // FIX: sanitize compound event fields
  const safeSev      = sanitizeHTML(ev.sev      || '');
  const safeLocation = sanitizeHTML(ev.location || '');
  const safeDesc     = sanitizeHTML(ev.desc     || '');
  const safeType     = sanitizeHTML((ev.type||'').replace(/_/g,' '));
  const chainHTML=ev.chain.map((n,i)=>`
    <div class="met-cascade-node">
      <div class="met-cascade-dot" style="background:${n.color};box-shadow:0 0 5px ${n.color}"></div>
      <span class="met-cascade-label">${sanitizeHTML(n.label)}</span>
      <span class="met-cascade-val" style="color:${n.color}">${fPct(n.val)}</span>
    </div>
    ${i<ev.chain.length-1?'<div class="met-cascade-arrow">↓</div>':''}`).join('');
  return `<div style="background:${bgC};border:1px solid ${borderC};border-radius:3px;padding:12px 14px">
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
      <span style="font-size:8px;font-weight:700;letter-spacing:.14em;padding:2px 6px;border-radius:2px;text-transform:uppercase;background:${badgeBg};color:${badgeTx};border:1px solid ${badgeBd}">${safeSev}</span>
      <span style="font-size:9px;color:var(--text-secondary)">${safeLocation}</span>
    </div>
    <div style="font-family:var(--font-display);font-size:14px;letter-spacing:.08em;text-transform:uppercase;color:${titleC};margin-bottom:4px">${safeType}</div>
    <div style="font-family:var(--font-serif);font-size:11px;color:var(--text-secondary);line-height:1.55;font-style:italic;margin-bottom:10px">${safeDesc}</div>
    <div class="met-cascade">
      <div class="met-cascade-title">Risk Propagation Chain</div>
      <div class="met-cascade-chain">${chainHTML}</div>
      <div class="met-cascade-amp">
        <span class="met-cascade-amp-label">Compound Amplifier</span>
        <span class="met-cascade-amp-val" style="color:${titleC}">${ev.amp.toFixed(1)}×</span>
      </div>
    </div>
  </div>`;
}

function _kpi(label,value,sub,cls) {
  return `<div class="met-kpi ${cls}"><div class="met-kpi-label">${label}</div><div class="met-kpi-value">${value}</div><div class="met-kpi-sub">${sub}</div></div>`;
}

async function _loadPortfolio() {
  const el = document.getElementById('dash-metrics');
  if (!el) return;
  let p = SIM_PORTFOLIO;
  try {
    const assets = store.get('assets') || SIM_ASSETS;
    const r = await scorePortfolio(assets, 'baseline');
    if (r?.portfolio_composite_risk != null) p = {
      composite_risk:   r.portfolio_composite_risk,
      var_95:           r.portfolio_var_95,
      cvar_95:          r.portfolio_cvar_95,
      loss_expected_mm: r.loss_expected_mm,
      total_value_mm:   r.total_value_mm,
    };
  } catch {}
  updateTopbar({ portfolioRisk: p.composite_risk });
  el.innerHTML = `<div style="display:grid;grid-template-columns:repeat(6,1fr);gap:14px">
    ${[
      ['Total Exposure', fUsd(p.total_value_mm),         'var(--cobalt)'],
      ['Composite Risk', fPct(p.composite_risk),         riskColor(p.composite_risk)],
      ['VaR 95%',        fPct(p.var_95),                 'var(--amber)'],
      ['CVaR 95%',       fPct(p.cvar_95),                'var(--red)'],
      ['Expected Loss',  fUsd(p.loss_expected_mm),        'var(--red)'],
      ['Scenario',       'SSP2-4.5',                     'var(--green)'],
    ].map(([l,v,c])=>`<div>
      <div style="font-size:7.5px;color:var(--text-muted);letter-spacing:.1em;text-transform:uppercase;margin-bottom:3px">${l}</div>
      <div style="font-family:var(--font-display);font-size:20px;color:${c};line-height:1">${v}</div>
    </div>`).join('')}
  </div>`;
}

function _bindMapClicks(container, assets) {
  container.addEventListener('click', e => {
    const dot = e.target.closest('.dash-asset-dot');
    if (!dot) return;
    const a = assets.find(x => x.id === dot.dataset.asset);
    if (!a) return;
    const el = document.getElementById('dash-detail');
    if (!el) return;
    el.style.display = 'block';
    const c = riskColor(a.cr);
    // FIX: sanitize asset name and ID before injecting
    const safeName = sanitizeHTML(a.name || '');
    const safeId   = sanitizeHTML(a.id   || '');
    el.innerHTML = `<div style="padding:10px 14px;border-top:1px solid var(--border);background:rgba(0,0,0,.3);display:flex;align-items:center;gap:16px">
      <div>
        <div style="font-family:var(--font-display);font-size:14px;color:white;letter-spacing:.08em">${safeName}</div>
        <div style="font-size:8.5px;color:var(--text-secondary)">${safeId} · ${a.lat.toFixed(2)}°, ${a.lon.toFixed(2)}°</div>
      </div>
      <div style="margin-left:auto;display:flex;gap:18px">
        ${[['Composite',a.cr],['Physical',a.pr],['Transition',a.tr]].map(([l,v])=>`<div>
          <div style="font-size:7px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.1em">${l}</div>
          <div style="font-family:var(--font-display);font-size:18px;color:${riskColor(v)}">${fPct(v)}</div>
        </div>`).join('')}
      </div>
      <button id="dash-detail-close" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:14px;padding:0 4px">✕</button>
    </div>`;
    // FIX: close button uses event listener, not inline onclick
    el.querySelector('#dash-detail-close')?.addEventListener('click', () => { el.style.display='none'; });
  });
}
