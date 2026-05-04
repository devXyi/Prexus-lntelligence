/**
 * modules/meteorium/sources.js
 * Prexus Intelligence — Data Source Registry
 * THE GREAT FILE · Phase 2
 *
 * FIXES:
 *  - XSS: sanitizeHTML() on all source fields injected into innerHTML
 *  - normalizeSource() — future-proofs against backend field drift
 *  - Status-first sort (offline → degraded → nominal)
 *  - Latency color intelligence
 */

import { listSources } from '../../js/api.js';
import { sanitizeHTML } from '../../js/utils.js';

const SIM=[
  {name:'Open-Meteo ECMWF Forecast',     agency:'ECMWF / Open-Meteo', type:'Weather',    latency:1,    status:'nominal', key:false,format:'JSON',    resolution_km:9},
  {name:'ERA5 Historical Reanalysis',     agency:'ECMWF / Open-Meteo', type:'Climate',    latency:120,  status:'nominal', key:false,format:'JSON',    resolution_km:31},
  {name:'NASA FIRMS VIIRS 375m',          agency:'NASA EOSDIS',         type:'Fire',       latency:3,    status:'nominal', key:true, format:'CSV',     resolution_km:0.375},
  {name:'NASA FIRMS MODIS 1km',           agency:'NASA EOSDIS',         type:'Fire',       latency:6,    status:'nominal', key:true, format:'CSV',     resolution_km:1},
  {name:'Carbon Monitor CO₂',            agency:'Carbon Monitor',      type:'Emissions',  latency:120,  status:'nominal', key:false,format:'JSON',    resolution_km:9999},
  {name:'ERA5 via Copernicus CDS',        agency:'ECMWF / Copernicus',  type:'Climate',    latency:120,  status:'nominal', key:true, format:'NetCDF',  resolution_km:31},
  {name:'Sentinel Hub ESA Copernicus',    agency:'ESA / Copernicus',    type:'Satellite',  latency:24,   status:'degraded',key:true, format:'GeoTIFF', resolution_km:0.01},
  {name:'Planet Labs PlanetScope',        agency:'Planet Labs',          type:'Satellite',  latency:24,   status:'offline', key:true, format:'GeoTIFF', resolution_km:0.003},
  {name:'Microsoft Planetary Computer',  agency:'Microsoft / STAC',    type:'Catalog',    latency:24,   status:'nominal', key:false,format:'COG',     resolution_km:0.01},
  {name:'CMIP6 Climate Projections',      agency:'IPCC / WCRP',         type:'Scenarios',  latency:0,    status:'nominal', key:false,format:'NetCDF',  resolution_km:111},
  {name:'NASA SRTM Digital Elevation',    agency:'NASA / USGS',         type:'Elevation',  latency:0,    status:'nominal', key:false,format:'GeoTIFF', resolution_km:0.03},
  {name:'JAXA GSMaP Precipitation',       agency:'JAXA',                type:'Precip',     latency:4,    status:'nominal', key:true, format:'NetCDF',  resolution_km:11.1},
  {name:'NASA GPM IMERG',                 agency:'NASA',                type:'Precip',     latency:6,    status:'nominal', key:true, format:'HDF5',    resolution_km:11.1},
  {name:'ISRO INSAT-3D/3DR',              agency:'ISRO / IMD',          type:'Weather',    latency:1,    status:'nominal', key:true, format:'HDF5',    resolution_km:4},
  {name:'Copernicus Marine Service',      agency:'Copernicus / Mercator',type:'Ocean',     latency:24,   status:'nominal', key:true, format:'NetCDF',  resolution_km:9},
  {name:'NASA GRACE-FO Groundwater',      agency:'NASA / GFZ',          type:'Groundwater',latency:1440, status:'nominal', key:true, format:'NetCDF',  resolution_km:330},
  {name:'Global Forest Watch',            agency:'WRI / Hansen / UMD',  type:'Land Cover', latency:8760, status:'nominal', key:false,format:'GeoTIFF', resolution_km:0.03},
  {name:'NOAA GFS Global Forecast',       agency:'NOAA / NCEP',         type:'Weather',    latency:4,    status:'nominal', key:false,format:'GRIB2',   resolution_km:27.8},
  {name:'ISRO Resourcesat-2A LISS',       agency:'ISRO / NRSC',         type:'Satellite',  latency:48,   status:'nominal', key:true, format:'GeoTIFF', resolution_km:0.024},
];

// ── Data normalization ────────────────────────────────────────────────────────
function normalizeSource(s) {
  return {
    name:         s.name         || 'Unknown',
    agency:       s.agency       || 'Unknown',
    type:         s.type         || 'Unknown',
    status:       s.status       || 'nominal',
    latency:      s.latency      ?? s.latency_hours ?? 0,
    resolution_km:s.resolution_km ?? s.resolution   ?? 0,
    format:       s.format       || '—',
    requires_key: s.key          || s.requires_key  || false,
  };
}

export async function init(container) {
  container.innerHTML=`<div style="text-align:center;padding:20px;color:var(--text-muted);font-size:10px"><span class="spinner"></span>&nbsp;Loading registry…</div>`;
  let sources = SIM;
  try { const live = await listSources(); if (live?.sources?.length) sources = live.sources; } catch {}
  // Normalize + sort: problems first, then by latency
  const STATUS_WEIGHT = { offline: 3, degraded: 2, nominal: 1 };
  sources = sources
    .map(normalizeSource)
    .sort((a, b) => (STATUS_WEIGHT[b.status] || 0) - (STATUS_WEIGHT[a.status] || 0) || a.latency - b.latency);
  _render(container, sources);
}

export function destroy(){}

function _render(container,sources){
  const nom  = sources.filter(s=>s.status==='nominal').length;
  const deg  = sources.filter(s=>s.status==='degraded').length;
  const off  = sources.filter(s=>s.status==='offline').length;
  const free = sources.filter(s=>!s.key&&!s.requires_key).length;

  container.innerHTML=`
    <div class="met-kpi-grid" style="margin-bottom:14px">
      <div class="met-kpi cobalt"><div class="met-kpi-label">Total Sources</div><div class="met-kpi-value">${sources.length}</div><div class="met-kpi-sub">Layer 0 registry</div></div>
      <div class="met-kpi green"><div class="met-kpi-label">Nominal</div><div class="met-kpi-value">${nom}</div><div class="met-kpi-sub">Online & fresh</div></div>
      <div class="met-kpi ${deg>0?'amber':'green'}"><div class="met-kpi-label">Degraded</div><div class="met-kpi-value">${deg}</div><div class="met-kpi-sub">Partial service</div></div>
      <div class="met-kpi ${off>0?'red':'green'}"><div class="met-kpi-label">Offline</div><div class="met-kpi-value">${off}</div><div class="met-kpi-sub">Not responding</div></div>
    </div>
    <div class="panel">
      <div class="panel-head">
        <span class="panel-title">Layer 0 Source Registry · ${sources.length} Sources</span>
        <div style="margin-left:auto;display:flex;gap:6px">
          <span class="tag tag-green">${free} FREE</span>
          <span class="tag tag-amber">${sources.length-free} KEY REQ.</span>
        </div>
      </div>
      <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse">
          <thead><tr style="background:rgba(0,0,0,.4)">
            ${['Status','Source','Agency','Type','Resolution','Format','Latency','Key'].map(h=>`<th style="padding:7px 10px;text-align:left;font-size:7.5px;letter-spacing:.14em;text-transform:uppercase;color:var(--text-muted);border-bottom:1px solid var(--border);white-space:nowrap">${h}</th>`).join('')}
          </tr></thead>
          <tbody>${sources.map(_srcRow).join('')}</tbody>
        </table>
      </div>
    </div>`;
}

function _srcRow(s){
  const status   = s.status || 'nominal';
  const requires = s.key || s.requires_key || false;
  const dotCls   = status==='nominal'?'live':status==='degraded'?'warn':'dead';
  const statusTx = status==='nominal'?'var(--green)':status==='degraded'?'var(--amber)':'var(--red)';
  const h        = s.latency || 0;
  const latStr   = h===0?'Static':h<1?`${Math.round(h*60)}min`:h<48?`${h.toFixed(0)}h`:`${Math.round(h/24)}d`;
  const latColor = h===0?'var(--text-muted)':h<2?'var(--green)':h<24?'var(--amber)':'var(--red)';
  const r        = s.resolution_km || 0;
  const resStr   = r>=100?`~${Math.round(r)}km`:r>=1?`${r.toFixed(0)}km`:`${(r*1000).toFixed(0)}m`;

  // FIX: sanitize all API-sourced string fields
  const safeName   = sanitizeHTML(s.name   || '—');
  const safeAgency = sanitizeHTML(s.agency || '—');
  const safeType   = sanitizeHTML(s.type   || '—');
  const safeFormat = sanitizeHTML(s.format || '—');
  const safeStatus = sanitizeHTML(status);

  return `<tr style="border-bottom:1px solid rgba(14,165,233,.06);transition:background .12s"
    onmouseover="this.style.background='rgba(14,165,233,.03)'" onmouseout="this.style.background='transparent'">
    <td style="padding:8px 10px"><div style="display:flex;align-items:center;gap:5px"><div class="status-dot ${dotCls}"></div><span style="font-size:8.5px;color:${statusTx};text-transform:uppercase;letter-spacing:.08em">${safeStatus}</span></div></td>
    <td style="padding:8px 10px;font-size:11px;color:var(--text-primary);white-space:nowrap">${safeName}</td>
    <td style="padding:8px 10px;font-size:9px;color:var(--text-secondary);white-space:nowrap">${safeAgency}</td>
    <td style="padding:8px 10px"><span class="tag tag-dim" style="font-size:7.5px">${safeType}</span></td>
    <td style="padding:8px 10px;font-family:var(--font-display);font-size:14px;color:var(--cobalt)">${resStr}</td>
    <td style="padding:8px 10px;font-size:9px;color:var(--text-secondary);font-family:var(--font-data)">${safeFormat}</td>
    <td style="padding:8px 10px;font-family:var(--font-display);font-size:14px;color:${latColor}">${latStr}</td>
    <td style="padding:8px 10px"><span class="tag ${requires?'tag-amber':'tag-green'}">${requires?'API KEY':'FREE'}</span></td>
  </tr>`;
}
