/**
 * globe.js — Meteorium 3D Globe Module
 * CesiumJS orthographic Earth + Three.js heatwave overlay
 * Climate risk layers, warning tabs, asset pins, RCP 8.5 animation
 *
 * Repo: frontend/modules/meteorium/globe.js
 * Depends: CesiumJS 1.114, Three.js r128
 * Usage: const globe = new MeteoGlobe(container, assets, opts); globe.init();
 */

'use strict';

/* ──────────────────────────────────────────────
   CONSTANTS
   ────────────────────────────────────────────── */
var GLOBE_COLORS = {
  CRITICAL:  { r:239, g:68,  b:68  },
  HIGH:      { r:245, g:158, b:11  },
  ELEVATED:  { r:249, g:115, b:22  },
  MODERATE:  { r:14,  g:165, b:233 },
  LOW:       { r:16,  g:185, b:129 },
};

var RISK_ZONES = [
  // [label, type, lat,lon, radiusKm, risk]
  { label:'Sao Paulo Fire Basin',    type:'wildfire', lat:-23.5, lon:-46.6, r:220, risk:.87, sev:'CRITICAL' },
  { label:'Mumbai Flood Zone',       type:'flood',    lat:18.9,  lon:72.8,  r:160, risk:.71, sev:'HIGH' },
  { label:'Chennai Heat Corridor',   type:'heat',     lat:13.1,  lon:80.3,  r:180, risk:.65, sev:'HIGH' },
  { label:'Deccan Drought Belt',     type:'drought',  lat:17.0,  lon:76.0,  r:380, risk:.61, sev:'ELEVATED' },
  { label:'Shanghai Transition',     type:'transition',lat:31.2, lon:121.5, r:140, risk:.61, sev:'ELEVATED' },
  { label:'Australia Drought Arc',   type:'drought',  lat:-25.0, lon:133.0, r:500, risk:.55, sev:'ELEVATED' },
  { label:'Sahel Aridity Zone',      type:'drought',  lat:15.0,  lon:20.0,  r:600, risk:.58, sev:'ELEVATED' },
  { label:'Siberian Fire Zone',      type:'wildfire', lat:62.0,  lon:105.0, r:450, risk:.42, sev:'MODERATE' },
];

var HEATWAVE_SOURCES = [
  { lat:-23.5, lon:-46.6, intensity:1.0 },
  { lat:18.9,  lon:72.8,  intensity:.85 },
  { lat:13.1,  lon:80.3,  intensity:.88 },
  { lat:28.6,  lon:77.2,  intensity:.72 },
  { lat:31.2,  lon:121.5, intensity:.70 },
  { lat:15.0,  lon:30.0,  intensity:.65 },
  { lat:-25.0, lon:133.0, intensity:.60 },
];

/* ──────────────────────────────────────────────
   HELPERS
   ────────────────────────────────────────────── */
function riskScore(s) {
  if (s >= .85) return { label:'CRITICAL', col:'#EF4444', gCol:GLOBE_COLORS.CRITICAL };
  if (s >= .65) return { label:'HIGH',     col:'#F59E0B', gCol:GLOBE_COLORS.HIGH };
  if (s >= .45) return { label:'ELEVATED', col:'#F97316', gCol:GLOBE_COLORS.ELEVATED };
  if (s >= .25) return { label:'MODERATE', col:'#0EA5E9', gCol:GLOBE_COLORS.MODERATE };
  return { label:'LOW', col:'#10B981', gCol:GLOBE_COLORS.LOW };
}

function pct(v) { return (((v || 0) * 100).toFixed(1)) + '%'; }

function latLonToCartesian3(lat, lon, alt) {
  alt = alt || 0;
  return Cesium.Cartesian3.fromDegrees(lon, lat, alt);
}

/* ──────────────────────────────────────────────
   CLIMATE HEATMAP CANVAS
   Equirectangular projection overlay on the globe
   ────────────────────────────────────────────── */
function buildHeatmapCanvas(zones, assets, time, activeLayers) {
  var W = 4096, H = 2048;
  var cvs = document.createElement('canvas');
  cvs.width = W; cvs.height = H;
  var ctx = cvs.getContext('2d');
  ctx.clearRect(0, 0, W, H);

  // RCP 8.5 time scale factor (0 at 2023, 1 at 2050)
  var tf = (time - 2023) / 27;

  zones.forEach(function(z) {
    // Check if layer is active or all shown
    var layerActive = activeLayers.all ||
      (z.type === 'wildfire'   && activeLayers.wildfire) ||
      (z.type === 'flood'      && activeLayers.flood) ||
      (z.type === 'heat'       && activeLayers.heat) ||
      (z.type === 'drought'    && activeLayers.drought) ||
      (z.type === 'transition' && activeLayers.political);
    if (!layerActive) return;

    var rs = riskScore(z.risk);
    var gc = rs.gCol;
    var alpha = Math.min(0.55, (z.risk + tf * 0.2) * 0.65);

    // Convert lat/lon to canvas pixels (equirectangular)
    var cx = (z.lon + 180) / 360 * W;
    var cy = (90 - z.lat) / 180 * H;
    var cr = (z.r / 20000) * W * (1 + tf * 0.4); // radius grows with time

    var grad = ctx.createRadialGradient(cx, cy, 0, cx, cy, cr);
    grad.addColorStop(0,   'rgba(' + gc.r + ',' + gc.g + ',' + gc.b + ',' + alpha + ')');
    grad.addColorStop(0.4, 'rgba(' + gc.r + ',' + gc.g + ',' + gc.b + ',' + (alpha * 0.6) + ')');
    grad.addColorStop(1,   'rgba(' + gc.r + ',' + gc.g + ',' + gc.b + ',0)');
    ctx.beginPath();
    ctx.arc(cx, cy, cr, 0, Math.PI * 2);
    ctx.fillStyle = grad;
    ctx.fill();
  });

  // Asset-level micro halos
  assets.forEach(function(a) {
    var rs = riskScore(a.cr);
    var gc = rs.gCol;
    if (a.cr < 0.3) return;
    var cx = (a.lon + 180) / 360 * W;
    var cy = (90 - a.lat) / 180 * H;
    var cr = 40 + a.cr * 60;
    var grad = ctx.createRadialGradient(cx, cy, 0, cx, cy, cr);
    var al = a.cr * 0.45;
    grad.addColorStop(0, 'rgba(' + gc.r + ',' + gc.g + ',' + gc.b + ',' + al + ')');
    grad.addColorStop(1, 'rgba(' + gc.r + ',' + gc.g + ',' + gc.b + ',0)');
    ctx.beginPath();
    ctx.arc(cx, cy, cr, 0, Math.PI * 2);
    ctx.fillStyle = grad;
    ctx.fill();
  });

  return cvs;
}

/* ──────────────────────────────────────────────
   WARNING TAB BILLBOARD SVG
   ────────────────────────────────────────────── */
function makeWarningBillboard(asset, rs) {
  var color = rs.col;
  var svg = [
    '<svg xmlns="http://www.w3.org/2000/svg" width="200" height="56">',
    '<defs>',
    '  <filter id="glow"><feGaussianBlur stdDeviation="2" result="blur"/>',
    '  <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge></filter>',
    '</defs>',
    // Tab body
    '<rect x="0" y="0" width="200" height="44" rx="3" fill="rgba(2,9,18,0.95)" stroke="' + color + '" stroke-width="1.2"/>',
    // Left accent bar
    '<rect x="0" y="0" width="3" height="44" rx="1" fill="' + color + '"/>',
    // Connector line
    '<line x1="100" y1="44" x2="100" y2="56" stroke="' + color + '" stroke-width="1" stroke-dasharray="2,2"/>',
    '<circle cx="100" cy="56" r="2" fill="' + color + '"/>',
    // Severity badge
    '<rect x="8" y="7" width="50" height="14" rx="2" fill="' + color + '" opacity="0.2"/>',
    '<rect x="8" y="7" width="50" height="14" rx="2" fill="none" stroke="' + color + '" stroke-width="0.8"/>',
    '<text x="33" y="17.5" font-family="\'Martian Mono\',monospace" font-size="8" fill="' + color + '" text-anchor="middle" font-weight="700">' + rs.label + '</text>',
    // Risk score
    '<text x="70" y="17.5" font-family="\'Bebas Neue\',sans-serif" font-size="14" fill="' + color + '">' + pct(asset.cr) + '</text>',
    // Asset name (truncated)
    '<text x="8" y="34" font-family="\'Martian Mono\',monospace" font-size="9" fill="#E0F0FF" opacity="0.9">' + asset.name.slice(0, 22) + '</text>',
    // Type indicator
    '<text x="160" y="17.5" font-family="\'Martian Mono\',monospace" font-size="7" fill="#7BA4C0" text-anchor="end">' + asset.type.slice(0, 6).toUpperCase() + '</text>',
    '</svg>'
  ].join('');

  var blob = new Blob([svg], { type: 'image/svg+xml' });
  return URL.createObjectURL(blob);
}

/* ──────────────────────────────────────────────
   METOGlobe CLASS
   ────────────────────────────────────────────── */
function MeteoGlobe(container, assets, opts) {
  this.container = container;
  this.assets = assets || [];
  this.opts = opts || {};
  this.viewer = null;
  this.scene = null;
  this.threeRenderer = null;
  this.threeScene = null;
  this.threeCamera = null;
  this.particles = null;
  this.heatmapLayer = null;
  this.assetEntities = {};
  this.zoneEntities = [];
  this.warningBillboards = [];
  this.currentTime = 2025;
  this.activeLayers = { all: true, wildfire:false, flood:false, heat:false, drought:false, political:false, supply:false };
  this.destroyed = false;
  this._billboardUrls = [];
  this._warningVisible = {};
  this._animFrame = null;
  this._heatFrame = null;
}

MeteoGlobe.prototype.init = function() {
  this._initCesium();
  this._initThreeOverlay();
  this._addGlowAtmosphere();
  this._buildHeatmapLayer();
  this._addRiskZones();
  this._addAssetPins();
  this._addWarningTabs();
  this._startHeatwaveAnimation();
  this._bindEvents();
  console.log('[MeteoGlobe] initialized. Assets:', this.assets.length, 'Zones:', RISK_ZONES.length);
};

/* ── CESIUM INIT ── */
MeteoGlobe.prototype._initCesium = function() {
  var self = this;

  // Set Cesium Ion token (user should replace with their own from cesium.com/ion)
  if (window.CESIUM_ION_TOKEN) {
    Cesium.Ion.defaultAccessToken = window.CESIUM_ION_TOKEN;
  }

  // Disable Cesium logo/attribution for cleaner look (keep attribution in prod)
  this.viewer = new Cesium.Viewer(this.container, {
    baseLayerPicker:   false,
    geocoder:          false,
    homeButton:        false,
    sceneModePicker:   false,
    navigationHelpButton: false,
    animation:         false,
    timeline:          false,
    fullscreenButton:  false,
    vrButton:          false,
    infoBox:           false,
    selectionIndicator:false,
    creditContainer:   document.createElement('div'), // hide credits div
    imageryProvider: new Cesium.TileMapServiceImageryProvider({
      url: Cesium.buildModuleUrl('Assets/Textures/NaturalEarthII'),
    }),
    terrainProvider: new Cesium.EllipsoidTerrainProvider(),
  });

  this.scene = this.viewer.scene;
  var scene = this.scene;

  // Dark space background
  scene.backgroundColor = new Cesium.Color(0.008, 0.035, 0.071, 1.0);
  scene.globe.baseColor = new Cesium.Color(0.02, 0.06, 0.12, 1.0);
  scene.globe.enableLighting = true;
  scene.globe.showGroundAtmosphere = true;
  scene.globe.atmosphereLightIntensity = 10.0;
  scene.globe.atmosphereHueShift = 0.0;
  scene.globe.atmosphereSaturationShift = 0.1;

  // Stars
  scene.skyBox = new Cesium.SkyBox({
    sources: {
      positiveX: Cesium.buildModuleUrl('Assets/Textures/SkyBox/tycho2t3_80_px.jpg'),
      negativeX: Cesium.buildModuleUrl('Assets/Textures/SkyBox/tycho2t3_80_mx.jpg'),
      positiveY: Cesium.buildModuleUrl('Assets/Textures/SkyBox/tycho2t3_80_py.jpg'),
      negativeY: Cesium.buildModuleUrl('Assets/Textures/SkyBox/tycho2t3_80_my.jpg'),
      positiveZ: Cesium.buildModuleUrl('Assets/Textures/SkyBox/tycho2t3_80_pz.jpg'),
      negativeZ: Cesium.buildModuleUrl('Assets/Textures/SkyBox/tycho2t3_80_mz.jpg'),
    }
  });
  scene.skyAtmosphere.show = true;

  // Camera start position - tilt slightly from South Asia view
  this.viewer.camera.setView({
    destination: Cesium.Cartesian3.fromDegrees(60.0, 20.0, 18000000),
    orientation: { heading:0, pitch: -Math.PI/2, roll:0 }
  });

  // Fog + scene settings
  scene.fog.enabled = true;
  scene.fog.density = 0.0001;
  scene.msaaSamples = 4;

  // Remove default double-click zoom behavior
  this.viewer.cesiumWidget.screenSpaceEventHandler.removeInputAction(
    Cesium.ScreenSpaceEventType.LEFT_DOUBLE_CLICK
  );
};

/* ── HEATMAP LAYER ── */
MeteoGlobe.prototype._buildHeatmapLayer = function() {
  this._rebuildHeatmap();
};

MeteoGlobe.prototype._rebuildHeatmap = function() {
  var self = this;
  if (this.heatmapLayer) {
    this.viewer.imageryLayers.remove(this.heatmapLayer, true);
    this.heatmapLayer = null;
  }

  var cvs = buildHeatmapCanvas(RISK_ZONES, this.assets, this.currentTime, this.activeLayers);

  var provider = new Cesium.SingleTileImageryProvider({
    url: cvs.toDataURL('image/png'),
    rectangle: Cesium.Rectangle.fromDegrees(-180, -90, 180, 90),
  });

  this.heatmapLayer = this.viewer.imageryLayers.addImageryProvider(provider);
  this.heatmapLayer.alpha = 0.72;
  this.heatmapLayer.brightness = 1.2;
  this.heatmapLayer.contrast = 1.1;
};

MeteoGlobe.prototype.setTime = function(year) {
  this.currentTime = year;
  this._rebuildHeatmap();
  this._updateWarningTabVisibility();
  if (this.opts.onTimeChange) this.opts.onTimeChange(year);
};

MeteoGlobe.prototype.setLayers = function(layers) {
  this.activeLayers = layers;
  if (!layers.wildfire && !layers.flood && !layers.heat && !layers.drought && !layers.political && !layers.supply) {
    this.activeLayers.all = true;
  } else {
    this.activeLayers.all = false;
  }
  this._rebuildHeatmap();
  this._updateZoneVisibility();
};

/* ── RISK ZONE POLYGONS ── */
MeteoGlobe.prototype._addRiskZones = function() {
  var self = this;
  this.zoneEntities = [];

  RISK_ZONES.forEach(function(z) {
    var rs = riskScore(z.risk);
    var gc = rs.gCol;

    // Build a circle approximation
    var positions = [];
    var N = 64;
    for (var i = 0; i < N; i++) {
      var angle = (i / N) * Math.PI * 2;
      var latOffset = (z.r / 111) * Math.cos(angle);
      var lonOffset = (z.r / (111 * Math.cos(z.lat * Math.PI / 180))) * Math.sin(angle);
      positions.push(Cesium.Cartesian3.fromDegrees(z.lon + lonOffset, z.lat + latOffset));
    }

    var entity = self.viewer.entities.add({
      id: 'zone_' + z.label.replace(/\s/g,'_'),
      name: z.label,
      polygon: {
        hierarchy: new Cesium.PolygonHierarchy(positions),
        material: new Cesium.ColorMaterialProperty(
          new Cesium.Color(gc.r/255, gc.g/255, gc.b/255, 0.12)
        ),
        outline: true,
        outlineColor: new Cesium.Color(gc.r/255, gc.g/255, gc.b/255, 0.5),
        outlineWidth: 1.5,
        heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
      },
      // Store metadata
      properties: { type: z.type, sev: z.sev, risk: z.risk }
    });

    self.zoneEntities.push({ entity: entity, type: z.type });
  });
};

MeteoGlobe.prototype._updateZoneVisibility = function() {
  var self = this;
  this.zoneEntities.forEach(function(ze) {
    var z = RISK_ZONES.find(function(r){ return ze.entity.id && ze.entity.id.indexOf(r.type) !== -1; });
    if (!z) { ze.entity.show = true; return; }
    ze.entity.show = self.activeLayers.all ||
      (z.type === 'wildfire'   && self.activeLayers.wildfire) ||
      (z.type === 'flood'      && self.activeLayers.flood) ||
      (z.type === 'heat'       && self.activeLayers.heat) ||
      (z.type === 'drought'    && self.activeLayers.drought) ||
      (z.type === 'transition' && self.activeLayers.political);
  });
};

/* ── ASSET PINS ── */
MeteoGlobe.prototype._addAssetPins = function() {
  var self = this;
  this.assetEntities = {};

  this.assets.forEach(function(asset) {
    var rs = riskScore(asset.cr);

    // Custom pin SVG
    var pinColor = rs.col.replace('#', '');
    var pinSvg = [
      '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="40">',
      '<defs><filter id="s"><feDropShadow dx="0" dy="2" stdDeviation="2" flood-color="' + rs.col + '" flood-opacity="0.8"/></filter></defs>',
      '<path d="M16 0 C7.16 0 0 7.16 0 16 C0 28 16 40 16 40 C16 40 32 28 32 16 C32 7.16 24.84 0 16 0z" fill="rgba(2,9,18,0.9)" stroke="' + rs.col + '" stroke-width="1.5" filter="url(#s)"/>',
      '<circle cx="16" cy="16" r="6" fill="' + rs.col + '" opacity="0.9"/>',
      '<circle cx="16" cy="16" r="10" fill="' + rs.col + '" opacity="0.15"/>',
      '</svg>'
    ].join('');

    var pinBlob = new Blob([pinSvg], { type: 'image/svg+xml' });
    var pinUrl = URL.createObjectURL(pinBlob);
    self._billboardUrls.push(pinUrl);

    var entity = self.viewer.entities.add({
      id: 'asset_' + asset.id,
      name: asset.name,
      position: Cesium.Cartesian3.fromDegrees(asset.lon, asset.lat, 0),
      billboard: {
        image: pinUrl,
        width: 32,
        height: 40,
        verticalOrigin: Cesium.VerticalOrigin.BOTTOM,
        pixelOffset: new Cesium.Cartesian2(0, 0),
        heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
        eyeOffset: new Cesium.Cartesian3(0, 0, -500),
        scaleByDistance: new Cesium.NearFarScalar(1000000, 1.2, 20000000, 0.6),
        disableDepthTestDistance: Number.POSITIVE_INFINITY,
      },
      properties: asset
    });

    // Pulse ring for critical assets
    if (asset.cr >= 0.65) {
      self.viewer.entities.add({
        position: Cesium.Cartesian3.fromDegrees(asset.lon, asset.lat, 100),
        ellipse: {
          semiMajorAxis: 60000 + asset.cr * 80000,
          semiMinorAxis: 60000 + asset.cr * 80000,
          heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
          material: new Cesium.ColorMaterialProperty(
            new Cesium.Color(
              rs.gCol.r/255, rs.gCol.g/255, rs.gCol.b/255,
              0.08 + Math.random() * 0.06
            )
          ),
          outline: true,
          outlineColor: new Cesium.Color(rs.gCol.r/255, rs.gCol.g/255, rs.gCol.b/255, 0.4),
          outlineWidth: 1,
        }
      });
    }

    self.assetEntities[asset.id] = entity;
  });
};

/* ── WARNING TABS ── */
MeteoGlobe.prototype._addWarningTabs = function() {
  var self = this;
  this.warningBillboards = [];
  this._warningVisible = {};

  this.assets.forEach(function(asset) {
    var rs = riskScore(asset.cr);
    // Only show warning tabs for ELEVATED and above
    if (asset.cr < 0.45) return;

    var billboardUrl = makeWarningBillboard(asset, rs);
    self._billboardUrls.push(billboardUrl);

    var entity = self.viewer.entities.add({
      id: 'warn_' + asset.id,
      position: Cesium.Cartesian3.fromDegrees(asset.lon, asset.lat, 800000 + asset.cr * 400000),
      billboard: {
        image: billboardUrl,
        width: 200,
        height: 56,
        verticalOrigin: Cesium.VerticalOrigin.BOTTOM,
        horizontalOrigin: Cesium.HorizontalOrigin.CENTER,
        heightReference: Cesium.HeightReference.NONE,
        disableDepthTestDistance: Number.POSITIVE_INFINITY,
        scaleByDistance: new Cesium.NearFarScalar(500000, 1.3, 15000000, 0.7),
        translucencyByDistance: new Cesium.NearFarScalar(500000, 1.0, 18000000, 0.0),
        show: asset.cr >= 0.65, // Only CRITICAL/HIGH visible by default
      },
    });

    self.warningBillboards.push({ entity: entity, asset: asset, rs: rs });
    self._warningVisible[asset.id] = asset.cr >= 0.65;
  });
};

MeteoGlobe.prototype._updateWarningTabVisibility = function() {
  var tf = (this.currentTime - 2023) / 27;
  this.warningBillboards.forEach(function(wb) {
    // As time progresses, lower threshold for showing tabs
    var threshold = 0.65 - tf * 0.15;
    wb.entity.billboard.show = wb.asset.cr >= threshold;
  });
};

/* ── ATMOSPHERE GLOW ── */
MeteoGlobe.prototype._addGlowAtmosphere = function() {
  this.scene.skyAtmosphere.hueShift = -0.05;
  this.scene.skyAtmosphere.saturationShift = 0.1;
  this.scene.skyAtmosphere.brightnessShift = 0.1;
  this.scene.globe.atmosphereMieScaleHeight = 11000;
  this.scene.globe.atmosphereRayleighScaleHeight = 10000;
};

/* ── THREE.JS HEATWAVE OVERLAY ── */
MeteoGlobe.prototype._initThreeOverlay = function() {
  var self = this;
  var W = this.container.clientWidth;
  var H = this.container.clientHeight;

  // Create canvas overlay on top of Cesium
  this._threeCanvas = document.createElement('canvas');
  this._threeCanvas.style.cssText = 'position:absolute;top:0;left:0;pointer-events:none;z-index:5';
  this._threeCanvas.width = W;
  this._threeCanvas.height = H;
  this.container.style.position = 'relative';
  this.container.appendChild(this._threeCanvas);

  // Three.js renderer
  this.threeRenderer = new THREE.WebGLRenderer({
    canvas: this._threeCanvas,
    alpha: true,
    antialias: false,
  });
  this.threeRenderer.setSize(W, H);
  this.threeRenderer.setClearColor(0x000000, 0);
  this.threeRenderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

  // Orthographic camera (screen space for 2D overlay)
  this.threeCamera = new THREE.OrthographicCamera(-W/2, W/2, H/2, -H/2, 0.1, 1000);
  this.threeCamera.position.z = 10;

  // Scene
  this.threeScene = new THREE.Scene();

  // Build heatwave particles
  this._buildHeatwaveParticles(W, H);

  // Handle resize
  window.addEventListener('resize', function() {
    var nW = self.container.clientWidth;
    var nH = self.container.clientHeight;
    self._threeCanvas.width = nW;
    self._threeCanvas.height = nH;
    self.threeRenderer.setSize(nW, nH);
    self.threeCamera.left   = -nW/2;
    self.threeCamera.right  =  nW/2;
    self.threeCamera.top    =  nH/2;
    self.threeCamera.bottom = -nH/2;
    self.threeCamera.updateProjectionMatrix();
    self._rebuildHeatwaveParticles(nW, nH);
  });
};

MeteoGlobe.prototype._buildHeatwaveParticles = function(W, H) {
  var self = this;
  if (this.particles) {
    this.threeScene.remove(this.particles);
    this.particles.geometry.dispose();
    this.particles.material.dispose();
  }

  var COUNT = 1800;
  var positions = new Float32Array(COUNT * 3);
  var velocities = new Float32Array(COUNT * 3);
  var opacities  = new Float32Array(COUNT);
  var sizes      = new Float32Array(COUNT);
  var heatSource = new Float32Array(COUNT); // 0-1 intensity

  for (var i = 0; i < COUNT; i++) {
    // Assign each particle to a random heatwave source
    var src = HEATWAVE_SOURCES[Math.floor(Math.random() * HEATWAVE_SOURCES.length)];
    // Random spread around source (will be mapped to screen later)
    positions[i*3]   = (src.lon / 360 + 0.5) * W - W/2 + (Math.random() - 0.5) * 120;
    positions[i*3+1] = (0.5 - src.lat / 180) * H - H/2 + (Math.random() - 0.5) * 80;
    positions[i*3+2] = 0;
    velocities[i*3]   = (Math.random() - 0.5) * 0.3;
    velocities[i*3+1] = 0.2 + Math.random() * 0.8;  // drift upward
    velocities[i*3+2] = 0;
    opacities[i]  = Math.random();
    sizes[i]      = 2 + Math.random() * 4;
    heatSource[i] = src.intensity;
  }

  var geo = new THREE.BufferGeometry();
  geo.setAttribute('position',   new THREE.BufferAttribute(positions, 3));
  geo.setAttribute('opacity',    new THREE.BufferAttribute(opacities, 1));
  geo.setAttribute('psize',      new THREE.BufferAttribute(sizes, 1));
  geo.setAttribute('heatSource', new THREE.BufferAttribute(heatSource, 1));
  geo._velocities = velocities;

  var mat = new THREE.ShaderMaterial({
    transparent: true,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
    vertexShader: [
      'attribute float opacity;',
      'attribute float psize;',
      'attribute float heatSource;',
      'varying float vOpacity;',
      'varying float vHeat;',
      'void main() {',
      '  vOpacity = opacity;',
      '  vHeat = heatSource;',
      '  gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);',
      '  gl_PointSize = psize;',
      '}'
    ].join('\n'),
    fragmentShader: [
      'varying float vOpacity;',
      'varying float vHeat;',
      'void main() {',
      '  vec2 uv = gl_PointCoord - vec2(0.5);',
      '  float d = length(uv);',
      '  if (d > 0.5) discard;',
      '  float fade = 1.0 - d * 2.0;',
      '  fade = fade * fade;',
      // Heat color: amber → red → white core
      '  vec3 coldColor = vec3(0.96, 0.62, 0.04);',  // amber
      '  vec3 hotColor  = vec3(0.94, 0.27, 0.27);',  // red
      '  vec3 coreColor = vec3(1.0, 0.82, 0.6);',    // warm white
      '  vec3 col = mix(coldColor, hotColor, vHeat);',
      '  col = mix(col, coreColor, fade * 0.4);',
      '  float alpha = vOpacity * fade * vHeat * 0.45;',
      '  gl_FragColor = vec4(col, alpha);',
      '}'
    ].join('\n'),
  });

  this.particles = new THREE.Points(geo, mat);
  this.threeScene.add(this.particles);
  this._particleCount = COUNT;
  this._particleW = W;
  this._particleH = H;
};

MeteoGlobe.prototype._rebuildHeatwaveParticles = function(W, H) {
  this._buildHeatwaveParticles(W, H);
};

MeteoGlobe.prototype._startHeatwaveAnimation = function() {
  var self = this;
  var t = 0;

  function loop() {
    if (self.destroyed) return;
    self._heatFrame = requestAnimationFrame(loop);
    t += 0.012;

    var geo = self.particles.geometry;
    var pos = geo.attributes.position.array;
    var vel = geo._velocities;
    var op  = geo.attributes.opacity.array;
    var W   = self._particleW;
    var H   = self._particleH;

    for (var i = 0; i < self._particleCount; i++) {
      // Drift up + slight wave
      pos[i*3]   += vel[i*3]   + Math.sin(t + i * 0.5) * 0.08;
      pos[i*3+1] += vel[i*3+1];

      // Fade out as particle rises
      op[i] -= 0.004;
      if (op[i] <= 0) {
        // Reset particle to its heatwave source
        var src = HEATWAVE_SOURCES[Math.floor(Math.random() * HEATWAVE_SOURCES.length)];
        pos[i*3]   = (src.lon / 360 + 0.5) * W - W/2 + (Math.random() - 0.5) * 100;
        pos[i*3+1] = (0.5 - src.lat / 180) * H - H/2 + (Math.random() - 0.5) * 60;
        op[i] = 0.2 + Math.random() * 0.8;
      }
    }

    geo.attributes.position.needsUpdate = true;
    geo.attributes.opacity.needsUpdate  = true;

    self.threeRenderer.render(self.threeScene, self.threeCamera);
  }

  loop();
};

/* ── EVENTS ── */
MeteoGlobe.prototype._bindEvents = function() {
  var self = this;
  var handler = new Cesium.ScreenSpaceEventHandler(this.viewer.canvas);

  // Hover → highlight
  handler.setInputAction(function(movement) {
    var picked = self.scene.pick(movement.endPosition);
    if (Cesium.defined(picked) && picked.id) {
      var entity = picked.id;
      if (entity.id && entity.id.indexOf('asset_') === 0) {
        var assetId = entity.id.replace('asset_', '');
        var asset = self.assets.find(function(a){ return a.id === assetId; });
        if (asset && self.opts.onHover) self.opts.onHover(asset, movement.endPosition);
      }
    } else {
      if (self.opts.onHover) self.opts.onHover(null, null);
    }
  }, Cesium.ScreenSpaceEventType.MOUSE_MOVE);

  // Click → select
  handler.setInputAction(function(click) {
    var picked = self.scene.pick(click.position);
    if (Cesium.defined(picked) && picked.id) {
      var entity = picked.id;
      if (entity.id && entity.id.indexOf('asset_') === 0) {
        var assetId = entity.id.replace('asset_', '');
        var asset = self.assets.find(function(a){ return a.id === assetId; });
        if (asset) {
          // Fly camera to asset
          self.viewer.camera.flyTo({
            destination: Cesium.Cartesian3.fromDegrees(asset.lon, asset.lat, 4500000),
            duration: 1.8,
            orientation: { heading:0, pitch: -Math.PI/3, roll:0 }
          });
          if (self.opts.onSelect) self.opts.onSelect(asset);
        }
      }
    }
  }, Cesium.ScreenSpaceEventType.LEFT_CLICK);

  // Double-click → deep zoom
  handler.setInputAction(function(dbl) {
    var picked = self.scene.pick(dbl.position);
    if (Cesium.defined(picked) && picked.id) {
      var entity = picked.id;
      if (entity.id && entity.id.indexOf('asset_') === 0) {
        var assetId = entity.id.replace('asset_', '');
        var asset = self.assets.find(function(a){ return a.id === assetId; });
        if (asset) {
          self.viewer.camera.flyTo({
            destination: Cesium.Cartesian3.fromDegrees(asset.lon, asset.lat, 1800000),
            duration: 2.2,
            orientation: { heading:0, pitch: -Math.PI/4, roll:0 }
          });
        }
      }
    }
  }, Cesium.ScreenSpaceEventType.LEFT_DOUBLE_CLICK);

  this._cesiumHandler = handler;
};

/* ── CAMERA CONTROLS ── */
MeteoGlobe.prototype.flyToAsset = function(asset, alt) {
  alt = alt || 3000000;
  this.viewer.camera.flyTo({
    destination: Cesium.Cartesian3.fromDegrees(asset.lon, asset.lat, alt),
    duration: 2.0,
    orientation: { heading:0, pitch: -Math.PI/3.5, roll:0 }
  });
};

MeteoGlobe.prototype.flyToGlobal = function() {
  this.viewer.camera.flyTo({
    destination: Cesium.Cartesian3.fromDegrees(60.0, 20.0, 18000000),
    duration: 2.5,
    orientation: { heading:0, pitch: -Math.PI/2, roll:0 }
  });
};

MeteoGlobe.prototype.addAsset = function(asset) {
  this.assets.push(asset);
  this._addSingleAssetPin(asset);
  this._addSingleWarningTab(asset);
  this._rebuildHeatmap();
};

MeteoGlobe.prototype._addSingleAssetPin = function(asset) {
  var self = this;
  var rs = riskScore(asset.cr);

  var pinSvg = '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="40"><path d="M16 0 C7.16 0 0 7.16 0 16 C0 28 16 40 16 40 C16 40 32 28 32 16 C32 7.16 24.84 0 16 0z" fill="rgba(2,9,18,0.9)" stroke="' + rs.col + '" stroke-width="1.5"/><circle cx="16" cy="16" r="6" fill="' + rs.col + '"/></svg>';
  var blob = new Blob([pinSvg], { type: 'image/svg+xml' });
  var url = URL.createObjectURL(blob);
  this._billboardUrls.push(url);

  var entity = this.viewer.entities.add({
    id: 'asset_' + asset.id,
    name: asset.name,
    position: Cesium.Cartesian3.fromDegrees(asset.lon, asset.lat, 0),
    billboard: {
      image: url,
      width: 32, height: 40,
      verticalOrigin: Cesium.VerticalOrigin.BOTTOM,
      heightReference: Cesium.HeightReference.CLAMP_TO_GROUND,
      disableDepthTestDistance: Number.POSITIVE_INFINITY,
      scaleByDistance: new Cesium.NearFarScalar(1000000, 1.2, 20000000, 0.6),
    }
  });
  this.assetEntities[asset.id] = entity;
};

/* ── Add warning tab for a single asset (called on addAsset after init) ── */
MeteoGlobe.prototype._addSingleWarningTab = function(asset) {
  if (asset.cr < 0.45) return; // Only ELEVATED and above get a tab
  var rs = riskScore(asset.cr);
  var billboardUrl = makeWarningBillboard(asset, rs);
  this._billboardUrls.push(billboardUrl);

  var entity = this.viewer.entities.add({
    id: 'warn_' + asset.id,
    position: Cesium.Cartesian3.fromDegrees(asset.lon, asset.lat, 800000 + asset.cr * 400000),
    billboard: {
      image: billboardUrl,
      width: 200,
      height: 56,
      verticalOrigin: Cesium.VerticalOrigin.BOTTOM,
      horizontalOrigin: Cesium.HorizontalOrigin.CENTER,
      heightReference: Cesium.HeightReference.NONE,
      disableDepthTestDistance: Number.POSITIVE_INFINITY,
      scaleByDistance: new Cesium.NearFarScalar(500000, 1.3, 15000000, 0.7),
      translucencyByDistance: new Cesium.NearFarScalar(500000, 1.0, 18000000, 0.0),
      show: asset.cr >= 0.65,
    },
  });
  this.warningBillboards.push({ entity: entity, asset: asset, rs: rs });
};

/* ── DESTROY ── */
MeteoGlobe.prototype.destroy = function() {
  this.destroyed = true;
  if (this._heatFrame) cancelAnimationFrame(this._heatFrame);
  if (this._cesiumHandler) this._cesiumHandler.destroy();
  this._billboardUrls.forEach(function(u){ URL.revokeObjectURL(u); });
  if (this.threeRenderer) {
    this.threeRenderer.dispose();
    this.threeRenderer.forceContextLoss();
  }
  if (this.viewer) this.viewer.destroy();
};

/* ──────────────────────────────────────────────
   EXPORT
   ────────────────────────────────────────────── */
window.MeteoGlobe = MeteoGlobe;
window.GlobeRiskScore = riskScore;
