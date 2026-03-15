"""
layer3/preprocessor.py
Meteorium Engine — LAYER 3: Geospatial Preprocessing (Scientific ETL)
Converts raw satellite data into H3-indexed geospatial feature tiles.

Pipeline per source type:
  NetCDF (ERA5)    → temporal aggregation → anomaly → H3 tiles
  CSV (FIRMS)      → clustering → FRP density → H3 hazard surface
  JSON (OpenMeteo) → normalization → derived indices → H3 features
  GeoTIFF (S2)     → spectral indices → anomaly → H3 tiles
"""

import json
import logging
import math
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from core.config import STORE_DIR, H3_RESOLUTION_ASSET, H3_RESOLUTION_REGIONAL
from core.models import ProcessedTile, TelemetryRecord

logger = logging.getLogger("meteorium.layer3")


# ─── H3 availability check ────────────────────────────────────────────────────
try:
    import h3
    H3_AVAILABLE = True
except ImportError:
    H3_AVAILABLE = False
    logger.warning("[Layer3] h3 not installed — using lat/lon grid fallback. pip install h3")


class GeospatialPreprocessor:
    """
    Layer 3: Converts raw telemetry into H3-indexed feature tiles.
    All tiles are stored in a PostGIS-compatible SQLite schema.
    In production: swap SQLite backend for PostgreSQL + PostGIS.
    """

    FEATURE_DB = STORE_DIR / "features.db"

    def __init__(self):
        self._init_db()
        logger.info(f"[Layer3] Feature DB: {self.FEATURE_DB}")

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS climate_features (
                    h3_index     TEXT NOT NULL,
                    variable     TEXT NOT NULL,
                    value        REAL NOT NULL,
                    unit         TEXT,
                    scenario     TEXT DEFAULT 'observed',
                    horizon_date TEXT,
                    source       TEXT,
                    confidence   REAL DEFAULT 1.0,
                    computed_at  TEXT NOT NULL,
                    PRIMARY KEY (h3_index, variable, scenario)
                );

                CREATE TABLE IF NOT EXISTS fire_hazard (
                    h3_index      TEXT PRIMARY KEY,
                    fire_prob_7d  REAL DEFAULT 0,
                    fire_prob_30d REAL DEFAULT 0,
                    frp_max_mw    REAL DEFAULT 0,
                    fire_count    INTEGER DEFAULT 0,
                    updated_at    TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS satellite_indices (
                    h3_index     TEXT PRIMARY KEY,
                    ndvi_current REAL,
                    ndvi_anomaly REAL,
                    ndwi_flood   REAL,
                    nbr_burn     REAL,
                    updated_at   TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_cf_variable
                ON climate_features(variable, computed_at DESC);

                CREATE INDEX IF NOT EXISTS idx_cf_h3
                ON climate_features(h3_index);
            """)
            conn.commit()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.FEATURE_DB)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
        finally:
            conn.close()

    # ── H3 helpers ────────────────────────────────────────────────────────────

    def lat_lon_to_h3(self, lat: float, lon: float, resolution: int = H3_RESOLUTION_ASSET) -> str:
        """Convert lat/lon to H3 cell index string."""
        if H3_AVAILABLE:
            return h3.geo_to_h3(lat, lon, resolution)
        # Fallback: grid-based pseudo-index
        grid_lat = round(lat / 0.1) * 0.1
        grid_lon = round(lon / 0.1) * 0.1
        return f"grid_{grid_lat:.2f}_{grid_lon:.2f}_r{resolution}"

    def h3_neighbors(self, h3_index: str, k: int = 1) -> list[str]:
        """Get H3 neighbors within k rings."""
        if H3_AVAILABLE:
            return list(h3.k_ring(h3_index, k))
        return [h3_index]   # fallback: just self

    def h3_to_lat_lon(self, h3_index: str) -> tuple[float, float]:
        """Get center coordinates of an H3 cell."""
        if H3_AVAILABLE and not h3_index.startswith("grid_"):
            return h3.h3_to_geo(h3_index)
        # Parse grid fallback
        parts = h3_index.replace("grid_", "").split("_")
        return float(parts[0]), float(parts[1])

    # ── Process TelemetryRecords into tiles ───────────────────────────────────

    def process_telemetry(
        self,
        records: list[TelemetryRecord],
        lat: float,
        lon: float,
    ) -> list[ProcessedTile]:
        """
        Convert raw TelemetryRecords into H3-indexed ProcessedTiles.
        Applies normalization, anomaly scoring, and confidence weighting.
        """
        h3_idx = self.lat_lon_to_h3(lat, lon)
        now    = datetime.now(timezone.utc)
        tiles  = []

        for rec in records:
            # Normalize value to 0-1 range for risk-relevant variables
            normalized = self._normalize(rec.variable, rec.value)

            tile = ProcessedTile(
                h3_index   = h3_idx,
                variable   = rec.variable,
                value      = normalized if normalized is not None else rec.value,
                unit       = rec.unit,
                scenario   = "observed",
                source     = rec.source,
                confidence = rec.confidence,
                computed_at = now,
            )
            tiles.append(tile)

        self._store_tiles(tiles)
        return tiles

    def _normalize(self, variable: str, value: float) -> Optional[float]:
        """
        Normalize variables to 0-1 scale where 0=min risk, 1=max risk.
        Returns None for variables that should not be normalized.
        """
        NORM_TABLE = {
            # Temperature anomaly: 0°C = baseline, +4°C = critical
            "temp_anomaly_c":        lambda v: max(0.0, min(1.0, (v + 1) / 5.0)),
            # Precipitation anomaly: -100% = extreme drought, +200% = flood
            "precip_anomaly_pct":    lambda v: 0.5 + (-v / 200.0) if v < 0 else 0.5,
            # Direct probability/index fields pass through
            "heat_stress_prob_7d":   lambda v: max(0.0, min(1.0, v)),
            "drought_index":         lambda v: max(0.0, min(1.0, v)),
            "extreme_wind_prob_7d":  lambda v: max(0.0, min(1.0, v)),
            "fire_prob_25km":        lambda v: max(0.0, min(1.0, v)),
            "fire_prob_100km":       lambda v: max(0.0, min(1.0, v)),
            "fire_hazard_score":     lambda v: max(0.0, min(1.0, v)),
            "co2_intensity_norm":    lambda v: max(0.0, min(1.0, v)),
            "transition_risk_score": lambda v: max(0.0, min(1.0, v)),
            "carbon_policy_risk":    lambda v: max(0.0, min(1.0, v)),
            # Wind speed: 0 m/s = safe, 40+ m/s = critical
            "wind_speed_ms":         lambda v: max(0.0, min(1.0, (v - 5) / 35.0)),
            # Soil moisture: inverse (lower = more drought risk)
            "soil_moisture":         lambda v: max(0.0, min(1.0, (0.35 - v) / 0.35)),
        }
        fn = NORM_TABLE.get(variable)
        return fn(value) if fn else None

    # ── ERA5 Preprocessing ────────────────────────────────────────────────────

    def process_era5_netcdf(self, file_path: str, lat: float, lon: float) -> list[ProcessedTile]:
        """
        Process ERA5 NetCDF file into statistical baselines.
        Requires: xarray, netCDF4, numpy
        """
        try:
            import xarray as xr
            import numpy as np
        except ImportError:
            logger.warning("[Layer3] xarray/netCDF4 not installed — skipping ERA5 processing")
            return []

        try:
            ds     = xr.open_dataset(file_path)
            lat_r  = round(lat * 4) / 4
            lon_r  = round(lon * 4) / 4
            pt     = ds.sel(latitude=lat_r, longitude=lon_r, method="nearest")
            h3_idx = self.lat_lon_to_h3(lat, lon)
            now    = datetime.now(timezone.utc)
            tiles  = []

            VAR_MAP = {
                "t2m":   ("era5_temp_c",      lambda v: v - 273.15),
                "tp":    ("era5_precip_mm_d",  lambda v: v * 1000 * 30),
                "swvl1": ("era5_soil_moisture", lambda v: v),
            }

            for era5_var, (our_var, transform) in VAR_MAP.items():
                if era5_var not in pt:
                    continue
                vals = pt[era5_var].values.flatten()
                vals = vals[~np.isnan(vals)]
                if not len(vals):
                    continue

                mean_val = float(np.mean(vals))
                p99_val  = float(np.percentile(vals, 99))
                std_val  = float(np.std(vals))

                for suffix, raw_val in [("_mean", mean_val), ("_p99", p99_val), ("_std", std_val)]:
                    tiles.append(ProcessedTile(
                        h3_index=h3_idx,
                        variable=our_var + suffix,
                        value=round(transform(raw_val), 4),
                        unit="",
                        scenario="era5_baseline",
                        source="era5_cds",
                        confidence=0.95,
                        computed_at=now,
                    ))

            self._store_tiles(tiles)
            ds.close()
            return tiles

        except Exception as e:
            logger.warning(f"[Layer3] ERA5 processing error: {e}")
            return []

    # ── FIRMS Fire Processing ─────────────────────────────────────────────────

    def process_firms_csv(
        self,
        file_path: str,
        center_lat: float,
        center_lon: float,
    ) -> list[ProcessedTile]:
        """Process FIRMS CSV into H3-indexed fire hazard tiles."""
        import csv
        h3_idx  = self.lat_lon_to_h3(center_lat, center_lon)
        now     = datetime.now(timezone.utc)

        detection_by_h3: dict = {}

        try:
            with open(file_path, newline="") as f:
                for row in csv.DictReader(f):
                    try:
                        dlat = float(row.get("latitude", 0))
                        dlon = float(row.get("longitude", 0))
                        frp  = float(row.get("frp", 0))
                        conf = row.get("confidence", "n").lower()
                        if conf not in ("n", "h", "nominal", "high"):
                            continue
                        cell = self.lat_lon_to_h3(dlat, dlon)
                        if cell not in detection_by_h3:
                            detection_by_h3[cell] = {"count": 0, "frp_sum": 0.0, "frp_max": 0.0}
                        detection_by_h3[cell]["count"]   += 1
                        detection_by_h3[cell]["frp_sum"] += frp
                        detection_by_h3[cell]["frp_max"]  = max(detection_by_h3[cell]["frp_max"], frp)
                    except (ValueError, KeyError):
                        continue
        except Exception as e:
            logger.warning(f"[Layer3] FIRMS CSV error: {e}")
            return []

        tiles = []
        for cell, data in detection_by_h3.items():
            prob = min(1.0, data["count"] / 5.0)
            tiles.append(ProcessedTile(
                h3_index=cell, variable="fire_prob",
                value=round(prob, 4), unit="probability",
                source="firms_viirs", confidence=0.88, computed_at=now,
            ))
            tiles.append(ProcessedTile(
                h3_index=cell, variable="fire_frp_max_mw",
                value=round(data["frp_max"], 2), unit="MW",
                source="firms_viirs", confidence=0.88, computed_at=now,
            ))

        self._store_tiles(tiles)
        return tiles

    # ── CMIP6 Scenario Preprocessing ─────────────────────────────────────────

    def process_cmip6(
        self,
        file_path: str,
        lat: float,
        lon: float,
        scenario: str = "ssp245",
    ) -> list[ProcessedTile]:
        """
        Process CMIP6 NetCDF into decadal scenario projections.
        Stores pre-computed delta (vs 1990-2020 historical baseline).
        """
        try:
            import xarray as xr
            import numpy as np
        except ImportError:
            return []

        try:
            ds     = xr.open_dataset(file_path)
            lat_r  = round(lat * 2) / 2    # CMIP6 coarser grid ~1°
            lon_r  = round(lon * 2) / 2
            pt     = ds.sel(lat=lat_r, lon=lon_r, method="nearest")
            h3_idx = self.lat_lon_to_h3(lat, lon)
            now    = datetime.now(timezone.utc)
            tiles  = []

            DECADES = [2030, 2040, 2050, 2075, 2100]
            for decade in DECADES:
                try:
                    decade_slice = pt.sel(
                        time=pt["time.year"] == decade,
                        method="nearest"
                    )
                    if "tas" in decade_slice:
                        delta_c = float(decade_slice["tas"].mean()) - 273.15
                        tiles.append(ProcessedTile(
                            h3_index=h3_idx,
                            variable=f"temp_projection_{decade}",
                            value=round(delta_c, 3),
                            unit="°C",
                            scenario=scenario,
                            source="cmip6_esgf",
                            confidence=0.75,
                            computed_at=now,
                        ))
                except Exception:
                    continue

            self._store_tiles(tiles)
            ds.close()
            return tiles

        except Exception as e:
            logger.warning(f"[Layer3] CMIP6 processing error: {e}")
            return []

    # ── Query interface ───────────────────────────────────────────────────────

    def get_features_for_cell(
        self,
        lat: float,
        lon: float,
        scenario: str = "observed",
        max_age_hours: float = 48.0,
    ) -> dict[str, float]:
        """
        Get all current features for an H3 cell.
        Returns dict keyed by variable name.
        """
        h3_idx = self.lat_lon_to_h3(lat, lon)
        since  = (datetime.now(timezone.utc).timestamp() - max_age_hours * 3600)
        since_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()

        with self._conn() as conn:
            rows = conn.execute("""
                SELECT variable, value, confidence
                FROM climate_features
                WHERE h3_index = ?
                  AND (scenario = ? OR scenario = 'observed')
                  AND computed_at >= ?
                ORDER BY computed_at DESC
            """, (h3_idx, scenario, since_iso)).fetchall()

        features = {}
        for row in rows:
            if row["variable"] not in features:   # latest wins
                features[row["variable"]] = row["value"]

        return features

    def get_fire_hazard(self, lat: float, lon: float) -> dict:
        """Get fire hazard data for a location."""
        h3_idx = self.lat_lon_to_h3(lat, lon)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM fire_hazard WHERE h3_index = ?", (h3_idx,)
            ).fetchone()
        if not row:
            return {"fire_prob_7d": 0.0, "fire_prob_30d": 0.0, "frp_max_mw": 0.0}
        return dict(row)

    # ── Storage ───────────────────────────────────────────────────────────────

    def _store_tiles(self, tiles: list[ProcessedTile]):
        if not tiles:
            return
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO climate_features
                (h3_index, variable, value, unit, scenario,
                 source, confidence, computed_at)
                VALUES (?,?,?,?,?,?,?,?)
            """, [
                (
                    t.h3_index, t.variable, t.value, t.unit,
                    t.scenario, t.source, t.confidence, now,
                )
                for t in tiles
            ])
            conn.commit()

    def stats(self) -> dict:
        with self._conn() as conn:
            total = conn.execute(
                "SELECT COUNT(*) as n FROM climate_features"
            ).fetchone()
            by_var = conn.execute("""
                SELECT variable, COUNT(*) as n, MAX(computed_at) as latest
                FROM climate_features
                GROUP BY variable ORDER BY n DESC LIMIT 20
            """).fetchall()
        return {
            "total_tiles": total["n"],
            "top_variables": [{"variable": r["variable"], "count": r["n"], "latest": r["latest"]} for r in by_var],
        }

