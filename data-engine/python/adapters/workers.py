"""
layer1/workers.py
Meteorium Engine — LAYER 1: Telemetry Acquisition Workers
Purpose-built async workers for each data source.
Each worker: discover → fetch → validate → deposit to lake.
Workers never crash the pipeline — they degrade gracefully.
"""

import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import httpx

from core.config import (
    LAKE_STRUCTURE, NASA_FIRMS_KEY, FRESHNESS,
    CDS_KEY, CDS_URL
)
from core.models import IngestRecord, TelemetryRecord
from layer2.lake import DataLake

logger = logging.getLogger("meteorium.layer1")


# ════════════════════════════════════════════════════════════════════════════
# BASE WORKER
# ════════════════════════════════════════════════════════════════════════════

class BaseWorker:
    SOURCE_ID:      str   = "unknown"
    TIMEOUT:        int   = 30
    MAX_RETRIES:    int   = 3
    RETRY_DELAY:    float = 2.0

    def __init__(self, lake: DataLake):
        self.lake         = lake
        self._fetch_count = 0
        self._fail_count  = 0
        self._last_run:   Optional[datetime] = None
        self._last_error: Optional[str]      = None

    async def run(self, **kwargs) -> Optional[IngestRecord]:
        """Top-level entry point — retries on failure."""
        start = time.perf_counter()
        for attempt in range(self.MAX_RETRIES):
            try:
                record = await self._run_once(**kwargs)
                self._fetch_count += 1
                self._last_run    = datetime.now(timezone.utc)
                self._last_error  = None
                elapsed = time.perf_counter() - start
                logger.info(f"[{self.SOURCE_ID}] ✓ ingested in {elapsed:.1f}s")
                return record
            except Exception as e:
                self._last_error = str(e)
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY * (attempt + 1))
                    logger.warning(f"[{self.SOURCE_ID}] attempt {attempt+1} failed: {e} — retrying")
                else:
                    self._fail_count += 1
                    logger.error(f"[{self.SOURCE_ID}] ✗ all retries failed: {e}")
        return None

    async def _run_once(self, **kwargs) -> IngestRecord:
        raise NotImplementedError

    @property
    def health(self) -> dict:
        rate = self._fail_count / max(self._fetch_count, 1)
        status = "nominal" if rate < 0.2 else "degraded" if rate < 0.8 else "offline"
        return {
            "source":     self.SOURCE_ID,
            "status":     status,
            "last_run":   self._last_run.isoformat() if self._last_run else None,
            "last_error": self._last_error,
            "fetches":    self._fetch_count,
            "failures":   self._fail_count,
        }

    @staticmethod
    def _hash_content(content: bytes) -> str:
        return hashlib.md5(content).hexdigest()

    @staticmethod
    def _size_mb(content: bytes) -> float:
        return len(content) / (1024 * 1024)


# ════════════════════════════════════════════════════════════════════════════
# OPEN-METEO WORKER (Weather forecast + ERA5 baseline)
# ════════════════════════════════════════════════════════════════════════════

class OpenMeteoWorker(BaseWorker):
    SOURCE_ID = "open_meteo_forecast"

    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
    ERA5_URL     = "https://archive-api.open-meteo.com/v1/archive"

    HOURLY_VARS = [
        "temperature_2m", "precipitation", "wind_speed_10m",
        "relative_humidity_2m", "surface_pressure",
        "soil_moisture_0_1cm", "et0_fao_evapotranspiration",
        "wind_direction_10m", "cloud_cover",
    ]
    DAILY_VARS = [
        "temperature_2m_max", "temperature_2m_min",
        "precipitation_sum", "wind_speed_10m_max",
        "wind_gusts_10m_max", "precipitation_hours",
        "et0_fao_evapotranspiration", "sunshine_duration",
    ]

    async def _run_once(
        self,
        lat: float,
        lon: float,
        include_baseline: bool = True,
        **kwargs
    ) -> IngestRecord:

        async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
            params = {
                "latitude":        lat,
                "longitude":       lon,
                "hourly":          ",".join(self.HOURLY_VARS),
                "daily":           ",".join(self.DAILY_VARS),
                "timezone":        "UTC",
                "forecast_days":   7,
                "wind_speed_unit": "ms",
                "models":          "best_match",
            }
            resp = await client.get(self.FORECAST_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

            # ERA5 baseline (last 10 years)
            if include_baseline:
                today  = datetime.now(timezone.utc).date()
                start  = (today - timedelta(days=365 * 10)).isoformat()
                end    = (today - timedelta(days=5)).isoformat()

                era5p = {
                    "latitude":       lat, "longitude": lon,
                    "start_date":     start, "end_date": end,
                    "daily":          "temperature_2m_max,temperature_2m_min,"
                                      "precipitation_sum,wind_speed_10m_max",
                    "timezone":       "UTC",
                    "wind_speed_unit": "ms",
                }
                era5r = await client.get(self.ERA5_URL, params=era5p)
                if era5r.status_code == 200:
                    data["era5_baseline"] = era5r.json()

        content = json.dumps(data).encode()
        now     = datetime.now(timezone.utc)
        path    = LAKE_STRUCTURE["era5"] / f"openmeteo_{lat}_{lon}_{now.strftime('%Y%m%d_%H')}.json"
        path.write_bytes(content)

        return IngestRecord(
            source_id    = self.SOURCE_ID,
            file_path    = str(path),
            bbox         = (lon - 0.5, lat - 0.5, lon + 0.5, lat + 0.5),
            time_start   = now - timedelta(hours=1),
            time_end     = now + timedelta(days=7),
            file_size_mb = self._size_mb(content),
            file_hash    = self._hash_content(content),
            record_count = len(data.get("hourly", {}).get("time", [])),
            ingested_at  = now,
        )

    async def fetch_features(self, lat: float, lon: float) -> list[TelemetryRecord]:
        """
        High-level call: fetch + parse into TelemetryRecords.
        Used directly by Layer 4 feature extraction.
        """
        now = datetime.now(timezone.utc)
        try:
            async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
                params = {
                    "latitude": lat, "longitude": lon,
                    "hourly":   ",".join(self.HOURLY_VARS),
                    "daily":    ",".join(self.DAILY_VARS),
                    "timezone": "UTC", "forecast_days": 7,
                    "wind_speed_unit": "ms",
                }
                resp = await client.get(self.FORECAST_URL, params=params)
                resp.raise_for_status()
                fc   = resp.json()

                # ERA5 10yr baseline
                today = now.date()
                era5r = await client.get(self.ERA5_URL, params={
                    "latitude": lat, "longitude": lon,
                    "start_date": (today - timedelta(days=365*10)).isoformat(),
                    "end_date":   (today - timedelta(days=5)).isoformat(),
                    "daily": "temperature_2m_max,precipitation_sum,wind_speed_10m_max",
                    "timezone": "UTC", "wind_speed_unit": "ms",
                })
                baseline = era5r.json() if era5r.status_code == 200 else {}

            return self._parse_to_records(lat, lon, fc, baseline, now)

        except Exception as e:
            logger.warning(f"[OpenMeteo] fetch_features failed: {e}")
            return []

    def _parse_to_records(
        self,
        lat: float, lon: float,
        fc: dict, baseline: dict,
        now: datetime
    ) -> list[TelemetryRecord]:

        hourly = fc.get("hourly", {})
        daily  = fc.get("daily", {})

        def safe(d, k, idx=0):
            vals = d.get(k, [])
            return float(vals[idx]) if vals and idx < len(vals) and vals[idx] is not None else 0.0

        def safe_list(d, k):
            return [v for v in d.get(k, []) if v is not None]

        temp      = safe(hourly, "temperature_2m")
        precip    = safe(hourly, "precipitation")
        wind      = safe(hourly, "wind_speed_10m")
        humidity  = safe(hourly, "relative_humidity_2m")
        soil      = safe(hourly, "soil_moisture_0_1cm") or 0.25
        temp_max7 = safe_list(daily, "temperature_2m_max")
        precip_7d = safe_list(daily, "precipitation_sum")
        wind_max7 = safe_list(daily, "wind_speed_10m_max")
        gusts     = safe_list(daily, "wind_gusts_10m_max")

        # Anomaly calculation
        bl_daily    = baseline.get("daily", {})
        bl_temps    = safe_list(bl_daily, "temperature_2m_max")
        bl_precip   = safe_list(bl_daily, "precipitation_sum")
        bl_wind     = safe_list(bl_daily, "wind_speed_10m_max")

        temp_anom   = (temp - sum(bl_temps)/len(bl_temps)) if bl_temps else 0.0
        precip_anom = 0.0
        if bl_precip:
            bl_mean   = sum(bl_precip) / len(bl_precip)
            rec_mean  = sum(precip_7d) / max(len(precip_7d), 1)
            precip_anom = ((rec_mean - bl_mean) / bl_mean * 100) if bl_mean > 0 else 0.0

        heat_days     = sum(1 for t in temp_max7 if t > 35.0)
        heat_prob     = min(heat_days / 7.0, 1.0)
        drought       = max(0.0, (0.3 - soil) / 0.3)
        if precip_anom < -30:
            drought   = min(1.0, drought + abs(precip_anom) / 200.0)
        extreme_wind  = sum(1 for w in gusts if w > 20.0) / max(len(gusts), 1)

        conf = 0.92

        def rec(var, val, unit):
            return TelemetryRecord(
                source=self.SOURCE_ID, variable=var, lat=lat, lon=lon,
                value=round(float(val), 5), unit=unit, timestamp=now,
                confidence=conf, freshness_hours=1.0,
            )

        return [
            rec("temperature_c",        temp,         "°C"),
            rec("precipitation_mm",      precip,       "mm/hr"),
            rec("wind_speed_ms",         wind,         "m/s"),
            rec("humidity_pct",          humidity,     "%"),
            rec("soil_moisture",         soil,         "m³/m³"),
            rec("temp_anomaly_c",        temp_anom,    "°C"),
            rec("precip_anomaly_pct",    precip_anom,  "%"),
            rec("heat_stress_prob_7d",   heat_prob,    "probability"),
            rec("drought_index",         drought,      "0-1"),
            rec("extreme_wind_prob_7d",  extreme_wind, "probability"),
            rec("temp_max_7d_c",         max(temp_max7, default=temp), "°C"),
            rec("precip_sum_7d_mm",      sum(precip_7d), "mm"),
        ]


# ════════════════════════════════════════════════════════════════════════════
# NASA FIRMS WORKER (Fire detections)
# ════════════════════════════════════════════════════════════════════════════

class FIRMSWorker(BaseWorker):
    SOURCE_ID = "firms_viirs"

    BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

    def __init__(self, lake: DataLake, api_key: Optional[str] = None):
        super().__init__(lake)
        self.api_key = api_key or NASA_FIRMS_KEY

    async def _run_once(
        self,
        bbox: tuple,            # (minlon, minlat, maxlon, maxlat)
        days_back: int = 10,
        satellite: str = "VIIRS_SNPP_NRT",
        **kwargs
    ) -> IngestRecord:

        minlon, minlat, maxlon, maxlat = bbox
        area = f"{minlon},{minlat},{maxlon},{maxlat}"
        url  = f"{self.BASE_URL}/{self.api_key}/{satellite}/{area}/{days_back}"

        async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            content = resp.content

        now  = datetime.now(timezone.utc)
        path = LAKE_STRUCTURE["firms"] / f"viirs_{now.strftime('%Y%m%d_%H%M')}.csv"
        path.write_bytes(content)

        # Count valid rows
        rows = list(csv.DictReader(io.StringIO(content.decode())))

        return IngestRecord(
            source_id    = self.SOURCE_ID,
            file_path    = str(path),
            bbox         = bbox,
            time_start   = now - timedelta(days=days_back),
            time_end     = now,
            file_size_mb = self._size_mb(content),
            file_hash    = self._hash_content(content),
            record_count = len(rows),
            ingested_at  = now,
        )

    async def fetch_features(
        self,
        lat: float,
        lon: float,
        radius_km: float = 100.0,
        days_back: int = 30,
    ) -> list[TelemetryRecord]:
        """Fetch fire detections and return TelemetryRecords."""
        import math
        deg_lat = radius_km / 111.0
        deg_lon = radius_km / (111.0 * math.cos(math.radians(lat)))
        area    = f"{lon-deg_lon},{lat-deg_lat},{lon+deg_lon},{lat+deg_lat}"

        all_detections = []

        async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
            for sat in ["VIIRS_SNPP_NRT", "MODIS_NRT"]:
                try:
                    url  = f"{self.BASE_URL}/{self.api_key}/{sat}/{area}/{min(days_back,10)}"
                    resp = await client.get(url)
                    if resp.status_code != 200 or not resp.text.strip():
                        continue
                    for row in csv.DictReader(io.StringIO(resp.text)):
                        try:
                            dlat = float(row.get("latitude", 0))
                            dlon = float(row.get("longitude", 0))
                            frp  = float(row.get("frp", 0))
                            conf = row.get("confidence", "n").lower()
                            if conf not in ("n", "h", "nominal", "high"):
                                continue
                            dist = self._haversine(lat, lon, dlat, dlon)
                            all_detections.append({
                                "lat": dlat, "lon": dlon, "frp": frp,
                                "dist_km": dist,
                                "conf": 1.0 if conf in ("h", "high") else 0.8,
                            })
                        except (ValueError, KeyError):
                            continue
                except Exception as e:
                    logger.debug(f"[FIRMS] {sat} error: {e}")

        return self._to_records(lat, lon, all_detections)

    def _to_records(
        self,
        lat: float,
        lon: float,
        detections: list,
    ) -> list[TelemetryRecord]:
        import math
        now = datetime.now(timezone.utc)

        d25  = [d for d in detections if d["dist_km"] <= 25]
        d100 = [d for d in detections if d["dist_km"] <= 100]
        d250 = [d for d in detections if d["dist_km"] <= 250]

        fire_prob_25  = min(1.0, len(d25)  / 3.0)
        fire_prob_100 = min(1.0, len(d100) / 10.0)
        fire_prob_250 = min(1.0, len(d250) / 25.0)
        frp_max       = max((d["frp"] for d in d100), default=0.0)

        hazard = 0.0
        for d in detections:
            if d["dist_km"] > 0:
                hazard += d["frp"] * math.exp(-d["dist_km"] / 50.0) * d["conf"]
        hazard_norm = min(1.0, hazard / 500.0)

        conf = 0.88 if detections else 0.0

        def rec(var, val, unit):
            return TelemetryRecord(
                source=self.SOURCE_ID, variable=var,
                lat=lat, lon=lon, value=round(float(val), 5),
                unit=unit, timestamp=now,
                confidence=conf, freshness_hours=3.0,
            )

        return [
            rec("fire_prob_25km",          fire_prob_25,  "probability"),
            rec("fire_prob_100km",         fire_prob_100, "probability"),
            rec("fire_prob_250km",         fire_prob_250, "probability"),
            rec("fire_hazard_score",       hazard_norm,   "0-1"),
            rec("fire_radiative_power_mw", frp_max,       "MW"),
            rec("fire_count_100km",        float(len(d100)), "count"),
        ]

    @staticmethod
    def _haversine(lat1, lon1, lat2, lon2) -> float:
        import math
        R  = 6371.0
        f1 = math.radians(lat1); f2 = math.radians(lat2)
        df = math.radians(lat2-lat1); dl = math.radians(lon2-lon1)
        a  = math.sin(df/2)**2 + math.cos(f1)*math.cos(f2)*math.sin(dl/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


# ════════════════════════════════════════════════════════════════════════════
# CARBON MONITOR WORKER
# ════════════════════════════════════════════════════════════════════════════

class CarbonMonitorWorker(BaseWorker):
    SOURCE_ID = "carbon_monitor"

    SECTOR_WEIGHTS = {
        "Power Industry": 0.92, "Industry": 0.78,
        "Ground Transport": 0.65, "Residential": 0.42,
        "Aviation": 0.70, "International Aviation": 0.75,
    }

    def __init__(self, lake: DataLake):
        super().__init__(lake)
        self._cache: dict = {}

    async def _run_once(self, country_code: str = "IND", **kwargs) -> IngestRecord:
        from core.config import ISO3_TO_ISO2
        iso2 = ISO3_TO_ISO2.get(country_code, country_code[:2])

        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                "https://carbonmonitor.org/api/data",
                params={"country": iso2}
            )
            resp.raise_for_status()
            content = resp.content

        now  = datetime.now(timezone.utc)
        path = LAKE_STRUCTURE["carbon"] / f"carbon_{country_code}_{now.strftime('%Y%m%d')}.json"
        path.write_bytes(content)

        return IngestRecord(
            source_id    = self.SOURCE_ID,
            file_path    = str(path),
            bbox         = (-180, -90, 180, 90),
            time_start   = now - timedelta(days=370),
            time_end     = now - timedelta(days=5),
            file_size_mb = self._size_mb(content),
            file_hash    = self._hash_content(content),
            record_count = 1,
            ingested_at  = now,
        )

    async def fetch_features(
        self,
        lat: float,
        lon: float,
        country_code: str = "IND",
    ) -> list[TelemetryRecord]:
        from core.config import CARBON_INTENSITY, ISO3_TO_ISO2
        now = datetime.now(timezone.utc)

        # Try live API
        live_data = {}
        try:
            iso2 = ISO3_TO_ISO2.get(country_code, country_code[:2])
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    "https://carbonmonitor.org/api/data",
                    params={"country": iso2}
                )
                if resp.status_code == 200:
                    live_data = resp.json()
        except Exception as e:
            logger.debug(f"[Carbon] live API failed: {e}")

        intensity     = CARBON_INTENSITY.get(country_code, 0.5)
        intensity_norm = min(1.0, intensity / 0.91)

        # Parse sector data if available
        high_risk     = 0.0
        yoy_change    = 0.0
        data_points   = live_data.get("data", [])
        if data_points:
            sector_totals: dict = {}
            for pt in data_points:
                for sec, val in (pt.get("sectors") or {}).items():
                    try:
                        sector_totals[sec] = sector_totals.get(sec, 0) + float(val)
                    except (ValueError, TypeError):
                        pass
            total = max(sum(sector_totals.values()), 1.0)
            for sec, amt in sector_totals.items():
                w = self.SECTOR_WEIGHTS.get(sec, 0.5)
                high_risk += (amt / total) * w

        transition_risk = min(1.0, intensity_norm * 0.5 + high_risk * 0.5)
        policy_risk     = min(1.0, transition_risk + max(0, yoy_change/100) * 0.15)
        conf            = 0.85 if data_points else 0.55
        freshness       = 24.0

        def rec(var, val, unit):
            return TelemetryRecord(
                source=self.SOURCE_ID, variable=var,
                lat=lat, lon=lon, value=round(float(val), 6),
                unit=unit, timestamp=now,
                confidence=conf, freshness_hours=freshness,
            )

        return [
            rec("co2_intensity_norm",       intensity_norm,  "0-1"),
            rec("co2_intensity_tco2_mwh",   intensity,       "tCO2/MWh"),
            rec("transition_risk_score",    transition_risk, "0-1"),
            rec("carbon_policy_risk",       policy_risk,     "0-1"),
            rec("emissions_yoy_change_pct", yoy_change,      "%"),
            rec("high_risk_sector_share",   high_risk,       "0-1"),
        ]


# ════════════════════════════════════════════════════════════════════════════
# ERA5 CDS WORKER (full reanalysis via Copernicus)
# ════════════════════════════════════════════════════════════════════════════

class ERA5Worker(BaseWorker):
    SOURCE_ID = "era5_cds"
    TIMEOUT   = 300    # ERA5 downloads can take minutes

    def __init__(self, lake: DataLake, cds_key: str = ""):
        super().__init__(lake)
        self.cds_key   = cds_key or CDS_KEY
        self.available = False
        self._try_init()

    def _try_init(self):
        try:
            import cdsapi
            self._client = cdsapi.Client(
                url=CDS_URL, key=self.cds_key, quiet=True
            )
            self.available = True
            logger.info("[ERA5] CDS client ready")
        except Exception as e:
            logger.warning(f"[ERA5] CDS unavailable: {e}")

    async def _run_once(
        self,
        lat: float,
        lon: float,
        years: int = 10,
        **kwargs
    ) -> IngestRecord:
        if not self.available:
            raise RuntimeError("CDS client not configured")

        record = await asyncio.get_event_loop().run_in_executor(
            None, self._download_sync, lat, lon, years
        )
        return record

    def _download_sync(self, lat: float, lon: float, years: int) -> IngestRecord:
        import tempfile
        now       = datetime.now(timezone.utc)
        end_year  = now.year - 1
        start_year = end_year - years
        lat_r     = round(lat * 4) / 4
        lon_r     = round(lon * 4) / 4

        cache_path = LAKE_STRUCTURE["era5"] / f"era5_monthly_{lat_r}_{lon_r}.nc"

        if cache_path.exists():
            age_days = (time.time() - cache_path.stat().st_mtime) / 86400
            if age_days < 7:
                content = cache_path.read_bytes()
                return IngestRecord(
                    source_id=self.SOURCE_ID, file_path=str(cache_path),
                    bbox=(lon_r-0.5, lat_r-0.5, lon_r+0.5, lat_r+0.5),
                    time_start=now-timedelta(days=365*years), time_end=now,
                    file_size_mb=self._size_mb(content),
                    file_hash=self._hash_content(content),
                    record_count=years*12, ingested_at=now,
                )

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            self._client.retrieve(
                "reanalysis-era5-single-levels-monthly-means",
                {
                    "product_type": "monthly_averaged_reanalysis",
                    "variable": [
                        "2m_temperature", "total_precipitation",
                        "10m_u_component_of_wind", "10m_v_component_of_wind",
                        "volumetric_soil_water_layer_1",
                    ],
                    "year":   [str(y) for y in range(start_year, end_year+1)],
                    "month":  [f"{m:02d}" for m in range(1, 13)],
                    "time":   "00:00",
                    "area":   [lat_r+0.5, lon_r-0.5, lat_r-0.5, lon_r+0.5],
                    "format": "netcdf",
                },
                tmp.name
            )
            content = Path(tmp.name).read_bytes()
            cache_path.write_bytes(content)

        return IngestRecord(
            source_id=self.SOURCE_ID, file_path=str(cache_path),
            bbox=(lon_r-0.5, lat_r-0.5, lon_r+0.5, lat_r+0.5),
            time_start=now-timedelta(days=365*years), time_end=now,
            file_size_mb=self._size_mb(content),
            file_hash=self._hash_content(content),
            record_count=years*12, ingested_at=now,
        )

    async def fetch_features(self, lat: float, lon: float) -> list[TelemetryRecord]:
        """Parse ERA5 NetCDF into TelemetryRecords."""
        if not self.available:
            return []
        try:
            import xarray as xr
            import numpy as np
            lat_r = round(lat * 4) / 4
            lon_r = round(lon * 4) / 4
            cache = LAKE_STRUCTURE["era5"] / f"era5_monthly_{lat_r}_{lon_r}.nc"
            if not cache.exists():
                await self._run_once(lat, lon)
            if not cache.exists():
                return []

            ds     = xr.open_dataset(cache)
            now    = datetime.now(timezone.utc)
            pt     = ds.sel(latitude=lat_r, longitude=lon_r, method="nearest")
            records = []

            if "t2m" in pt:
                vals  = pt["t2m"].values.flatten()
                vals  = vals[~np.isnan(vals)]
                if len(vals):
                    records.append(TelemetryRecord(
                        source=self.SOURCE_ID, variable="era5_temp_mean_c",
                        lat=lat, lon=lon,
                        value=round(float(np.mean(vals)) - 273.15, 3),
                        unit="°C", timestamp=now,
                        confidence=0.95, freshness_hours=168.0,
                    ))
                    records.append(TelemetryRecord(
                        source=self.SOURCE_ID, variable="era5_temp_p99_c",
                        lat=lat, lon=lon,
                        value=round(float(np.percentile(vals, 99)) - 273.15, 3),
                        unit="°C", timestamp=now,
                        confidence=0.95, freshness_hours=168.0,
                    ))
            return records
        except Exception as e:
            logger.warning(f"[ERA5] parse error: {e}")
            return []


# ════════════════════════════════════════════════════════════════════════════
# WORKER REGISTRY — instantiated at startup
# ════════════════════════════════════════════════════════════════════════════

class WorkerRegistry:
    """Manages all ingestion workers."""

    def __init__(self, lake: DataLake):
        self.lake    = lake
        self.workers = {
            "open_meteo":     OpenMeteoWorker(lake),
            "firms":          FIRMSWorker(lake),
            "carbon_monitor": CarbonMonitorWorker(lake),
            "era5":           ERA5Worker(lake),
        }

    async def fetch_all_features(
        self,
        lat: float,
        lon: float,
        country_code: str = "IND",
    ) -> list[TelemetryRecord]:
        """
        Parallel fetch from all available workers.
        Returns combined TelemetryRecord list.
        """
        tasks = [
            self.workers["open_meteo"].fetch_features(lat, lon),
            self.workers["firms"].fetch_features(lat, lon),
            self.workers["carbon_monitor"].fetch_features(lat, lon, country_code=country_code),
        ]
        if self.workers["era5"].available:
            tasks.append(self.workers["era5"].fetch_features(lat, lon))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_records = []
        for r in results:
            if isinstance(r, list):
                all_records.extend(r)
            elif isinstance(r, Exception):
                logger.warning(f"Worker error: {r}")
        return all_records

    def health_report(self) -> dict:
        return {name: w.health for name, w in self.workers.items()}
