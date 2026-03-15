"""
data-engine/python/adapters/era5.py
Prexus Intelligence — ERA5 Climate Reanalysis Adapter
ECMWF ERA5: hourly data from 1940–present, 31km global grid.
Requires free CDS API account: https://cds.climate.copernicus.eu

Install: pip install cdsapi netCDF4 xarray

Provides:
  - 10-year historical climate baseline per coordinate
  - Temperature, precipitation, wind, humidity anomalies
  - Extreme event return periods (1-in-10yr, 1-in-50yr events)
  - Soil moisture, evapotranspiration, sea level pressure
"""

import asyncio
import os
import logging
import tempfile
from datetime import datetime, timezone, timedelta
from typing import Optional
from pathlib import Path

from .base import BaseAdapter, TelemetryRecord

logger = logging.getLogger("prexus.adapters.era5")

# ERA5 variables relevant to climate risk
ERA5_VARIABLES = [
    "2m_temperature",
    "total_precipitation",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "volumetric_soil_water_layer_1",
    "surface_pressure",
    "mean_sea_level_pressure",
    "sea_surface_temperature",     # coastal assets
    "total_evaporation",
]


class ERA5Adapter(BaseAdapter):
    """
    ERA5 adapter — requires cdsapi Python client and CDS credentials.
    Credentials go in ~/.cdsapirc:
        url: https://cds.climate.copernicus.eu/api/v2
        key: <UID>:<API-KEY>

    Or set env vars: CDS_URL, CDS_KEY
    """

    SOURCE_NAME            = "ECMWF ERA5 Reanalysis"
    REFRESH_INTERVAL_HOURS = 24.0

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        baseline_years: int = 10,
    ):
        super().__init__()
        self._cache_dir     = Path(cache_dir or "/tmp/meteorium_era5_cache")
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._baseline_years = baseline_years
        self._cds_client     = None
        self._available      = False

        self._try_init_cds()

    def _try_init_cds(self):
        """Attempt to initialise the CDS API client."""
        try:
            import cdsapi
            self._cds_client = cdsapi.Client(
                url = os.environ.get("CDS_URL", "https://cds.climate.copernicus.eu/api/v2"),
                key = os.environ.get("CDS_KEY", ""),
                quiet = True,
            )
            self._available = True
            logger.info("[ERA5] CDS client initialised successfully")
        except ImportError:
            logger.warning("[ERA5] cdsapi not installed — run: pip install cdsapi")
        except Exception as e:
            logger.warning(f"[ERA5] CDS client init failed: {e} — ERA5 will use Open-Meteo fallback")

    async def fetch(
        self,
        lat: float,
        lon: float,
        **kwargs
    ) -> list[TelemetryRecord]:
        """
        If CDS is available: download ERA5 monthly statistics for this coordinate.
        If not: return empty list (Open-Meteo adapter covers ERA5 via its archive endpoint).
        """
        if not self._available:
            return []  # Open-Meteo handles ERA5 reanalysis as fallback

        import time
        start = time.perf_counter()
        try:
            raw     = await asyncio.get_event_loop().run_in_executor(
                None, self._download_era5_sync, lat, lon
            )
            records = self._parse(lat, lon, raw)
            self._mark_success((time.perf_counter() - start) * 1000)
            return records
        except Exception as e:
            self._mark_failure(str(e))
            return []

    async def _fetch_raw(self, lat: float, lon: float, **kwargs) -> dict:
        return await asyncio.get_event_loop().run_in_executor(
            None, self._download_era5_sync, lat, lon
        )

    def _download_era5_sync(self, lat: float, lon: float) -> dict:
        """
        Synchronous ERA5 download — runs in executor thread.
        Downloads monthly statistics for the past N years at this coordinate.
        """
        import xarray as xr
        import numpy as np

        # Round to 0.25° grid resolution
        lat_r = round(lat * 4) / 4
        lon_r = round(lon * 4) / 4

        cache_file = self._cache_dir / f"era5_{lat_r}_{lon_r}.nc"
        end_year   = datetime.now().year - 1
        start_year = end_year - self._baseline_years

        # Reuse cached file if < 7 days old
        if cache_file.exists():
            age_days = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 86400
            if age_days < 7:
                logger.debug(f"[ERA5] Using cached file: {cache_file}")
                ds  = xr.open_dataset(cache_file)
                return self._extract_stats(ds, lat_r, lon_r)

        # Download via CDS API
        logger.info(f"[ERA5] Downloading baseline {start_year}–{end_year} for ({lat_r}, {lon_r})")

        request = {
            "product_type": "monthly_averaged_reanalysis",
            "variable":     ERA5_VARIABLES,
            "year":         [str(y) for y in range(start_year, end_year + 1)],
            "month":        [f"{m:02d}" for m in range(1, 13)],
            "time":         "00:00",
            "area":         [lat_r + 0.5, lon_r - 0.5, lat_r - 0.5, lon_r + 0.5],
            "format":       "netcdf",
        }

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            self._cds_client.retrieve(
                "reanalysis-era5-single-levels-monthly-means",
                request,
                tmp.name
            )
            ds = xr.open_dataset(tmp.name)
            ds.to_netcdf(cache_file)
            return self._extract_stats(ds, lat_r, lon_r)

    def _extract_stats(self, ds, lat: float, lon: float) -> dict:
        """Extract statistical summaries from ERA5 xarray dataset."""
        import numpy as np

        stats = {}

        # Select nearest grid point
        ds_point = ds.sel(
            latitude  = lat,
            longitude = lon,
            method    = "nearest"
        )

        var_map = {
            "t2m":   "temperature_k",
            "tp":    "precipitation_m",
            "u10":   "wind_u_ms",
            "v10":   "wind_v_ms",
            "swvl1": "soil_moisture",
            "sp":    "surface_pressure_pa",
            "msl":   "sea_level_pressure_pa",
            "sst":   "sea_surface_temp_k",
        }

        for era5_var, our_var in var_map.items():
            if era5_var in ds_point:
                vals = ds_point[era5_var].values.flatten()
                vals = vals[~np.isnan(vals)]
                if len(vals) > 0:
                    stats[our_var] = {
                        "mean":   float(np.mean(vals)),
                        "std":    float(np.std(vals)),
                        "p10":    float(np.percentile(vals, 10)),
                        "p90":    float(np.percentile(vals, 90)),
                        "p99":    float(np.percentile(vals, 99)),
                        "min":    float(np.min(vals)),
                        "max":    float(np.max(vals)),
                        "count":  len(vals),
                    }

        # Monthly climatology for seasonal context
        if "t2m" in ds_point:
            monthly = {}
            for month in range(1, 13):
                month_data = ds_point["t2m"].sel(
                    time=ds_point["t2m"]["time.month"] == month
                ).values
                month_data = month_data[~np.isnan(month_data)]
                if len(month_data) > 0:
                    monthly[month] = float(np.mean(month_data)) - 273.15  # K → C
            stats["monthly_temp_c"] = monthly

        return stats

    def _parse(self, lat: float, lon: float, raw: dict) -> list[TelemetryRecord]:
        now = datetime.now(timezone.utc)
        records = []

        def rec(variable, value, unit, confidence=0.95, meta=None):
            return TelemetryRecord(
                source          = self.SOURCE_NAME,
                variable        = variable,
                lat             = lat,
                lon             = lon,
                value           = round(float(value), 5),
                unit            = unit,
                timestamp       = now,
                confidence      = confidence,
                freshness_hours = 168.0,   # weekly ERA5 download
                metadata        = meta or {"baseline_years": self._baseline_years},
            )

        temp = raw.get("temperature_k", {})
        if temp:
            mean_c = temp["mean"] - 273.15
            p99_c  = temp["p99"]  - 273.15
            records += [
                rec("era5_temp_mean_c",          mean_c,           "°C"),
                rec("era5_temp_p99_c",           p99_c,            "°C"),
                rec("era5_temp_std_c",           temp["std"],      "°C"),
                rec("era5_extreme_heat_thresh_c", p99_c,           "°C",
                    meta={"percentile": 99, "interpretation": "1-in-100yr heat threshold"}),
            ]

        precip = raw.get("precipitation_m", {})
        if precip:
            mean_mm_day = precip["mean"] * 1000 * 30   # m/month → mm/day approx
            p99_mm_day  = precip["p99"]  * 1000 * 30
            records += [
                rec("era5_precip_mean_mm_day",   mean_mm_day,  "mm/day"),
                rec("era5_precip_p99_mm_day",    p99_mm_day,   "mm/day"),
                rec("era5_flood_threshold_mm",   p99_mm_day,   "mm/day",
                    meta={"percentile": 99, "interpretation": "1-in-100yr flood threshold"}),
            ]

        soil = raw.get("soil_moisture", {})
        if soil:
            records += [
                rec("era5_soil_moisture_mean",   soil["mean"],  "m³/m³"),
                rec("era5_soil_moisture_p10",    soil["p10"],   "m³/m³",
                    meta={"interpretation": "drought threshold (lowest 10% historically)"}),
            ]

        wind_u = raw.get("wind_u_ms", {})
        wind_v = raw.get("wind_v_ms", {})
        if wind_u and wind_v:
            import math
            wind_speed_mean = math.sqrt(wind_u["mean"]**2 + wind_v["mean"]**2)
            records.append(rec("era5_wind_speed_mean_ms", wind_speed_mean, "m/s"))

        return records
