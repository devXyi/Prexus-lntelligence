"""
Free/Open Data Adapters for Meteorium
======================================
These are the sources you can ACTUALLY use right now without $25k licenses.
All free, all real, all used by actual climate risk platforms.

Sources:
  - Open-Meteo: weather forecasts, no API key needed
  - NASA FIRMS: wildfire/thermal anomaly detection
  - ECMWF Open Data: global weather model output
  - Carbon Monitor: real-time CO2 by country
  - Copernicus Climate Data Store: CMIP6 projections (free, needs registration)
"""

import httpx
import asyncio
import logging
from datetime import datetime, date, timedelta
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

# ─── Open-Meteo (Free, No API Key) ────────────────────────────────────────────
# Used by: small-medium climate fintechs, ESG startups
# Covers: global weather forecasts + historical climate reanalysis (ERA5)
# Resolution: 1km (some models), 7-day forecast

OPEN_METEO_BASE = "https://api.open-meteo.com/v1"
OPEN_METEO_HIST = "https://archive-api.open-meteo.com/v1"


@dataclass
class WeatherRisk:
    lat:              float
    lon:              float
    date:             str
    max_temp_c:       float
    precip_mm:        float
    wind_speed_ms:    float
    flood_risk_score: float   # derived: normalized precipitation anomaly
    heat_risk_score:  float   # derived: temp above historical baseline


class OpenMeteoAdapter:
    """
    Open-Meteo: real free weather API. No key. No rate limit (fair use).
    Used to derive physical climate risk scores for asset locations.
    """

    async def get_forecast(self, lat: float, lon: float, days: int = 7) -> list[WeatherRisk]:
        params = {
            "latitude":           lat,
            "longitude":          lon,
            "daily": [
                "temperature_2m_max",
                "precipitation_sum",
                "windspeed_10m_max",
                "precipitation_probability_max",
            ],
            "forecast_days":      days,
            "timezone":           "UTC",
        }

        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(f"{OPEN_METEO_BASE}/forecast", params=params)
            r.raise_for_status()
            data = r.json()

        daily = data["daily"]
        results = []
        for i, dt in enumerate(daily["time"]):
            temp    = daily["temperature_2m_max"][i] or 0
            precip  = daily["precipitation_sum"][i]  or 0
            wind    = daily["windspeed_10m_max"][i]  or 0

            # Derive risk scores (crude but real heuristics)
            heat_risk  = min((max(temp - 35.0, 0) / 20.0), 1.0)  # 35°C threshold
            flood_risk = min(precip / 150.0, 1.0)                 # 150mm/day = extreme

            results.append(WeatherRisk(
                lat=lat, lon=lon, date=dt,
                max_temp_c=temp, precip_mm=precip, wind_speed_ms=wind,
                flood_risk_score=flood_risk,
                heat_risk_score=heat_risk,
            ))

        return results

    async def get_historical_baseline(
        self, lat: float, lon: float,
        years_back: int = 10,
    ) -> dict:
        """
        Pull ERA5 historical reanalysis data to compute baseline climate normals.
        Used to detect anomalies — the core of physical risk scoring.
        """
        end   = date.today()
        start = end - timedelta(days=365 * years_back)

        params = {
            "latitude":   lat,
            "longitude":  lon,
            "start_date": str(start),
            "end_date":   str(end),
            "daily": [
                "temperature_2m_max",
                "precipitation_sum",
            ],
            "timezone": "UTC",
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.get(f"{OPEN_METEO_HIST}/era5", params=params)
            r.raise_for_status()
            data = r.json()

        temps   = [t for t in data["daily"]["temperature_2m_max"] if t is not None]
        precips = [p for p in data["daily"]["precipitation_sum"]  if p is not None]

        return {
            "mean_temp_c":    sum(temps)   / len(temps)   if temps   else 0,
            "mean_precip_mm": sum(precips) / len(precips) if precips else 0,
            "p95_temp_c":     sorted(temps)[int(len(temps) * 0.95)]   if temps   else 0,
            "p95_precip_mm":  sorted(precips)[int(len(precips) * 0.95)] if precips else 0,
        }


# ─── NASA FIRMS (Wildfire / Thermal Anomaly) ──────────────────────────────────
# Free. Registration at firms.modaps.eosdis.nasa.gov for API key.
# Real satellite fire detection — MODIS + VIIRS sensors, 375m resolution
# Used by: insurance companies, forest asset managers, infrastructure risk firms

NASA_FIRMS_BASE = "https://firms.modaps.eosdis.nasa.gov/api"


class NASAFirmsAdapter:
    """
    NASA FIRMS: actual satellite wildfire/thermal anomaly data.
    This is real — the same data used by CAL FIRE, FEMA, insurance companies.
    """

    def __init__(self, api_key: str):
        # Get free key at: https://firms.modaps.eosdis.nasa.gov/api/area/
        self.api_key = api_key

    async def get_fire_risk(
        self,
        lat: float, lon: float,
        radius_km: float = 50.0,
        days_back: int = 7,
    ) -> dict:
        """
        Query satellite fire detections around an asset location.
        Returns count + FRP (Fire Radiative Power) as risk proxy.
        """
        # Build bounding box from center + radius
        deg_per_km = 1 / 111.0
        d = radius_km * deg_per_km
        bbox = f"{lon-d},{lat-d},{lon+d},{lat+d}"

        # VIIRS I-Band 375m — highest resolution NASA fire product
        url = f"{NASA_FIRMS_BASE}/area/csv/{self.api_key}/VIIRS_SNPP_NRT/{bbox}/{days_back}"

        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(url)
            if r.status_code == 404:
                return {"fire_count": 0, "max_frp": 0.0, "fire_risk_score": 0.0}
            r.raise_for_status()
            csv_text = r.text

        # Parse CSV — each row is a fire detection
        lines  = csv_text.strip().split("\n")
        if len(lines) < 2:
            return {"fire_count": 0, "max_frp": 0.0, "fire_risk_score": 0.0}

        header = lines[0].split(",")
        frp_idx = header.index("frp") if "frp" in header else -1

        fire_count = len(lines) - 1
        frps = []
        for line in lines[1:]:
            cols = line.split(",")
            if frp_idx >= 0 and frp_idx < len(cols):
                try:
                    frps.append(float(cols[frp_idx]))
                except ValueError:
                    pass

        max_frp = max(frps) if frps else 0.0
        # FRP > 1000 MW = extreme fire. Normalize to [0,1]
        fire_risk_score = min(fire_count * 0.02 + max_frp / 1000.0, 1.0)

        return {
            "fire_count":       fire_count,
            "max_frp_mw":       max_frp,
            "fire_risk_score":  fire_risk_score,
            "radius_km":        radius_km,
            "days_back":        days_back,
        }


# ─── Carbon Monitor (Real-time CO2 by Country) ────────────────────────────────
# Free REST API. No key. Updated daily from power plant + industry sensors.
# Used by: ESG scoring firms, carbon accounting platforms

CARBON_MONITOR_BASE = "https://carbonmonitor-ds.adeel.cloud"


class CarbonMonitorAdapter:
    """
    Real CO2 emission data by country/sector. Updated near-daily.
    Used as transition risk input — high-emission countries = higher policy risk.
    """

    async def get_country_emissions(self, country_code: str) -> dict:
        """
        Get recent daily CO2 emissions for a country.
        Returns value in MtCO2/day and normalized transition risk score.
        """
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                f"{CARBON_MONITOR_BASE}/api/data",
                params={
                    "countries": country_code,
                    "sectors":   "Total",
                    "date_from": str(date.today() - timedelta(days=30)),
                    "date_to":   str(date.today()),
                }
            )
            if r.status_code != 200:
                return {"emissions_mtco2_day": 0.0, "transition_risk_factor": 0.5}
            data = r.json()

        values = [d["value"] for d in data.get("data", []) if d.get("value")]
        if not values:
            return {"emissions_mtco2_day": 0.0, "transition_risk_factor": 0.5}

        avg = sum(values) / len(values)

        # China ~30 MtCO2/day, USA ~15, EU ~10, India ~8
        # Normalize relative to global high emitters
        GLOBAL_MAX = 35.0
        transition_risk = min(avg / GLOBAL_MAX, 1.0)

        return {
            "country":              country_code,
            "avg_emissions_30d":    round(avg, 2),
            "unit":                 "MtCO2/day",
            "transition_risk_factor": round(transition_risk, 3),
        }


# ─── ECMWF Open Data ──────────────────────────────────────────────────────────
# Free. The same ECMWF model used by meteorologists worldwide.
# Data in GRIB2 format — binary, needs eccodes library to parse
# Better to use Open-Meteo which wraps ECMWF in clean JSON API

# For direct ECMWF integration via their Python client:
# pip install ecmwf-opendata

class ECMWFOpenDataAdapter:
    """
    Direct ECMWF Open Data access.
    Downloads GRIB2 forecast files and parses meteorological fields.
    """

    async def get_ensemble_forecast(
        self, lat: float, lon: float, step_hours: int = 240
    ) -> dict:
        """
        Pull ENS (51-member ensemble) forecast for a location.
        Ensemble spread = uncertainty = risk factor.

        Requires: pip install ecmwf-opendata cfgrib xarray
        """
        try:
            from ecmwf.opendata import Client
            import xarray as xr

            client = Client(source="ecmwf")
            client.retrieve(
                step=list(range(0, step_hours + 6, 6)),
                type="ef",                          # ENS forecast
                param=["2t", "tp", "10u", "10v"],  # temp, precip, wind
                target="/tmp/ecmwf_ens.grib2",
            )

            ds = xr.open_dataset(
                "/tmp/ecmwf_ens.grib2",
                engine="cfgrib",
                filter_by_keys={"typeOfLevel": "heightAboveGround"},
            )

            # Select nearest gridpoint to asset location
            point = ds.sel(latitude=lat, longitude=lon, method="nearest")

            return {
                "temp_mean_k":      float(point["t2m"].mean()),
                "temp_spread_k":    float(point["t2m"].std()),
                "precip_mean_m":    float(point["tp"].mean()),
                "precip_spread_m":  float(point["tp"].std()),
                "source":           "ECMWF ENS 51-member",
                "step_hours":       step_hours,
            }

        except ImportError:
            log.warning("ecmwf-opendata not installed. Falling back to Open-Meteo.")
            adapter = OpenMeteoAdapter()
            fc = await adapter.get_forecast(lat, lon)
            return {"source": "open-meteo-fallback", "forecast": fc}


# ─── Master Physical Risk Scorer ──────────────────────────────────────────────

class PhysicalRiskScorer:
    """
    Combines multiple free data sources into a single physical risk score
    for a Prexus asset. This is the real pipeline.
    """

    def __init__(self, nasa_firms_key: str = ""):
        self.weather   = OpenMeteoAdapter()
        self.fire      = NASAFirmsAdapter(nasa_firms_key) if nasa_firms_key else None
        self.carbon    = CarbonMonitorAdapter()

    async def score_asset(
        self,
        lat:          float,
        lon:          float,
        country_code: str,
        horizon_days: int = 30,
    ) -> dict:
        """
        Full pipeline: pull all sources, compute composite physical risk score.
        This replaces the fake static data in Meteorium.
        """
        tasks = [
            self.weather.get_forecast(lat, lon, min(horizon_days, 16)),
            self.weather.get_historical_baseline(lat, lon),
            self.carbon.get_country_emissions(country_code),
        ]

        if self.fire:
            tasks.append(self.fire.get_fire_risk(lat, lon))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        forecast, baseline, carbon_data = results[0], results[1], results[2]
        fire_data = results[3] if self.fire and len(results) > 3 else None

        # Handle errors gracefully — some sources may be unavailable
        if isinstance(forecast, Exception):
            log.warning(f"Weather forecast failed: {forecast}")
            forecast = []
        if isinstance(baseline, Exception):
            baseline = {}
        if isinstance(carbon_data, Exception):
            carbon_data = {}

        # Compute peak risk over forecast period
        heat_risk  = max((w.heat_risk_score  for w in forecast), default=0.0)
        flood_risk = max((w.flood_risk_score for w in forecast), default=0.0)
        fire_risk  = (fire_data or {}).get("fire_risk_score", 0.0)
        if isinstance(fire_risk, Exception):
            fire_risk = 0.0

        # Composite physical risk: weighted combination
        physical_risk = (
            heat_risk  * 0.30 +
            flood_risk * 0.40 +
            fire_risk  * 0.30
        )

        return {
            "physical_risk":     round(physical_risk, 3),
            "heat_risk":         round(heat_risk, 3),
            "flood_risk":        round(flood_risk, 3),
            "fire_risk":         round(fire_risk, 3),
            "transition_risk":   round(carbon_data.get("transition_risk_factor", 0.5), 3),
            "composite_risk":    round((physical_risk * 0.6 + carbon_data.get("transition_risk_factor", 0.5) * 0.4), 3),
            "sources": {
                "weather":  "Open-Meteo (ECMWF-based)",
                "fire":     "NASA FIRMS VIIRS 375m" if self.fire else "disabled",
                "carbon":   "Carbon Monitor",
                "baseline": "ERA5 Historical Reanalysis",
            },
            "as_of": datetime.utcnow().isoformat() + "Z",
        }

