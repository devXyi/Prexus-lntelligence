"""
data-engine/python/adapters/carbon_monitor.py
Meteorium Engine — Carbon Monitor Emissions Adapter
Prexus Intelligence · v2.0.0

Source       : Carbon Monitor  https://carbonmonitor.org
               Near-real-time CO₂ emissions (< 5 day latency)
               Data by sector × country, daily resolution
API docs     : https://carbonmonitor-india.ac.cn/api   (global endpoint)
               https://api.carbonmonitor.org            (primary)

No API key required. Publicly accessible.

Variables produced
──────────────────
  co2_intensity_norm     Country/sector CO₂ intensity normalised [0, 1]
  carbon_policy_risk     Transition risk score from emissions trajectory [0, 1]
  co2_ppm                Atmospheric CO₂ concentration (ppm) — from NOAA GML
                         Used as macro-level context, not asset-specific.

Architecture notes
──────────────────
  - Carbon Monitor provides daily CO₂ emissions by country × sector.
    Sectors: Power, Industry, Ground Transport, Residential, Domestic Aviation.
  - co2_intensity_norm: current 30-day mean emission vs 2019 baseline
    (IPCC reference year). Score = clamp(current/baseline, 0, 2) / 2.
    2019 baseline embedded per country to avoid extra API calls.
  - carbon_policy_risk: rate-of-change of emissions trajectory.
    Rising emissions → high policy risk (carbon price exposure).
    Falling emissions → lower risk. Window: 90-day slope.
  - NOAA GML Mauna Loa CO₂ feed: daily flask measurements.
    Used as global atmospheric context for transition risk framing.
  - Country extraction: reverse-geocode lat/lon to ISO-3166 alpha-2
    via a lightweight embedded lookup (Prexus primary markets only).
    Unmapped coordinates fall back to global mean.
  - All emissions values in MtCO₂/day. Normalisation against 2019 baseline
    follows IPCC AR6 WG3 methodology for transition risk assessment.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp

from adapters.base import BaseAdapter, TelemetryRecord

logger = logging.getLogger("meteorium.adapter.carbon_monitor")

# ── API endpoints ─────────────────────────────────────────────────────────────

_CM_API      = "https://api.carbonmonitor.org/api/data"
_NOAA_GML    = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"

_CM_SECTORS  = ["Power", "Industry", "Ground Transport", "Residential", "Domestic Aviation"]

# ── 2019 annual baseline emissions by country (MtCO₂/year → /day = /365) ──────
# Source: Global Carbon Project 2023, IEA 2023.
# These are total energy-related emissions, all sectors combined.
# MtCO₂/day values derived from annual totals.
_BASELINE_2019_MTCO2_DAY: dict[str, float] = {
    "CN": 27.40,    # China
    "US": 14.50,    # United States
    "IN":  6.88,    # India
    "RU":  4.70,    # Russia
    "JP":  3.16,    # Japan
    "DE":  1.96,    # Germany
    "KR":  1.60,    # South Korea
    "IR":  1.54,    # Iran
    "SA":  1.46,    # Saudi Arabia
    "CA":  1.44,    # Canada
    "BR":  1.18,    # Brazil
    "ZA":  1.16,    # South Africa
    "MX":  1.00,    # Mexico
    "AU":  0.94,    # Australia
    "ID":  0.84,    # Indonesia
    "GB":  0.82,    # United Kingdom
    "FR":  0.76,    # France
    "IT":  0.68,    # Italy
    "TR":  0.62,    # Turkey
    "PL":  0.58,    # Poland
    "TH":  0.52,    # Thailand
    "MY":  0.48,    # Malaysia
    "NG":  0.44,    # Nigeria
    "VN":  0.42,    # Vietnam
    "PK":  0.40,    # Pakistan
    "EG":  0.38,    # Egypt
    "UA":  0.36,    # Ukraine
    "AR":  0.34,    # Argentina
    "BD":  0.22,    # Bangladesh
    "GLOBAL": 8.49, # Global mean (109 countries / 365)
}

# ── Country bounding boxes for lightweight reverse-geocode ────────────────────
# Covers Prexus primary markets. Format: (iso2, lat_min, lat_max, lon_min, lon_max)
# Larger/simpler countries have one bbox; complex geographies have multiple.
_COUNTRY_BBOXES: list[tuple[str, float, float, float, float]] = [
    ("IN",  6.0,  37.0,  68.0,  97.0),   # India
    ("CN", 18.0,  53.5,  73.0, 135.0),   # China
    ("US", 24.0,  49.5, -125.0, -66.0),  # Continental US
    ("US", 51.0,  71.0, -168.0, -141.0), # Alaska
    ("BR", -34.0,  5.5, -74.0, -34.0),   # Brazil
    ("RU", 41.0,  81.0,  27.0, 180.0),   # Russia
    ("AU", -44.0, -10.0, 113.0, 154.0),  # Australia
    ("ZA", -35.0, -22.0,  16.0,  33.0),  # South Africa
    ("NG",  4.0,  14.0,   3.0,  15.0),   # Nigeria
    ("DE", 47.0,  55.0,   6.0,  15.0),   # Germany
    ("GB", 49.0,  61.0,  -8.0,   2.0),   # United Kingdom
    ("FR", 41.0,  51.0,  -5.0,   9.0),   # France
    ("JP", 30.0,  45.0, 129.0, 145.0),   # Japan
    ("KR", 34.0,  38.0, 126.0, 130.0),   # South Korea
    ("ID", -9.0,   6.0,  95.0, 141.0),   # Indonesia
    ("MY",  1.0,   7.5, 100.0, 119.0),   # Malaysia Peninsular
    ("TH",  5.5,  21.0,  97.0, 106.0),   # Thailand
    ("VN",  8.0,  24.0, 102.0, 110.0),   # Vietnam
    ("PK", 24.0,  37.0,  61.0,  77.0),   # Pakistan
    ("BD", 20.0,  27.0,  88.0,  93.0),   # Bangladesh
    ("EG", 22.0,  31.5,  25.0,  37.0),   # Egypt
    ("SA", 16.0,  32.0,  36.0,  56.0),   # Saudi Arabia
    ("IR", 25.0,  40.0,  44.0,  64.0),   # Iran
    ("TR", 36.0,  42.0,  26.0,  45.0),   # Turkey
    ("UA", 44.0,  52.5,  22.0,  40.0),   # Ukraine
    ("AR", -55.0, -21.0, -74.0, -53.0),  # Argentina
    ("MX", 14.0,  33.0, -118.0, -86.0),  # Mexico
    ("CA", 42.0,  83.0, -141.0, -52.0),  # Canada
    ("ZA", -35.0, -22.0,  16.0,  33.0),  # South Africa (dup for overlap)
]

def _country_from_coords(lat: float, lon: float) -> str:
    """Lightweight reverse-geocode to ISO-2 for Prexus primary markets."""
    for iso2, lat_min, lat_max, lon_min, lon_max in _COUNTRY_BBOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return iso2
    return "GLOBAL"


class CarbonMonitorAdapter(BaseAdapter):

    SOURCE_NAME            = "Carbon Monitor"
    REFRESH_INTERVAL_HOURS = 24.0    # daily emissions data; no point fetching more often
    TIMEOUT_SECONDS        = 25
    MAX_RETRIES            = 3

    _semaphore = asyncio.Semaphore(4)

    async def fetch(
        self,
        lat: float,
        lon: float,
        **kwargs,
    ) -> list[TelemetryRecord]:
        t0 = time.monotonic()
        try:
            raw     = await self._fetch_raw(lat, lon)
            records = self._compute_risk_records(lat, lon, raw)
            self._mark_success((time.monotonic() - t0) * 1000)
            logger.info("[CarbonMonitor] %.4f,%.4f → %d records", lat, lon, len(records))
            return records
        except Exception as e:
            self._mark_failure(str(e))
            return []

    async def _fetch_raw(self, lat: float, lon: float, **kwargs) -> dict:
        """
        Fetch 90 days of daily emissions for the asset's country.
        Returns: {country, emissions: [{date, value_mtco2, sector}], noaa_ppm: float}
        """
        country  = _country_from_coords(lat, lon)
        cm_data  = await self._fetch_carbon_monitor(country)
        noaa_ppm = await self._fetch_noaa_co2()
        return {
            "country":   country,
            "emissions": cm_data,
            "noaa_ppm":  noaa_ppm,
        }

    async def _fetch_carbon_monitor(self, country: str) -> list[dict]:
        """
        Fetch last 90 days of daily emissions from Carbon Monitor API.
        Returns list of {date_str, value_mtco2, sector}.
        """
        end_date   = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=90)

        params = {
            "countries": country if country != "GLOBAL" else "",
            "sectors":   "total",
            "since":     start_date.isoformat(),
            "until":     end_date.isoformat(),
        }

        last_e: Optional[Exception] = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                async with self._semaphore:
                    timeout = aiohttp.ClientTimeout(total=self.TIMEOUT_SECONDS)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(_CM_API, params=params) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(30)
                                raise RuntimeError("Rate limited")
                            if resp.status >= 500:
                                raise RuntimeError(f"CM server error {resp.status}")
                            if resp.status != 200:
                                text = await resp.text()
                                raise RuntimeError(f"CM HTTP {resp.status}: {text[:200]}")
                            payload = await resp.json()
                            return self._parse_cm_response(payload, country)

            except Exception as e:
                last_e = e
                if attempt < self.MAX_RETRIES:
                    await asyncio.sleep(2 ** (attempt - 1))

        logger.warning("[CarbonMonitor] CM API failed: %s — using baseline only", last_e)
        return []   # Graceful: caller uses embedded 2019 baseline

    def _parse_cm_response(self, payload: dict | list, country: str) -> list[dict]:
        """
        Carbon Monitor API returns either:
          {"data": [{timestamp, value, ...}]}  or  [{...}, ...]
        Normalise to list of {date_str, value_mtco2}.
        """
        rows = payload if isinstance(payload, list) else payload.get("data", [])
        result = []
        for row in rows:
            try:
                date_str = (
                    row.get("timestamp") or
                    row.get("date")      or
                    row.get("time",  "")
                ).split("T")[0]
                val = float(row.get("value", 0) or row.get("emission", 0) or 0)
                result.append({"date": date_str, "value_mtco2": val})
            except (ValueError, TypeError, AttributeError):
                continue
        return sorted(result, key=lambda x: x["date"])

    async def _fetch_noaa_co2(self) -> float:
        """
        Fetch latest Mauna Loa CO₂ reading from NOAA GML text file.
        Returns ppm as float, or known-good fallback on error.
        """
        # Known CO₂ level fallback if NOAA unreachable (updated periodically)
        _FALLBACK_PPM = 424.5   # approximate 2025 Mauna Loa mean

        try:
            async with self._semaphore:
                timeout = aiohttp.ClientTimeout(total=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(_NOAA_GML) as resp:
                        if resp.status != 200:
                            return _FALLBACK_PPM
                        text = await resp.text()
                        # File format: year month day decimal_date ppm
                        # Last non-comment, non-negative line is most recent
                        lines = [
                            l for l in text.splitlines()
                            if l and not l.startswith("#")
                        ]
                        for line in reversed(lines):
                            parts = line.split()
                            if len(parts) >= 5:
                                try:
                                    ppm = float(parts[4])
                                    if ppm > 0:
                                        return ppm
                                except ValueError:
                                    continue
                        return _FALLBACK_PPM

        except Exception:
            return _FALLBACK_PPM

    # ── Risk variable computation ─────────────────────────────────────────────

    def _compute_risk_records(
        self,
        lat:        float,
        lon:        float,
        raw:        dict,
    ) -> list[TelemetryRecord]:

        country    = raw["country"]
        emissions  = raw["emissions"]       # list[{date, value_mtco2}]
        noaa_ppm   = raw["noaa_ppm"]
        now        = datetime.now(timezone.utc)

        baseline   = _BASELINE_2019_MTCO2_DAY.get(country, _BASELINE_2019_MTCO2_DAY["GLOBAL"])
        records: list[TelemetryRecord] = []

        # ── Compute trajectory from available emissions data ───────────────
        has_live_data = bool(emissions)
        recent_30     = [e["value_mtco2"] for e in emissions[-30:]] if emissions else []
        recent_90     = [e["value_mtco2"] for e in emissions]        if emissions else []

        # ── 1. co2_intensity_norm ──────────────────────────────────────────
        # Current 30-day mean vs 2019 baseline.
        # Score: 0.0 = emissions at 0% of baseline, 1.0 = 200%+ of baseline.
        if recent_30:
            mean_30    = sum(recent_30) / len(recent_30)
            intensity  = mean_30 / max(baseline, 0.001)
            # Clamp to [0, 2]; normalise to [0, 1]
            norm_score = min(1.0, max(0.0, intensity / 2.0))
            data_conf  = 0.87
            freshness  = 2.0    # Carbon Monitor latency ≈ 2-5 days
        else:
            # No live data — use baseline-derived neutral score
            norm_score = 0.45   # assume near-baseline
            data_conf  = 0.50
            freshness  = 72.0

        records.append(TelemetryRecord(
            source          = self.SOURCE_NAME,
            variable        = "co2_intensity_norm",
            lat=lat, lon=lon,
            value           = round(norm_score, 4),
            unit            = "index [0–1]",
            timestamp       = now,
            confidence      = data_conf,
            freshness_hours = freshness,
            metadata        = {
                "country":          country,
                "baseline_2019":    round(baseline, 3),
                "mean_30d_mtco2":   round(sum(recent_30) / len(recent_30), 3)
                                    if recent_30 else None,
                "source_records":   len(emissions),
            },
        ))

        # ── 2. carbon_policy_risk ─────────────────────────────────────────
        # Measures the TRAJECTORY — rising emissions = high policy risk.
        # Computed as 90-day linear slope normalised to ±1.
        # Positive slope (rising) → high risk. Negative → risk declining.
        if len(recent_90) >= 14:
            slope = self._linear_slope(recent_90)
            # Slope in MtCO₂/day per day. Normalise: ±0.05 Mt/day² = extreme.
            slope_norm     = max(-1.0, min(1.0, slope / 0.05))
            # Convert signed slope to [0, 1] risk: 0.5 = flat, 1.0 = sharply rising
            policy_risk    = 0.5 + slope_norm * 0.5
            policy_conf    = 0.82
        elif has_live_data:
            # Insufficient window — use intensity as proxy
            policy_risk = norm_score * 0.8
            policy_conf = 0.60
        else:
            policy_risk = 0.40
            policy_conf = 0.45

        records.append(TelemetryRecord(
            source          = self.SOURCE_NAME,
            variable        = "carbon_policy_risk",
            lat=lat, lon=lon,
            value           = round(policy_risk, 4),
            unit            = "index [0–1]",
            timestamp       = now,
            confidence      = policy_conf,
            freshness_hours = freshness,
            metadata        = {
                "country":        country,
                "trajectory_days":len(recent_90),
                "interpretation": (
                    "rising"  if policy_risk > 0.60 else
                    "stable"  if policy_risk > 0.40 else
                    "falling"
                ),
            },
        ))

        # ── 3. co2_ppm — atmospheric context ─────────────────────────────
        # Mauna Loa reading provides macro-level transition risk framing.
        # Normalised: 280 ppm (pre-industrial) = 0.0, 560 ppm (2×PI) = 1.0.
        ppm_norm = min(1.0, max(0.0, (noaa_ppm - 280.0) / 280.0))
        records.append(TelemetryRecord(
            source          = self.SOURCE_NAME,
            variable        = "co2_ppm",
            lat=lat, lon=lon,
            value           = round(noaa_ppm, 2),
            unit            = "ppm",
            timestamp       = now,
            confidence      = 0.99,   # NOAA GML is authoritative
            freshness_hours = 1.0,
            metadata        = {
                "station":     "Mauna Loa Observatory, NOAA GML",
                "pre_ind_ppm": 280.0,
                "normalised":  round(ppm_norm, 4),
            },
        ))

        return records

    @staticmethod
    def _linear_slope(values: list[float]) -> float:
        """
        Ordinary least-squares slope of a time series.
        Returns MtCO₂/day per day (rate of change).
        """
        n = len(values)
        if n < 2:
            return 0.0
        xs    = list(range(n))
        mean_x = (n - 1) / 2.0
        mean_y = sum(values) / n
        num    = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
        den    = sum((x - mean_x) ** 2 for x in xs)
        return num / den if den > 0 else 0.0
