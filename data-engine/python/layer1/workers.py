"""
layer1/workers.py
Meteorium Engine — LAYER 1: Telemetry Worker Registry

Owns all adapter instances and orchestrates parallel fetch across sources.
This is the single entrypoint for raw telemetry ingestion.

Sources wired up:
  - Open-Meteo      (weather / climate — free, no key)
  - NASA FIRMS      (active fire / thermal anomaly — free)
  - Carbon Monitor  (CO₂ / emissions — free)

Adding a new source: create an adapter in adapters/, register it below.
"""

import asyncio
import logging
import time
from typing import Optional

from adapters.base import TelemetryRecord
from layer2.lake import DataLake

logger = logging.getLogger("meteorium.layer1")


# ─────────────────────────────────────────────────────────────────────────────
# Individual worker wrappers
# Each wraps one adapter and exposes a uniform fetch_features() interface.
# ─────────────────────────────────────────────────────────────────────────────

class OpenMeteoWorker:
    """Wraps the Open-Meteo weather adapter."""

    SOURCE_ID = "open_meteo"

    def __init__(self, lake: DataLake):
        self._lake = lake
        self._last_ok: Optional[float] = None
        self._last_err: Optional[str] = None
        try:
            from adapters.open_meteo import OpenMeteoAdapter
            self._adapter = OpenMeteoAdapter(lake)
        except Exception as e:
            self._adapter = None
            self._last_err = str(e)
            logger.warning(f"[OpenMeteo] adapter init failed: {e}")

    async def fetch_features(
        self,
        lat: float,
        lon: float,
        country_code: str = "IND",
    ) -> list[TelemetryRecord]:
        if self._adapter is None:
            return []
        try:
            records = await self._adapter.fetch(lat, lon, country_code=country_code)
            self._last_ok = time.time()
            self._last_err = None
            return records or []
        except Exception as e:
            self._last_err = str(e)
            logger.debug(f"[OpenMeteo] fetch({lat},{lon}): {e}")
            return []

    def health(self) -> dict:
        return {
            "source":    self.SOURCE_ID,
            "available": self._adapter is not None,
            "last_ok":   self._last_ok,
            "last_err":  self._last_err,
        }


class FIRMSWorker:
    """Wraps the NASA FIRMS fire/thermal adapter."""

    SOURCE_ID = "firms"

    def __init__(self, lake: DataLake):
        self._lake = lake
        self._last_ok: Optional[float] = None
        self._last_err: Optional[str] = None
        try:
            from adapters.firms import FIRMSAdapter
            self._adapter = FIRMSAdapter(lake)
        except Exception as e:
            self._adapter = None
            self._last_err = str(e)
            logger.warning(f"[FIRMS] adapter init failed: {e}")

    async def fetch_features(
        self,
        lat: float,
        lon: float,
        country_code: str = "IND",
    ) -> list[TelemetryRecord]:
        if self._adapter is None:
            return []
        try:
            records = await self._adapter.fetch(lat, lon, country_code=country_code)
            self._last_ok = time.time()
            self._last_err = None
            return records or []
        except Exception as e:
            self._last_err = str(e)
            logger.debug(f"[FIRMS] fetch({lat},{lon}): {e}")
            return []

    def health(self) -> dict:
        return {
            "source":    self.SOURCE_ID,
            "available": self._adapter is not None,
            "last_ok":   self._last_ok,
            "last_err":  self._last_err,
        }


class CarbonMonitorWorker:
    """Wraps the Carbon Monitor CO₂/emissions adapter."""

    SOURCE_ID = "carbon_monitor"

    def __init__(self, lake: DataLake):
        self._lake = lake
        self._last_ok: Optional[float] = None
        self._last_err: Optional[str] = None
        try:
            from adapters.carbon_monitor import CarbonMonitorAdapter
            self._adapter = CarbonMonitorAdapter(lake)
        except Exception as e:
            self._adapter = None
            self._last_err = str(e)
            logger.warning(f"[CarbonMonitor] adapter init failed: {e}")

    async def fetch_features(
        self,
        lat: float,
        lon: float,
        country_code: str = "IND",
    ) -> list[TelemetryRecord]:
        if self._adapter is None:
            return []
        try:
            records = await self._adapter.fetch(lat, lon, country_code=country_code)
            self._last_ok = time.time()
            self._last_err = None
            return records or []
        except Exception as e:
            self._last_err = str(e)
            logger.debug(f"[CarbonMonitor] cc={country_code}: {e}")
            return []

    def health(self) -> dict:
        return {
            "source":    self.SOURCE_ID,
            "available": self._adapter is not None,
            "last_ok":   self._last_ok,
            "last_err":  self._last_err,
        }


# ─────────────────────────────────────────────────────────────────────────────
# WorkerRegistry
# ─────────────────────────────────────────────────────────────────────────────

class WorkerRegistry:
    """
    Owns all telemetry workers. Orchestrates parallel fetch across all sources.

    Usage:
        registry = WorkerRegistry(lake)
        records  = await registry.fetch_all_features(lat, lon, country_code)

    Workers are also accessible by key for targeted fetches:
        firms_records = await registry.workers["firms"].fetch_features(lat, lon)
    """

    def __init__(self, lake: DataLake):
        self._lake = lake

        # Keyed workers — publisher.py accesses these by key directly
        self.workers: dict = {
            "open_meteo":     OpenMeteoWorker(lake),
            "firms":          FIRMSWorker(lake),
            "carbon_monitor": CarbonMonitorWorker(lake),
        }

        logger.info(
            f"[WorkerRegistry] Initialised {len(self.workers)} workers: "
            f"{list(self.workers.keys())}"
        )

    async def fetch_all_features(
        self,
        lat:          float,
        lon:          float,
        country_code: str = "IND",
    ) -> list[TelemetryRecord]:
        """
        Fetch from all workers in parallel.
        Failed workers return empty lists — never raises.
        Returns a flat deduplicated list of TelemetryRecord.
        """
        tasks = [
            worker.fetch_features(lat, lon, country_code)
            for worker in self.workers.values()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        records: list[TelemetryRecord] = []
        for i, result in enumerate(results):
            if isinstance(result, list):
                records.extend(result)
            else:
                worker_key = list(self.workers.keys())[i]
                logger.warning(f"[WorkerRegistry] {worker_key} raised: {result}")

        logger.debug(
            f"[WorkerRegistry] fetch_all_features({lat},{lon},{country_code}) "
            f"→ {len(records)} records from {len(self.workers)} workers"
        )
        return records

    def health_report(self) -> dict:
        """Structured health across all workers — consumed by /risk/health."""
        worker_health = {key: w.health() for key, w in self.workers.items()}
        available     = sum(1 for w in worker_health.values() if w["available"])

        return {
            "workers_total":     len(self.workers),
            "workers_available": available,
            "workers":           worker_health,
        }
