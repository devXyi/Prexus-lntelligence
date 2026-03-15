"""
layer1/publisher.py
Meteorium Engine — Queue-Aware Worker Wrapper
Wraps existing workers so every fetch automatically publishes to Redis Streams.
Drop-in replacement — if Redis is unavailable, falls back to direct pipeline.

Before queue:   fetch → directly into feature store (blocking)
After queue:    fetch → publish to stream → consumer processes async

This file wraps the WorkerRegistry from workers.py.
You don't rewrite workers.py — you wrap it here.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from core.queue import QueueManager
from layer1.workers import WorkerRegistry
from layer2.lake import DataLake
from adapters.base import TelemetryRecord

logger = logging.getLogger("meteorium.publisher")


class QueueAwareWorkerRegistry:
    """
    Wraps WorkerRegistry. After every fetch, publishes records
    to the appropriate Redis Stream.

    If queue is unavailable, returns records directly (original behavior).
    This means zero downtime if Redis goes offline.
    """

    def __init__(
        self,
        lake:  DataLake,
        queue: QueueManager,
    ):
        self._workers = WorkerRegistry(lake)
        self._queue   = queue

    async def fetch_and_publish(
        self,
        lat:          float,
        lon:          float,
        country_code: str  = "IND",
        asset_id:     Optional[str] = None,
    ) -> list[TelemetryRecord]:
        """
        Fetch all telemetry, publish to queue, return records.
        Callers get records immediately (for on-demand requests).
        Queue consumers get the same records for background processing.
        """

        # Fetch from all workers in parallel
        records = await self._workers.fetch_all_features(lat, lon, country_code)

        if not records:
            return records

        # Publish to queue asynchronously (don't block the API response)
        if self._queue.available and self._queue.producer:

            # Separate satellite-type records for the satellite stream
            satellite_sources = {
                "Sentinel Hub / ESA Copernicus",
                "Planet Labs PlanetScope",
                "Maxar Intelligence WorldView",
                "STAC Catalog (Microsoft Planetary Computer)",
            }

            telemetry_recs = [r for r in records if r.source not in satellite_sources]
            satellite_recs = [r for r in records if r.source in satellite_sources]

            # Fire and forget — don't await (non-blocking)
            asyncio.create_task(
                self._queue.producer.publish_telemetry(
                    telemetry_recs, asset_id=asset_id, stream="telemetry"
                )
            )
            if satellite_recs:
                asyncio.create_task(
                    self._queue.producer.publish_telemetry(
                        satellite_recs, asset_id=asset_id, stream="satellite"
                    )
                )

            logger.debug(
                f"[Publisher] {len(records)} records queued "
                f"({len(telemetry_recs)} telemetry, {len(satellite_recs)} satellite)"
            )

        return records

    async def publish_alerts(self, alerts: list, asset_id: str):
        """Push generated alerts to the alerts stream."""
        if not self._queue.available or not self._queue.producer:
            return
        for alert in alerts:
            asyncio.create_task(
                self._queue.producer.publish_alert(
                    alert.to_dict() if hasattr(alert, "to_dict") else alert,
                    asset_id=asset_id,
                )
            )

    async def trigger_rescore(self, asset_ids: list[str], reason: str = ""):
        """Queue assets for background re-scoring."""
        if not self._queue.available or not self._queue.producer:
            return
        await self._queue.producer.publish_rescore(asset_ids, reason)

    def health_report(self) -> dict:
        return {
            **self._workers.health_report(),
            "queue": {
                "available": self._queue.available,
                "mode": "redis_streams" if self._queue.available else "direct",
            }
        }


# ════════════════════════════════════════════════════════════════════════════
# CONTINUOUS INGESTION LOOP
# Runs alongside the API — keeps streams populated even without requests
# ════════════════════════════════════════════════════════════════════════════

class ContinuousIngestor:
    """
    Runs ingestion workers on their natural cadences,
    publishing everything to Redis Streams.

    This is the "planetary monitoring" loop — runs forever,
    independent of API requests. When a user requests a risk score,
    the data is already in the feature store.

    Cadences:
      Open-Meteo:     every 1 hour
      FIRMS:          every 3 hours
      Carbon Monitor: every 24 hours
    """

    # Sample coverage points — expand as assets grow
    COVERAGE_POINTS = [
        (28.61,  77.20,  "IND"),   # Delhi
        (18.93,  72.83,  "IND"),   # Mumbai
        (12.97,  77.59,  "IND"),   # Bangalore
        (13.08,  80.27,  "IND"),   # Chennai
        (22.57,  88.36,  "IND"),   # Kolkata
        (1.35,  103.82,  "SGP"),   # Singapore
        (31.23, 121.47,  "CHN"),   # Shanghai
        (40.71, -74.01,  "USA"),   # New York
        (51.51,  -0.13,  "GBR"),   # London
        (-23.55,-46.63,  "BRA"),   # São Paulo
        (-33.87, 151.21, "AUS"),   # Sydney
        (19.43, -99.13,  "MEX"),   # Mexico City
        (35.68, 139.69,  "JPN"),   # Tokyo
        (55.75,  37.62,  "RUS"),   # Moscow
        (24.47,  54.37,  "ARE"),   # Abu Dhabi
    ]

    def __init__(
        self,
        publisher:    QueueAwareWorkerRegistry,
        scheduler,    # APScheduler AsyncIOScheduler
    ):
        self._publisher  = publisher
        self._scheduler  = scheduler

    def register_jobs(self):
        """Add ingestion jobs to the scheduler."""
        from apscheduler.triggers.interval import IntervalTrigger

        self._scheduler.add_job(
            self._ingest_weather,
            IntervalTrigger(hours=1),
            id               = "continuous_weather",
            name             = "Continuous Weather Ingest",
            max_instances    = 1,
            replace_existing = True,
        )
        self._scheduler.add_job(
            self._ingest_fire,
            IntervalTrigger(hours=3),
            id               = "continuous_fire",
            name             = "Continuous Fire Ingest",
            max_instances    = 1,
            replace_existing = True,
        )
        self._scheduler.add_job(
            self._ingest_carbon,
            IntervalTrigger(hours=24),
            id               = "continuous_carbon",
            name             = "Continuous Carbon Ingest",
            max_instances    = 1,
            replace_existing = True,
        )

        logger.info(
            f"[Ingestor] Registered continuous ingestion for "
            f"{len(self.COVERAGE_POINTS)} coverage points"
        )

    async def _ingest_weather(self):
        logger.info("[Ingestor] → Weather pass starting")
        tasks = [
            self._publisher.fetch_and_publish(lat, lon, cc)
            for lat, lon, cc in self.COVERAGE_POINTS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total   = sum(len(r) for r in results if isinstance(r, list))
        logger.info(f"[Ingestor] ✓ Weather: {total} records across {len(self.COVERAGE_POINTS)} points")

    async def _ingest_fire(self):
        logger.info("[Ingestor] → Fire pass starting")
        tasks = [
            self._publisher._workers.workers["firms"].fetch_features(lat, lon)
            for lat, lon, _ in self.COVERAGE_POINTS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        records = [r for batch in results if isinstance(batch, list) for r in batch]
        if records and self._publisher._queue.available:
            await self._publisher._queue.producer.publish_telemetry(records, stream="telemetry")
        logger.info(f"[Ingestor] ✓ Fire: {len(records)} detection records")

    async def _ingest_carbon(self):
        logger.info("[Ingestor] → Carbon pass starting")
        seen_countries = set()
        for _, _, cc in self.COVERAGE_POINTS:
            if cc in seen_countries:
                continue
            seen_countries.add(cc)
            try:
                records = await self._publisher._workers.workers["carbon_monitor"].fetch_features(
                    0, 0, country_code=cc
                )
                if records and self._publisher._queue.available:
                    await self._publisher._queue.producer.publish_telemetry(records, stream="telemetry")
            except Exception as e:
                logger.debug(f"[Ingestor] Carbon {cc}: {e}")
        logger.info(f"[Ingestor] ✓ Carbon: {len(seen_countries)} countries")

