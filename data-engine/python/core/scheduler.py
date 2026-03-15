"""
core/scheduler.py
Meteorium Engine — Background Ingestion Scheduler
APScheduler runs ingestion workers on their natural cadences.
Workers run regardless of API requests — the system never sleeps.
"""

import asyncio
import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval  import IntervalTrigger

logger = logging.getLogger("meteorium.scheduler")


class MeteoriumScheduler:
    """
    Orchestrates all Layer 1 ingestion workers on their natural cadences.
    Runs in the background while Layer 6 API serves requests.

    Schedules:
      Open-Meteo forecast:   every 1 hour
      NASA FIRMS:            every 3 hours
      Carbon Monitor:        every 24 hours
      ERA5 archive:          every 7 days
    """

    def __init__(self, workers, lake, preproc):
        self.workers  = workers
        self.lake     = lake
        self.preproc  = preproc
        self._sched   = AsyncIOScheduler(timezone="UTC")
        self._active  = False

        # Default coverage area: global bounding box for FIRMS
        # In production: iterate over all registered asset locations
        self._default_bbox = (-180, -90, 180, 90)
        self._sample_points = [
            # Seed with major risk regions until assets are loaded
            (28.61,  77.20,  "IND"),   # Delhi
            (18.93,  72.83,  "IND"),   # Mumbai
            (1.35,  103.82,  "SGP"),   # Singapore
            (31.23, 121.47,  "CHN"),   # Shanghai
            (40.71, -74.01,  "USA"),   # New York
            (51.51,  -0.13,  "GBR"),   # London
            (-23.55, -46.63, "BRA"),   # São Paulo
        ]

    def start(self):
        """Register all jobs and start scheduler."""
        self._sched.add_job(
            self._job_openmeteo,
            IntervalTrigger(hours=1),
            id          = "openmeteo_forecast",
            name        = "Open-Meteo Forecast",
            max_instances = 1,
            replace_existing = True,
        )
        self._sched.add_job(
            self._job_firms,
            IntervalTrigger(hours=3),
            id          = "firms_viirs",
            name        = "NASA FIRMS VIIRS",
            max_instances = 1,
            replace_existing = True,
        )
        self._sched.add_job(
            self._job_carbon,
            IntervalTrigger(hours=24),
            id          = "carbon_monitor",
            name        = "Carbon Monitor",
            max_instances = 1,
            replace_existing = True,
        )
        self._sched.add_job(
            self._job_era5,
            IntervalTrigger(days=7),
            id          = "era5_archive",
            name        = "ERA5 Archive",
            max_instances = 1,
            replace_existing = True,
        )
        self._sched.add_job(
            self._job_purge,
            IntervalTrigger(hours=12),
            id   = "lake_purge",
            name = "Lake Maintenance",
        )

        self._sched.start()
        self._active = True
        logger.info("✓ Scheduler started. Jobs: openmeteo(1h), firms(3h), carbon(24h), era5(7d)")

    def stop(self):
        self._sched.shutdown(wait=False)
        self._active = False

    @property
    def jobs(self) -> list[dict]:
        if not self._active:
            return []
        return [
            {
                "id":       job.id,
                "name":     job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            }
            for job in self._sched.get_jobs()
        ]

    # ── Job handlers ──────────────────────────────────────────────────────────

    async def _job_openmeteo(self):
        logger.info("[Scheduler] Running Open-Meteo ingest")
        count = 0
        for lat, lon, cc in self._sample_points:
            try:
                rec = await self.workers.workers["open_meteo"].run(lat=lat, lon=lon)
                if rec:
                    self.lake.deposit(rec, variable="weather_forecast")
                    count += 1
            except Exception as e:
                logger.warning(f"[Scheduler] OpenMeteo error ({lat},{lon}): {e}")
        logger.info(f"[Scheduler] Open-Meteo: {count}/{len(self._sample_points)} deposited")

    async def _job_firms(self):
        logger.info("[Scheduler] Running FIRMS VIIRS ingest")
        try:
            rec = await self.workers.workers["firms"].run(
                bbox=self._default_bbox, days_back=1
            )
            if rec:
                self.lake.deposit(rec, variable="fire_detection")
                logger.info(f"[Scheduler] FIRMS: {rec.record_count} detections deposited")
        except Exception as e:
            logger.warning(f"[Scheduler] FIRMS error: {e}")

    async def _job_carbon(self):
        logger.info("[Scheduler] Running Carbon Monitor ingest")
        countries = list({cc for _, _, cc in self._sample_points})
        for cc in countries:
            try:
                rec = await self.workers.workers["carbon_monitor"].run(country_code=cc)
                if rec:
                    self.lake.deposit(rec, variable="co2_emissions")
            except Exception as e:
                logger.warning(f"[Scheduler] Carbon error ({cc}): {e}")

    async def _job_era5(self):
        logger.info("[Scheduler] Running ERA5 archive ingest")
        if not self.workers.workers["era5"].available:
            logger.info("[Scheduler] ERA5: CDS not configured — skipping")
            return
        for lat, lon, _ in self._sample_points[:3]:   # ERA5 is slow — limit
            try:
                rec = await self.workers.workers["era5"].run(lat=lat, lon=lon)
                if rec:
                    self.lake.deposit(rec, variable="climate_reanalysis")
            except Exception as e:
                logger.warning(f"[Scheduler] ERA5 error ({lat},{lon}): {e}")

    async def _job_purge(self):
        purged = self.lake.purge_expired()
        logger.info(f"[Scheduler] Lake purge: {purged} expired entries removed")
