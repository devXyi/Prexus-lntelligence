"""
layer6/api_queued.py
Meteorium Engine — Queue-Integrated API
Drop-in replacement for layer6/api.py that wires Redis Streams
into the startup lifecycle and all risk endpoints.

Changes from api.py:
  1. QueueManager created on startup
  2. QueueAwareWorkerRegistry replaces WorkerRegistry
  3. ContinuousIngestor registered with scheduler
  4. /queue/stats endpoint added
  5. Risk endpoints publish alerts to queue after scoring
  6. Graceful degradation if Redis unavailable
"""

import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from apscheduler.schedulers.asyncio import AsyncIOScheduler

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.config import (
    API_HOST, API_PORT, API_WORKERS, API_RELOAD,
    ENGINE_SECRET, MONTE_CARLO_DRAWS, RISK_CACHE_TTL_SEC
)
from core.queue import QueueManager
from layer1.workers import WorkerRegistry
from layer1.publisher import QueueAwareWorkerRegistry, ContinuousIngestor
from layer2.lake import DataLake
from layer3.preprocessor import GeospatialPreprocessor
from layer4.feature_store import FeatureStore
from layer5.engine import RiskEngine, RUST_AVAILABLE
from layer5.intelligence import FusedRiskEngine

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("meteorium.api")

# ─── Globals ─────────────────────────────────────────────────────────────────
_lake:      Optional[DataLake]                    = None
_preproc:   Optional[GeospatialPreprocessor]      = None
_store:     Optional[FeatureStore]                = None
_queue:     Optional[QueueManager]                = None
_publisher: Optional[QueueAwareWorkerRegistry]    = None
_engine:    Optional[RiskEngine]                  = None
_fused:     Optional[FusedRiskEngine]             = None
_scheduler: Optional[AsyncIOScheduler]            = None
_ingestor:  Optional[ContinuousIngestor]          = None
_risk_cache: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _lake, _preproc, _store, _queue, _publisher
    global _engine, _fused, _scheduler, _ingestor

    logger.info("╔══════════════════════════════════════════════╗")
    logger.info("║  METEORIUM ENGINE v2.0 — QUEUE MODE          ║")
    logger.info("╚══════════════════════════════════════════════╝")

    # ── Core layers ───────────────────────────────────────────────────────────
    _lake    = DataLake()
    _preproc = GeospatialPreprocessor()
    _store   = FeatureStore(_preproc)
    _engine  = RiskEngine(n_draws=MONTE_CARLO_DRAWS)
    _fused   = FusedRiskEngine(n_draws=MONTE_CARLO_DRAWS)

    # ── Queue ─────────────────────────────────────────────────────────────────
    _queue = QueueManager(redis_url=os.environ.get("REDIS_URL"))
    await _queue.connect(preprocessor=_preproc, feature_store=_store)

    # ── Queue-aware workers ────────────────────────────────────────────────────
    _publisher = QueueAwareWorkerRegistry(lake=_lake, queue=_queue)

    # ── Scheduler + continuous ingestion ─────────────────────────────────────
    _scheduler = AsyncIOScheduler(timezone="UTC")
    _ingestor  = ContinuousIngestor(publisher=_publisher, scheduler=_scheduler)
    _ingestor.register_jobs()
    _scheduler.start()

    logger.info(f"✓ Redis queue:      {'ACTIVE' if _queue.available else 'OFFLINE (direct mode)'}")
    logger.info(f"✓ Rust engine:      {'LOADED' if RUST_AVAILABLE else 'Python fallback'}")
    logger.info(f"✓ Continuous ingest: {len(ContinuousIngestor.COVERAGE_POINTS)} coverage points")
    logger.info(f"✓ Monte Carlo draws: {MONTE_CARLO_DRAWS:,}")
    logger.info(f"✓ API ready:         {API_HOST}:{API_PORT}")

    yield

    logger.info("Shutting down...")
    _scheduler.shutdown(wait=False)
    await _queue.disconnect()
    logger.info("Meteorium stopped.")


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Meteorium Engine",
    description = "Prexus Intelligence — 7-Layer Climate Risk System + Redis Streams",
    version     = "2.1.0",
    lifespan    = lifespan,
    docs_url    = "/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# ─── Models ───────────────────────────────────────────────────────────────────

class AssetRiskRequest(BaseModel):
    asset_id:     str   = Field(...,   example="MUM-PORT-001")
    lat:          float = Field(...,   ge=-90,  le=90)
    lon:          float = Field(...,   ge=-180, le=180)
    country_code: str   = Field("IND", min_length=2, max_length=3)
    value_mm:     float = Field(10.0,  gt=0, le=1_000_000)
    asset_type:   str   = Field("infrastructure")
    scenario:     str   = Field("baseline")
    horizon_days: int   = Field(365,   ge=1, le=3650)
    use_cache:    bool  = Field(True)
    use_satellite:bool  = Field(False)  # enables satellite adapters (uses more quota)

    @validator("scenario")
    def valid_scenario(cls, v):
        valid = {"ssp119","paris","ssp245","baseline","ssp370","ssp585","failed"}
        if v not in valid:
            raise ValueError(f"scenario must be one of {valid}")
        return v

    @validator("country_code")
    def upper_cc(cls, v):
        return v.upper()


class PortfolioAsset(BaseModel):
    asset_id:     str
    lat:          float
    lon:          float
    country_code: str   = "IND"
    value_mm:     float = 10.0
    asset_type:   str   = "infrastructure"
    horizon_days: int   = 30


class PortfolioRequest(BaseModel):
    assets:       list[PortfolioAsset]
    scenario:     str  = "baseline"
    horizon_days: int  = Field(30, ge=1, le=365)


# ─── Auth ─────────────────────────────────────────────────────────────────────

def _verify(authorization: Optional[str] = Header(None)):
    secret = ENGINE_SECRET
    if not secret:
        return True
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Unauthorized")
    if authorization.split(" ", 1)[1] != secret:
        raise HTTPException(401, "Invalid token")
    return True


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status":    "ok",
        "service":   "meteorium-engine",
        "version":   "2.1.0",
        "rust":      RUST_AVAILABLE,
        "queue":     _queue.available if _queue else False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/risk/health")
async def risk_health():
    queue_stats = await _queue.stats() if _queue else {"available": False}
    return {
        "status":        "ok",
        "rust_engine":   RUST_AVAILABLE,
        "queue":         queue_stats,
        "workers":       _publisher.health_report() if _publisher else {},
        "layer3_stats":  _preproc.stats() if _preproc else {},
        "layer4_stats":  _store.stats()   if _store   else {},
        "scheduler_jobs": [
            {"id": j.id, "next_run": j.next_run_time.isoformat() if j.next_run_time else None}
            for j in (_scheduler.get_jobs() if _scheduler else [])
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/queue/stats")
async def queue_stats(_: bool = Depends(_verify)):
    """Redis Streams pipeline statistics."""
    if not _queue:
        raise HTTPException(503, "Queue not initialized")
    return await _queue.stats()


# ─── Risk endpoints ───────────────────────────────────────────────────────────

@app.post("/risk/asset")
async def score_asset(
    req: AssetRiskRequest,
    _:   bool = Depends(_verify),
):
    """
    Full pipeline: fetch → queue → feature store → score.
    If Redis active: data may already be pre-computed.
    If not: fetches live (original behavior).
    """
    start = time.perf_counter()

    # Cache check
    if req.use_cache:
        cache_key = f"{req.asset_id}:{req.scenario}:{req.horizon_days}"
        if cache_key in _risk_cache:
            result, expires = _risk_cache[cache_key]
            if time.time() < expires:
                result["from_cache"] = True
                return result

    logger.info(
        f"[/risk/asset] {req.asset_id} @ ({req.lat},{req.lon}) "
        f"scenario={req.scenario} queue={'on' if _queue.available else 'off'}"
    )

    # Fetch + publish to queue
    telemetry = await _publisher.fetch_and_publish(
        lat          = req.lat,
        lon          = req.lon,
        country_code = req.country_code,
        asset_id     = req.asset_id,
    )

    if not telemetry:
        raise HTTPException(503, "All telemetry sources unavailable")

    # Route to fused engine (with satellite) or standard engine
    if req.use_satellite:
        result_dict = await _fused.score_with_satellites(
            asset_id     = req.asset_id,
            lat          = req.lat,
            lon          = req.lon,
            base_records = telemetry,
            asset_type   = req.asset_type,
            value_mm     = req.value_mm,
            scenario     = req.scenario,
            horizon_days = req.horizon_days,
            country_code = req.country_code,
        )
    else:
        features = _store.extract(
            asset_id     = req.asset_id,
            lat          = req.lat,
            lon          = req.lon,
            country_code = req.country_code,
            telemetry    = telemetry,
            scenario     = req.scenario,
        )
        risk = _engine.score_asset(
            asset_id     = req.asset_id,
            features     = features,
            asset_type   = req.asset_type,
            value_mm     = req.value_mm,
            scenario     = req.scenario,
            horizon_days = req.horizon_days,
        )
        result_dict = risk.to_dict()

        # Publish alerts to queue
        if risk.alerts:
            await _publisher.publish_alerts(risk.alerts, req.asset_id)

    elapsed_ms = (time.perf_counter() - start) * 1000
    result_dict.update({
        "telemetry_records": len(telemetry),
        "elapsed_ms":        round(elapsed_ms, 1),
        "from_cache":        False,
        "queue_active":      _queue.available,
    })

    # Cache
    cache_key = f"{req.asset_id}:{req.scenario}:{req.horizon_days}"
    _risk_cache[cache_key] = (result_dict, time.time() + RISK_CACHE_TTL_SEC)

    return result_dict


@app.post("/risk/portfolio")
async def score_portfolio(
    req: PortfolioRequest,
    _:   bool = Depends(_verify),
):
    if not req.assets:
        raise HTTPException(400, "assets cannot be empty")
    if len(req.assets) > 200:
        raise HTTPException(400, "max 200 assets per request")

    start  = time.perf_counter()
    BATCH  = 10
    asset_scores = []

    for i in range(0, len(req.assets), BATCH):
        batch = req.assets[i:i + BATCH]

        async def score_one(a=None):
            tel = await _publisher.fetch_and_publish(
                a.lat, a.lon, a.country_code, asset_id=a.asset_id
            )
            feat = _store.extract(
                a.asset_id, a.lat, a.lon, a.country_code, tel
            )
            r = _engine.score_asset(
                a.asset_id, feat, a.asset_type, a.value_mm,
                req.scenario, a.horizon_days,
            )
            return {
                "asset_id":        a.asset_id,
                "physical_risk":   r.physical_risk,
                "transition_risk": r.transition_risk,
                "composite_risk":  r.composite_risk,
                "value_mm":        a.value_mm,
                "type":            a.asset_type,
            }

        batch_results = await asyncio.gather(
            *[score_one(a) for a in batch], return_exceptions=True
        )
        for r in batch_results:
            if isinstance(r, dict):
                asset_scores.append(r)

    portfolio  = _engine.score_portfolio(asset_scores, req.scenario)
    elapsed_ms = (time.perf_counter() - start) * 1000

    return {
        **portfolio.to_dict(),
        "elapsed_ms":   round(elapsed_ms, 1),
        "queue_active": _queue.available,
    }


@app.post("/queue/trigger-rescore")
async def trigger_rescore(
    asset_ids: list[str],
    reason:    str = "manual",
    _:         bool = Depends(_verify),
):
    """Manually queue assets for re-scoring."""
    count = await _publisher.trigger_rescore(asset_ids, reason)
    return {"queued": count, "asset_ids": asset_ids, "reason": reason}


# ─── AI endpoints (unchanged from api.py) ────────────────────────────────────

@app.post("/analyze")
async def ai_analyze(req: dict):
    from layer6.api import _call_ai
    result = await _call_ai(req.get("prompt", ""), req.get("model", "gemini"))
    return {"result": result}


@app.post("/chat")
async def ai_chat(req: dict):
    from layer6.api import _call_ai
    messages = req.get("messages", [])
    last     = next((m["content"] for m in reversed(messages) if m["role"] == "user"), "")
    result   = await _call_ai(last, req.get("model", "gemini"), history=messages[:-1])
    return {"result": result}


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "layer6.api_queued:app",
        host      = API_HOST,
        port      = API_PORT,
        reload    = API_RELOAD,
        log_level = "info",
    )

