"""
layer6/api.py
Meteorium Engine — LAYER 6: API & Intelligence Delivery
Full FastAPI service. Orchestrates all 7 layers on each request.
Every response includes provenance, freshness, and confidence metadata.

FIXES:
 - CORS: removed wildcard+credentials combo (invalid); explicit origin list
 - cache_key: always initialized before use, not just inside if req.use_cache
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
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.config import (
    API_HOST, API_PORT, API_WORKERS, API_RELOAD,
    ENGINE_SECRET, MONTE_CARLO_DRAWS, RISK_CACHE_TTL_SEC
)
from layer1.workers import WorkerRegistry
from layer2.lake import DataLake
from layer3.preprocessor import GeospatialPreprocessor
from layer4.feature_store import FeatureStore
from layer5.engine import RiskEngine, RUST_AVAILABLE

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("meteorium.layer6")

# ─── Globals ──────────────────────────────────────────────────────────────────
_lake:     Optional[DataLake]               = None
_preproc:  Optional[GeospatialPreprocessor] = None
_store:    Optional[FeatureStore]           = None
_workers:  Optional[WorkerRegistry]        = None
_engine:   Optional[RiskEngine]            = None
_risk_cache: dict = {}   # {cache_key: (result_dict, expires_at)}


# ─── Startup / shutdown ───────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _lake, _preproc, _store, _workers, _engine

    logger.info("╔══════════════════════════════════════╗")
    logger.info("║  METEORIUM ENGINE v2.0 — STARTING    ║")
    logger.info("╚══════════════════════════════════════╝")

    _lake    = DataLake()
    _preproc = GeospatialPreprocessor()
    _store   = FeatureStore(_preproc)
    _workers = WorkerRegistry(_lake)
    _engine  = RiskEngine(n_draws=MONTE_CARLO_DRAWS)

    logger.info(f"✓ Layer 2: Data Lake ready")
    logger.info(f"✓ Layer 3: Preprocessor ready")
    logger.info(f"✓ Layer 4: Feature Store ready")
    logger.info(f"✓ Layer 5: Risk Engine ready — Rust: {RUST_AVAILABLE}")
    logger.info(f"✓ Layer 6: API ready on {API_HOST}:{API_PORT}")
    logger.info(f"✓ Monte Carlo draws: {MONTE_CARLO_DRAWS:,}")

    yield

    logger.info("Meteorium Engine shutting down.")


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Meteorium Engine",
    description = "Prexus Intelligence — 7-Layer Climate Risk Intelligence System",
    version     = "2.0.0",
    lifespan    = lifespan,
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

# FIX: wildcard origin + allow_credentials=True is an invalid CORS combo.
# Browsers reject credentialed requests unless origin is explicitly listed.
_ALLOWED_ORIGINS = [o.strip() for o in os.environ.get(
    "ALLOWED_ORIGINS",
    "https://prexus-intelligence.onrender.com,http://localhost:3000,http://localhost:5500"
).split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins     = _ALLOWED_ORIGINS,
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# ─── Request / response models ────────────────────────────────────────────────

class AssetRiskRequest(BaseModel):
    asset_id:     str   = Field(...,   example="MUM-PORT-001")
    lat:          float = Field(...,   ge=-90,  le=90,    example=18.93)
    lon:          float = Field(...,   ge=-180, le=180,   example=72.83)
    country_code: str   = Field("IND", min_length=2, max_length=3)
    value_mm:     float = Field(10.0,  gt=0, le=1_000_000)
    asset_type:   str   = Field("infrastructure")
    scenario:     str   = Field("baseline")
    horizon_days: int   = Field(365,   ge=1,  le=3650)
    use_cache:    bool  = Field(True)

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
    scenario:     str = "baseline"
    horizon_days: int = Field(30, ge=1, le=365)


class AIRequest(BaseModel):
    prompt: str = Field(..., max_length=4000)
    model:  str = "gemini"


class ChatMessage(BaseModel):
    role:    str
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    model:    str = "gemini"


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
        "version":   "2.0.0",
        "rust":      RUST_AVAILABLE,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/risk/health")
async def risk_health():
    """Full system health across all 7 layers."""
    return {
        "status":         "ok",
        "rust_engine":    RUST_AVAILABLE,
        "monte_carlo_n":  _engine.n_draws if _engine else 0,
        "workers":        _workers.health_report() if _workers else {},
        "lake_stats":     _lake.stats()    if _lake    else {},
        "layer3_stats":   _preproc.stats() if _preproc else {},
        "layer4_stats":   _store.stats()   if _store   else {},
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }


# ─── Layer 0 — source catalogue ───────────────────────────────────────────────

@app.get("/sources")
async def list_sources():
    """Layer 0: Return full source registry."""
    from layer0.sources import REGISTRY, FREE_SOURCES, KEYED_SOURCES
    return {
        "total":        len(REGISTRY),
        "free":         len(FREE_SOURCES),
        "keyed":        len(KEYED_SOURCES),
        "sources": [
            {
                "id":            s.id,
                "name":          s.name,
                "agency":        s.agency,
                "type":          s.type,
                "format":        s.format,
                "update_hours":  s.update_freq_hours,
                "requires_key":  s.requires_key,
                "resolution_km": s.resolution_km,
                "coverage":      s.coverage,
                "notes":         s.notes,
            }
            for s in REGISTRY.values()
        ]
    }


# ─── Main risk endpoints ──────────────────────────────────────────────────────

@app.post("/risk/asset")
async def score_asset(
    req: AssetRiskRequest,
    _:   bool = Depends(_verify),
):
    """
    Full 7-layer pipeline for a single asset.
    Layer 1 → 2 → 3 → 4 → 5 → response.
    """
    start = time.perf_counter()

    # FIX: cache_key always initialized — was only set inside if req.use_cache
    # but written to _risk_cache unconditionally at the end of the function.
    cache_key = f"{req.asset_id}:{req.scenario}:{req.horizon_days}"

    # ── Cache check ───────────────────────────────────────────────────────────
    if req.use_cache and cache_key in _risk_cache:
        cached_result, expires = _risk_cache[cache_key]
        if time.time() < expires:
            cached_result["from_cache"] = True
            return cached_result

    logger.info(f"[/risk/asset] {req.asset_id} @ ({req.lat},{req.lon}) [{req.country_code}] "
                f"scenario={req.scenario} horizon={req.horizon_days}d")

    # ── Layer 1: Fetch telemetry ───────────────────────────────────────────────
    telemetry = await _workers.fetch_all_features(
        req.lat, req.lon, req.country_code
    )

    if not telemetry:
        raise HTTPException(
            503, "All telemetry sources unavailable — cannot compute risk"
        )

    # ── Layer 4: Extract features ─────────────────────────────────────────────
    features = _store.extract(
        asset_id     = req.asset_id,
        lat          = req.lat,
        lon          = req.lon,
        country_code = req.country_code,
        telemetry    = telemetry,
        scenario     = req.scenario,
    )

    # ── Layer 5: Score ────────────────────────────────────────────────────────
    result = _engine.score_asset(
        asset_id     = req.asset_id,
        features     = features,
        asset_type   = req.asset_type,
        value_mm     = req.value_mm,
        scenario     = req.scenario,
        horizon_days = req.horizon_days,
    )

    elapsed_ms = (time.perf_counter() - start) * 1000

    response = {
        **result.to_dict(),
        "telemetry_records": len(telemetry),
        "elapsed_ms":        round(elapsed_ms, 1),
        "from_cache":        False,
        "pipeline": {
            "layer1": f"{len(telemetry)} records from {len({r.source for r in telemetry})} sources",
            "layer3": "H3 tiles updated",
            "layer4": f"confidence={features.confidence:.2f}",
            "layer5": f"engine={result.engine}",
        }
    }

    # Cache result
    if req.use_cache:
        _risk_cache[cache_key] = (response, time.time() + RISK_CACHE_TTL_SEC)

    return response


@app.post("/risk/portfolio")
async def score_portfolio(
    req: PortfolioRequest,
    _:   bool = Depends(_verify),
):
    """Portfolio risk with correlated Monte Carlo."""
    if not req.assets:
        raise HTTPException(400, "assets cannot be empty")
    if len(req.assets) > 200:
        raise HTTPException(400, "max 200 assets per request")

    start = time.perf_counter()
    logger.info(f"[/risk/portfolio] {len(req.assets)} assets, scenario={req.scenario}")

    BATCH = 10
    asset_scores = []

    for i in range(0, len(req.assets), BATCH):
        batch = req.assets[i:i + BATCH]

        async def score_one(a=None):
            tel = await _workers.fetch_all_features(
                a.lat, a.lon, a.country_code
            )
            feat = _store.extract(
                asset_id=a.asset_id, lat=a.lat, lon=a.lon,
                country_code=a.country_code, telemetry=tel,
            )
            r = _engine.score_asset(
                asset_id=a.asset_id, features=feat,
                asset_type=a.asset_type, value_mm=a.value_mm,
                scenario=req.scenario, horizon_days=a.horizon_days,
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
            *[score_one(a) for a in batch],
            return_exceptions=True,
        )
        for r in batch_results:
            if isinstance(r, dict):
                asset_scores.append(r)

    portfolio = _engine.score_portfolio(asset_scores, req.scenario)
    elapsed   = (time.perf_counter() - start) * 1000

    return {
        **portfolio.to_dict(),
        "elapsed_ms": round(elapsed, 1),
    }


@app.post("/risk/stress-test")
async def stress_test(
    req: AssetRiskRequest,
    _:   bool = Depends(_verify),
):
    """SSP scenario stress test for a single asset."""
    if not RUST_AVAILABLE:
        raise HTTPException(503, "Stress test requires Rust engine")

    tel      = await _workers.fetch_all_features(req.lat, req.lon, req.country_code)
    features = _store.extract(
        req.asset_id, req.lat, req.lon, req.country_code, tel
    )
    base = _engine.score_asset(
        req.asset_id, features, req.asset_type, req.value_mm, req.scenario, req.horizon_days
    )

    import meteorium_engine as rust
    scenarios = rust.stress_test_scenarios(
        base.physical_risk, base.transition_risk,
        req.value_mm, req.asset_type,
        min(_engine.n_draws, 5000),
    )

    return {
        "asset_id": req.asset_id,
        "scenarios": [
            {"label": l, "composite_risk": round(c, 4),
             "var_95": round(v, 4), "expected_loss_mm": round(loss, 2)}
            for l, c, v, loss in scenarios
        ],
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/risk/histogram")
async def loss_histogram(
    req: AssetRiskRequest,
    _:   bool = Depends(_verify),
):
    """Loss distribution histogram for visualization."""
    if not RUST_AVAILABLE:
        raise HTTPException(503, "Histogram requires Rust engine")

    tel      = await _workers.fetch_all_features(req.lat, req.lon, req.country_code)
    features = _store.extract(req.asset_id, req.lat, req.lon, req.country_code, tel)
    base     = _engine.score_asset(
        req.asset_id, features, req.asset_type, req.value_mm, req.scenario, req.horizon_days
    )

    import meteorium_engine as rust
    buckets = rust.loss_histogram(
        base.physical_risk, base.transition_risk,
        req.value_mm, req.scenario,
        min(_engine.n_draws, 5000),
    )

    return {
        "asset_id": req.asset_id,
        "histogram": [
            {"lower": b[0], "upper": b[1], "frequency": round(b[2], 6)}
            for b in buckets
        ],
    }


# ─── Lake / manifest endpoints ────────────────────────────────────────────────

@app.get("/lake/stats")
async def lake_stats(_: bool = Depends(_verify)):
    """Layer 2: Data lake statistics."""
    return _lake.stats()


@app.get("/lake/files")
async def lake_files(
    source_id:    Optional[str]   = None,
    since_hours:  Optional[float] = 24.0,
    _:            bool = Depends(_verify),
):
    """Layer 2: List files in data lake."""
    entries = _lake.list_files(source_id=source_id, since_hours=since_hours)
    return {
        "count": len(entries),
        "files": [
            {
                "lake_id":    e.lake_id,
                "source_id":  e.source_id,
                "file_path":  e.file_path,
                "size_mb":    e.file_size_mb,
                "deposited":  e.deposited_at.isoformat(),
            }
            for e in entries
        ]
    }


# ─── AI endpoints ─────────────────────────────────────────────────────────────

@app.post("/analyze")
async def ai_analyze(req: AIRequest):
    result = await _call_ai(req.prompt, req.model)
    return {"result": result, "model": req.model}


@app.post("/chat")
async def ai_chat(req: ChatRequest):
    messages = [{"role": m.role, "content": m.content} for m in req.messages]
    last     = next((m["content"] for m in reversed(messages) if m["role"] == "user"), "")
    result   = await _call_ai(last, req.model, history=messages[:-1])
    return {"result": result, "model": req.model}


async def _call_ai(
    prompt:  str,
    model:   str = "gemini",
    history: list = None,
) -> str:
    import httpx
    history = history or []

    if model == "gemini":
        key = os.environ.get("GEMINI_API_KEY", "")
        if not key:
            return "AI unavailable — set GEMINI_API_KEY env var."
        async with httpx.AsyncClient(timeout=30) as c:
            contents = [
                *[{"role": m["role"], "parts": [{"text": m["content"]}]} for m in history[-6:]],
                {"role": "user", "parts": [{"text": prompt}]},
            ]
            resp = await c.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"gemini-2.0-flash:generateContent?key={key}",
                json={"contents": contents,
                      "generationConfig": {"temperature": 0.4, "maxOutputTokens": 600}},
            )
            resp.raise_for_status()
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    if model == "claude":
        key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not key:
            return "AI unavailable — set ANTHROPIC_API_KEY env var."
        async with httpx.AsyncClient(timeout=30) as c:
            msgs = [*history[-6:], {"role": "user", "content": prompt}]
            resp = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": key, "anthropic-version": "2023-06-01"},
                json={"model": "claude-3-5-sonnet-20241022",
                      "max_tokens": 600, "messages": msgs,
                      "system": "You are a senior climate risk analyst at Prexus Intelligence."},
            )
            resp.raise_for_status()
            return resp.json()["content"][0]["text"]

    if model == "chatgpt":
        key = os.environ.get("OPENAI_API_KEY", "")
        if not key:
            return "AI unavailable — set OPENAI_API_KEY env var."
        async with httpx.AsyncClient(timeout=30) as c:
            msgs = [
                {"role": "system", "content": "Senior climate risk analyst. Be concise."},
                *history[-6:],
                {"role": "user", "content": prompt},
            ]
            resp = await c.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}"},
                json={"model": "gpt-4o", "messages": msgs, "max_tokens": 600},
            )
            resp.raise_for_status()
            return resp.json()["choices"][0]["message"]["content"]

    return "Unknown model. Use: gemini | claude | chatgpt"


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "layer6.api:app",
        host    = API_HOST,
        port    = API_PORT,
        reload  = API_RELOAD,
        workers = API_WORKERS,
        log_level = "info",
    )
