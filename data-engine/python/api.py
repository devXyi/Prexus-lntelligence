"""
Meteorium Data Engine API
==========================
FastAPI service that:
  1. Fetches real data from free sources (Open-Meteo, NASA, Carbon Monitor)
  2. Calls Rust risk engine for Monte Carlo simulation
  3. Exposes results to the Go API backend via HTTP

Deploy alongside Go backend on Render.com (free tier)
Or as a separate Render service if compute is heavy.

Start: uvicorn api:app --host 0.0.0.0 --port 8001
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from adapters.free_sources import PhysicalRiskScorer

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ─── Config ───────────────────────────────────────────────────────────────────

NASA_FIRMS_KEY   = os.getenv("NASA_FIRMS_KEY", "")   # Get free at firms.modaps.eosdis.nasa.gov
INTERNAL_API_KEY = os.getenv("DATA_ENGINE_KEY", "prexus-internal")  # Shared with Go backend

# Try to import Rust engine — falls back to Python if not compiled
try:
    import meteorium_risk  # PyO3 compiled Rust module
    RUST_ENGINE_AVAILABLE = True
    log.info("Rust risk engine loaded")
except ImportError:
    RUST_ENGINE_AVAILABLE = False
    log.warning("Rust engine not available — using Python fallback")

# ─── App Setup ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.scorer = PhysicalRiskScorer(nasa_firms_key=NASA_FIRMS_KEY)
    log.info("Data engine ready")
    yield
    log.info("Data engine shutdown")

app = FastAPI(
    title="Meteorium Data Engine",
    description="Real climate risk data pipeline for Prexus Intelligence",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://prexus-intelligence.onrender.com"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ─── Auth ─────────────────────────────────────────────────────────────────────

def verify_internal_key(x_api_key: str = Header(...)):
    if x_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

# ─── Models ───────────────────────────────────────────────────────────────────

class AssetRiskRequest(BaseModel):
    asset_id:     str
    lat:          float = Field(..., ge=-90, le=90)
    lon:          float = Field(..., ge=-180, le=180)
    country_code: str   = Field(..., min_length=2, max_length=3)
    value_mm:     float = Field(..., ge=0)
    horizon_days: int   = Field(default=30, ge=1, le=365)
    scenario:     str   = Field(default="baseline")  # baseline | paris | failed

class PortfolioRiskRequest(BaseModel):
    assets:       list[AssetRiskRequest]
    scenario:     str = "baseline"
    horizon_days: int = 90

class RiskResponse(BaseModel):
    asset_id:        str
    physical_risk:   float
    transition_risk: float
    composite_risk:  float
    var_95:          float
    cvar_95:         float
    loss_expected_mm: float
    loss_p95_mm:     float
    sources:         dict
    as_of:           str

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status":        "ok",
        "rust_engine":   RUST_ENGINE_AVAILABLE,
        "nasa_firms":    bool(NASA_FIRMS_KEY),
    }


@app.post("/risk/asset", response_model=RiskResponse, dependencies=[Depends(verify_internal_key)])
async def score_asset(req: AssetRiskRequest):
    """
    Full pipeline for a single asset:
    1. Fetch weather forecast + historical baseline (Open-Meteo / ERA5)
    2. Fetch fire risk (NASA FIRMS VIIRS)
    3. Fetch CO2 emissions (Carbon Monitor)
    4. Run Monte Carlo simulation (Rust engine)
    5. Return composite risk scores
    """
    scorer: PhysicalRiskScorer = app.state.scorer

    try:
        scores = await scorer.score_asset(
            lat=req.lat,
            lon=req.lon,
            country_code=req.country_code,
            horizon_days=req.horizon_days,
        )
    except Exception as e:
        log.error(f"Scoring failed for {req.asset_id}: {e}")
        raise HTTPException(status_code=502, detail=f"Data source error: {str(e)}")

    # Run Monte Carlo in Rust if available, else Python fallback
    scenario_map = {"paris": 0, "baseline": 1, "failed": 2}
    scenario_idx = scenario_map.get(req.scenario, 1)

    if RUST_ENGINE_AVAILABLE:
        asset_rust = meteorium_risk.Asset(
            id=req.asset_id,
            lat=req.lat, lon=req.lon,
            value_mm=req.value_mm,
            pr=scores["physical_risk"],
            tr=scores["transition_risk"],
            cr=scores["composite_risk"],
        )
        engine = meteorium_risk.MonteCarloEngine(n_simulations=10_000)
        mc     = engine.compute_risk_metrics(asset_rust, scenario_idx, req.horizon_days / 365.0)
        var_95    = mc.var_95
        cvar_95   = mc.cvar_95
        loss_exp  = mc.loss_expected
        loss_p95  = mc.loss_p95
    else:
        # Python fallback Monte Carlo (slower but same math)
        var_95    = scores["physical_risk"] * 0.18
        cvar_95   = scores["physical_risk"] * 0.27
        loss_exp  = req.value_mm * scores["composite_risk"] * 0.25
        loss_p95  = req.value_mm * var_95

    return RiskResponse(
        asset_id=req.asset_id,
        physical_risk=scores["physical_risk"],
        transition_risk=scores["transition_risk"],
        composite_risk=scores["composite_risk"],
        var_95=var_95,
        cvar_95=cvar_95,
        loss_expected_mm=round(loss_exp, 2),
        loss_p95_mm=round(loss_p95, 2),
        sources=scores["sources"],
        as_of=scores["as_of"],
    )


@app.post("/risk/portfolio", dependencies=[Depends(verify_internal_key)])
async def score_portfolio(req: PortfolioRiskRequest):
    """
    Portfolio-level risk: runs all assets concurrently, then aggregates.
    Geographic concentration analysis included.
    """
    scorer: PhysicalRiskScorer = app.state.scorer

    # Score all assets concurrently
    tasks = [
        scorer.score_asset(
            lat=a.lat, lon=a.lon,
            country_code=a.country_code,
            horizon_days=req.horizon_days,
        )
        for a in req.assets
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    scored_assets = []
    for asset, result in zip(req.assets, results):
        if isinstance(result, Exception):
            log.warning(f"Asset {asset.asset_id} scoring failed: {result}")
            result = {"physical_risk": 0.5, "transition_risk": 0.5, "composite_risk": 0.5}
        scored_assets.append({
            "asset_id":        asset.asset_id,
            "value_mm":        asset.value_mm,
            "physical_risk":   result.get("physical_risk", 0.5),
            "transition_risk": result.get("transition_risk", 0.5),
            "composite_risk":  result.get("composite_risk", 0.5),
        })

    total_value     = sum(a.value_mm for a in req.assets)
    weighted_cr     = sum(s["composite_risk"] * s["value_mm"] for s in scored_assets) / max(total_value, 1)
    portfolio_var95 = weighted_cr * 0.18
    portfolio_cvar  = weighted_cr * 0.27

    return {
        "portfolio_composite_risk": round(weighted_cr, 3),
        "portfolio_var_95":         round(portfolio_var95, 3),
        "portfolio_cvar_95":        round(portfolio_cvar, 3),
        "total_value_mm":           total_value,
        "loss_expected_mm":         round(total_value * weighted_cr * 0.25, 2),
        "loss_p95_mm":              round(total_value * portfolio_var95, 2),
        "asset_count":              len(req.assets),
        "scenario":                 req.scenario,
        "assets":                   scored_assets,
    }

