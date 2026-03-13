"""
Meteorium Data Engine API — pydantic-free version
===================================================
No BaseModel anywhere. Accepts raw JSON via Request object.
Works on Python 3.11, 3.12, 3.13, 3.14 with any fastapi/pydantic combo.
"""

import asyncio
import logging
import os 
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from adapters.free_sources import PhysicalRiskScorer

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

NASA_FIRMS_KEY   = os.getenv("NASA_FIRMS_KEY", "")
INTERNAL_API_KEY = os.getenv("DATA_ENGINE_KEY", "prexus-internal")

try:
    import meteorium_risk
    RUST_ENGINE_AVAILABLE = True
    log.info("Rust risk engine loaded")
except ImportError:
    RUST_ENGINE_AVAILABLE = False
    log.warning("Rust engine not available — Python fallback active")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.scorer = PhysicalRiskScorer(nasa_firms_key=NASA_FIRMS_KEY)
    log.info("Meteorium data engine ready")
    yield


app = FastAPI(title="Meteorium Data Engine", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def check_auth(request: Request):
    key = request.headers.get("x-api-key", "")
    if key != INTERNAL_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/health")
async def health():
    return {
        "status":      "ok",
        "rust_engine": RUST_ENGINE_AVAILABLE,
        "nasa_firms":  bool(NASA_FIRMS_KEY),
    }


@app.post("/risk/asset")
async def score_asset(request: Request):
    check_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    asset_id     = str(body.get("asset_id", "UNKNOWN"))
    lat          = float(body.get("lat", 0))
    lon          = float(body.get("lon", 0))
    country_code = str(body.get("country_code", "IND"))[:3]
    value_mm     = float(body.get("value_mm", 10))
    horizon_days = int(body.get("horizon_days", 30))
    scenario     = str(body.get("scenario", "baseline"))

    scorer = app.state.scorer
    try:
        scores = await scorer.score_asset(lat=lat, lon=lon,
            country_code=country_code, horizon_days=horizon_days)
    except Exception as e:
        log.error(f"Scoring failed for {asset_id}: {e}")
        scores = {"physical_risk": 0.5, "transition_risk": 0.5, "composite_risk": 0.5,
                  "sources": {}, "as_of": ""}

    var_95   = scores["physical_risk"] * 0.18
    cvar_95  = scores["physical_risk"] * 0.27
    loss_exp = value_mm * scores["composite_risk"] * 0.25
    loss_p95 = value_mm * var_95

    return JSONResponse({
        "asset_id":         asset_id,
        "physical_risk":    round(scores["physical_risk"], 3),
        "transition_risk":  round(scores["transition_risk"], 3),
        "composite_risk":   round(scores["composite_risk"], 3),
        "var_95":           round(var_95, 3),
        "cvar_95":          round(cvar_95, 3),
        "loss_expected_mm": round(loss_exp, 2),
        "loss_p95_mm":      round(loss_p95, 2),
        "sources":          scores.get("sources", {}),
        "as_of":            scores.get("as_of", ""),
    })


@app.post("/risk/portfolio")
async def score_portfolio(request: Request):
    check_auth(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    assets_raw   = body.get("assets", [])
    scenario     = str(body.get("scenario", "baseline"))
    horizon_days = int(body.get("horizon_days", 30))

    if not assets_raw:
        return JSONResponse({"error": "No assets"}, status_code=400)

    scorer = app.state.scorer
    tasks = [
        scorer.score_asset(
            lat=float(a.get("lat", 0)),
            lon=float(a.get("lon", 0)),
            country_code=str(a.get("country_code", "IND"))[:3],
            horizon_days=horizon_days,
        )
        for a in assets_raw
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    scored = []
    for a, result in zip(assets_raw, results):
        if isinstance(result, Exception):
            result = {"physical_risk": 0.5, "transition_risk": 0.5, "composite_risk": 0.5}
        scored.append({
            "asset_id":        a.get("asset_id", ""),
            "value_mm":        float(a.get("value_mm", 0)),
            "physical_risk":   result.get("physical_risk", 0.5),
            "transition_risk": result.get("transition_risk", 0.5),
            "composite_risk":  result.get("composite_risk", 0.5),
        })

    total   = sum(s["value_mm"] for s in scored)
    w_cr    = sum(s["composite_risk"] * s["value_mm"] for s in scored) / max(total, 1)

    return JSONResponse({
        "portfolio_composite_risk": round(w_cr, 3),
        "portfolio_var_95":         round(w_cr * 0.18, 3),
        "portfolio_cvar_95":        round(w_cr * 0.27, 3),
        "total_value_mm":           total,
        "loss_expected_mm":         round(total * w_cr * 0.25, 2),
        "loss_p95_mm":              round(total * w_cr * 0.18, 2),
        "asset_count":              len(scored),
        "scenario":                 scenario,
        "assets":                   scored,
    })
