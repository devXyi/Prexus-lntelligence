"""
data-engine/python/core/risk_engine.py
Prexus Intelligence — Risk Scoring Engine
Converts TelemetryRecords → structured risk scores.
Calls Rust Monte Carlo core via PyO3 if available.
Falls back to pure Python scoring if Rust not compiled.
"""

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from adapters.base import TelemetryRecord

logger = logging.getLogger("prexus.risk_engine")

# ─── Try to import compiled Rust core ─────────────────────────────────────────
try:
    import meteorium_engine as _rust
    RUST_AVAILABLE = True
    logger.info("✓ Rust Monte Carlo engine loaded")
except ImportError:
    RUST_AVAILABLE = False
    logger.warning("⚠ Rust engine not compiled — using Python fallback. "
                   "Run: cd data-engine/rust && maturin develop --release")


# ─── Result types ─────────────────────────────────────────────────────────────

@dataclass
class AssetRiskResult:
    asset_id:         str
    composite_risk:   float
    physical_risk:    float
    transition_risk:  float
    var_95:           float
    cvar_95:          float
    loss_expected_mm: float
    confidence:       float
    scenario:         str
    horizon_days:     int
    sources:          dict = field(default_factory=dict)
    feature_snapshot: dict = field(default_factory=dict)
    computed_at:      str  = ""
    engine:           str  = "python_fallback"

    def to_dict(self) -> dict:
        return {
            "asset_id":         self.asset_id,
            "composite_risk":   round(self.composite_risk,   4),
            "physical_risk":    round(self.physical_risk,    4),
            "transition_risk":  round(self.transition_risk,  4),
            "var_95":           round(self.var_95,           4),
            "cvar_95":          round(self.cvar_95,          4),
            "loss_expected_mm": round(self.loss_expected_mm, 2),
            "confidence":       round(self.confidence,       4),
            "scenario":         self.scenario,
            "horizon_days":     self.horizon_days,
            "sources":          self.sources,
            "feature_snapshot": self.feature_snapshot,
            "computed_at":      self.computed_at or datetime.now(timezone.utc).isoformat(),
            "engine":           self.engine,
        }


@dataclass
class PortfolioRiskResult:
    portfolio_composite_risk: float
    portfolio_var_95:         float
    portfolio_cvar_95:        float
    loss_expected_mm:         float
    diversification_ratio:    float
    asset_count:              int
    total_value_mm:           float
    scenario:                 str
    computed_at:              str = ""

    def to_dict(self) -> dict:
        return {
            "portfolio_composite_risk": round(self.portfolio_composite_risk, 4),
            "portfolio_var_95":         round(self.portfolio_var_95,         4),
            "portfolio_cvar_95":        round(self.portfolio_cvar_95,        4),
            "loss_expected_mm":         round(self.loss_expected_mm,         2),
            "diversification_ratio":    round(self.diversification_ratio,    4),
            "asset_count":              self.asset_count,
            "total_value_mm":           round(self.total_value_mm,           2),
            "scenario":                 self.scenario,
            "computed_at":              self.computed_at or datetime.now(timezone.utc).isoformat(),
        }


# ─── Feature extractor ────────────────────────────────────────────────────────

class FeatureExtractor:
    """
    Extracts a structured feature vector from a flat list of TelemetryRecords.
    Records from multiple adapters are merged by variable name.
    """

    def extract(self, records: list[TelemetryRecord]) -> dict:
        """Return feature dict keyed by variable name."""
        feat = {}
        sources = {}

        for r in records:
            feat[r.variable]    = r.value
            sources[r.variable] = r.source

        return feat, sources

    def get_physical_risk_features(self, feat: dict) -> dict:
        """Extract physical hazard features with safe defaults."""
        return {
            "temp_anomaly_c":        feat.get("temp_anomaly_c",        0.0),
            "precip_anomaly_pct":    feat.get("precip_anomaly_pct",    0.0),
            "heat_stress_prob":      feat.get("heat_stress_prob_7d",   0.0),
            "drought_index":         feat.get("drought_index",         0.0),
            "extreme_wind_prob":     feat.get("extreme_wind_prob_7d",  0.0),
            "fire_prob_100km":       feat.get("fire_prob_100km",       0.0),
            "fire_hazard_score":     feat.get("fire_hazard_score",     0.0),
            "soil_moisture":         feat.get("soil_moisture",         0.25),
            "wind_speed_ms":         feat.get("wind_speed_ms",         5.0),
        }

    def get_transition_risk_features(self, feat: dict) -> dict:
        """Extract transition risk features with safe defaults."""
        return {
            "co2_intensity_norm":    feat.get("co2_intensity_norm",    0.5),
            "transition_risk_score": feat.get("transition_risk_score", 0.4),
            "carbon_policy_risk":    feat.get("carbon_policy_risk",    0.4),
            "emissions_yoy_change":  feat.get("emissions_yoy_change_pct", 0.0),
        }


# ─── Physical risk scorer ─────────────────────────────────────────────────────

class PhysicalRiskScorer:
    """
    Scores physical climate hazard exposure from environmental features.
    Weights calibrated against IPCC AR6 Working Group II damage functions.
    """

    # Hazard weights (must sum to 1.0)
    HAZARD_WEIGHTS = {
        "heat":       0.22,
        "drought":    0.20,
        "fire":       0.18,
        "flood":      0.18,
        "wind":       0.12,
        "sea_level":  0.10,
    }

    def score(self, features: dict, asset_type: str = "infrastructure") -> float:
        """Returns 0–1 physical risk score."""
        scores = {}

        # ── Heat hazard ────────────────────────────────────────────────────────
        temp_anom   = features.get("temp_anomaly_c", 0.0)
        heat_prob   = features.get("heat_stress_prob", 0.0)
        heat_score  = self._sigmoid(temp_anom, center=1.5, steepness=0.8)
        heat_score  = max(heat_score, heat_prob)
        scores["heat"] = heat_score

        # ── Drought hazard ─────────────────────────────────────────────────────
        drought     = features.get("drought_index", 0.0)
        precip_anom = features.get("precip_anomaly_pct", 0.0)
        drought_from_precip = max(0.0, -precip_anom / 100.0) * 0.5
        scores["drought"] = min(1.0, drought + drought_from_precip)

        # ── Fire hazard ────────────────────────────────────────────────────────
        fire_prob   = features.get("fire_prob_100km",   0.0)
        fire_hazard = features.get("fire_hazard_score", 0.0)
        scores["fire"] = min(1.0, fire_prob * 0.6 + fire_hazard * 0.4)

        # ── Flood hazard (from precip anomaly + soil saturation) ───────────────
        precip_flood = max(0.0, precip_anom / 100.0) * 0.7
        soil         = features.get("soil_moisture", 0.25)
        soil_sat     = max(0.0, (soil - 0.3) / 0.4)   # saturated above 30% VWC
        scores["flood"] = min(1.0, precip_flood + soil_sat * 0.3)

        # ── Wind hazard ────────────────────────────────────────────────────────
        wind_prob = features.get("extreme_wind_prob", 0.0)
        wind_ms   = features.get("wind_speed_ms", 5.0)
        wind_norm = min(1.0, max(0.0, (wind_ms - 15.0) / 25.0))  # 15–40 m/s range
        scores["wind"] = min(1.0, wind_prob * 0.7 + wind_norm * 0.3)

        # ── Sea level / coastal ────────────────────────────────────────────────
        scores["sea_level"] = features.get("sea_level_anomaly_norm", 0.0)

        # ── Weighted composite ─────────────────────────────────────────────────
        composite = sum(
            scores.get(h, 0.0) * w
            for h, w in self.HAZARD_WEIGHTS.items()
        )

        # Asset type vulnerability modifier
        vuln = self._vulnerability(asset_type)

        return min(1.0, composite * vuln)

    @staticmethod
    def _sigmoid(x: float, center: float = 0.0, steepness: float = 1.0) -> float:
        return 1.0 / (1.0 + math.exp(-steepness * (x - center)))

    @staticmethod
    def _vulnerability(asset_type: str) -> float:
        table = {
            "agriculture":     1.35,
            "energy":          1.20,
            "infrastructure":  1.15,
            "transport":       1.15,
            "real estate":     1.10,
            "manufacturing":   1.08,
            "technology":      1.05,
            "healthcare":      1.00,
            "financial":       0.85,
        }
        return table.get(asset_type.lower(), 1.0)


# ─── Transition risk scorer ───────────────────────────────────────────────────

class TransitionRiskScorer:
    """
    Scores transition risk: financial exposure to the low-carbon transition.
    """

    SCENARIO_CARBON_PRICE_2030 = {
        "ssp119": 250.0,   # USD/tCO2
        "paris":  250.0,
        "ssp245": 100.0,
        "baseline": 80.0,
        "ssp370":  55.0,
        "ssp585":  20.0,
        "failed":  20.0,
    }

    def score(
        self,
        features:     dict,
        scenario:     str = "baseline",
        horizon_days: int = 365,
    ) -> float:
        """Returns 0–1 transition risk score."""

        carbon_intensity = features.get("co2_intensity_norm",    0.5)
        policy_risk      = features.get("carbon_policy_risk",    0.4)
        tr_raw           = features.get("transition_risk_score", 0.4)
        yoy_change       = features.get("emissions_yoy_change",  0.0)

        # Carbon price trajectory risk
        carbon_price    = self.SCENARIO_CARBON_PRICE_2030.get(scenario, 80.0)
        price_risk_norm = min(1.0, carbon_price / 250.0)

        # Emissions trajectory penalty (rising emissions = higher stranded asset risk)
        traj_penalty = max(0.0, yoy_change / 100.0) * 0.2

        # Time horizon amplifier — longer horizon → more exposure to policy shifts
        horizon_amp = min(1.3, 1.0 + (horizon_days / 365.0) * 0.15)

        composite = (
            carbon_intensity * 0.30 +
            policy_risk      * 0.25 +
            tr_raw           * 0.25 +
            price_risk_norm  * 0.20 +
            traj_penalty
        ) * horizon_amp

        return min(1.0, composite)


# ─── Main risk engine ─────────────────────────────────────────────────────────

class RiskEngine:

    def __init__(self, n_draws: int = 10_000):
        self.n_draws      = n_draws
        self.extractor    = FeatureExtractor()
        self.phys_scorer  = PhysicalRiskScorer()
        self.tran_scorer  = TransitionRiskScorer()

    def score_asset(
        self,
        asset_id:     str,
        records:      list[TelemetryRecord],
        asset_type:   str  = "infrastructure",
        value_mm:     float = 10.0,
        scenario:     str  = "baseline",
        horizon_days: int  = 365,
        country_code: str  = "IND",
    ) -> AssetRiskResult:

        feat, sources = self.extractor.extract(records)
        phys_feat     = self.extractor.get_physical_risk_features(feat)
        tran_feat     = self.extractor.get_transition_risk_features(feat)

        physical_risk   = self.phys_scorer.score(phys_feat,  asset_type)
        transition_risk = self.tran_scorer.score(tran_feat,  scenario, horizon_days)

        # ── Monte Carlo: prefer Rust, fall back to Python ──────────────────────
        if RUST_AVAILABLE:
            try:
                cr, var95, cvar95, loss_mm, confidence = _rust.monte_carlo_asset(
                    physical_risk   = physical_risk,
                    transition_risk = transition_risk,
                    asset_value_mm  = value_mm,
                    scenario        = scenario,
                    asset_type      = asset_type,
                    horizon_days    = horizon_days,
                    n_draws         = self.n_draws,
                )
                engine_name = f"rust_monte_carlo_n{self.n_draws}"
            except Exception as e:
                logger.warning(f"Rust engine error: {e} — falling back to Python")
                cr, var95, cvar95, loss_mm, confidence = self._python_mc(
                    physical_risk, transition_risk, value_mm, scenario, horizon_days
                )
                engine_name = "python_monte_carlo_fallback"
        else:
            cr, var95, cvar95, loss_mm, confidence = self._python_mc(
                physical_risk, transition_risk, value_mm, scenario, horizon_days
            )
            engine_name = "python_monte_carlo_fallback"

        # Build source annotations
        source_summary = {}
        for var, src in sources.items():
            category = self._var_to_category(var)
            source_summary[category] = src

        return AssetRiskResult(
            asset_id         = asset_id,
            composite_risk   = cr,
            physical_risk    = physical_risk,
            transition_risk  = transition_risk,
            var_95           = var95,
            cvar_95          = cvar95,
            loss_expected_mm = loss_mm,
            confidence       = confidence,
            scenario         = scenario,
            horizon_days     = horizon_days,
            sources          = source_summary,
            feature_snapshot = {
                "temp_anomaly_c":      round(phys_feat.get("temp_anomaly_c", 0), 3),
                "fire_prob_100km":     round(phys_feat.get("fire_prob_100km", 0), 3),
                "drought_index":       round(phys_feat.get("drought_index", 0), 3),
                "heat_stress_prob":    round(phys_feat.get("heat_stress_prob", 0), 3),
                "co2_intensity_norm":  round(tran_feat.get("co2_intensity_norm", 0), 3),
                "carbon_policy_risk":  round(tran_feat.get("carbon_policy_risk", 0), 3),
            },
            computed_at      = datetime.now(timezone.utc).isoformat(),
            engine           = engine_name,
        )

    def score_portfolio(
        self,
        assets:   list[dict],   # [{asset_id, physical_risk, transition_risk, value_mm, type}]
        scenario: str  = "baseline",
        n_draws:  int  = 10_000,
    ) -> PortfolioRiskResult:

        total_value = sum(a.get("value_mm", 0) for a in assets)

        if RUST_AVAILABLE:
            try:
                rust_assets = [
                    (
                        a.get("physical_risk", 0.5),
                        a.get("transition_risk", 0.4),
                        a.get("value_mm", 10.0),
                        a.get("type", "infrastructure"),
                    )
                    for a in assets
                ]
                cr, var95, cvar95, loss_mm, div_ratio = _rust.monte_carlo_portfolio(
                    rust_assets, scenario, n_draws
                )
            except Exception as e:
                logger.warning(f"Rust portfolio engine error: {e}")
                cr, var95, cvar95, loss_mm, div_ratio = self._python_portfolio(assets, scenario)
        else:
            cr, var95, cvar95, loss_mm, div_ratio = self._python_portfolio(assets, scenario)

        return PortfolioRiskResult(
            portfolio_composite_risk = cr,
            portfolio_var_95         = var95,
            portfolio_cvar_95        = cvar95,
            loss_expected_mm         = loss_mm,
            diversification_ratio    = div_ratio,
            asset_count              = len(assets),
            total_value_mm           = total_value,
            scenario                 = scenario,
            computed_at              = datetime.now(timezone.utc).isoformat(),
        )

    # ── Python fallback Monte Carlo ────────────────────────────────────────────

    def _python_mc(
        self,
        pr:   float,
        tr:   float,
        val:  float,
        scen: str,
        days: int,
    ) -> tuple:
        """Pure Python Monte Carlo — slower but always available."""
        import random
        import statistics

        scen_mult  = {"ssp585": 1.38, "failed": 1.38, "ssp245": 1.12,
                      "baseline": 1.12, "ssp119": 0.88, "paris": 0.88}.get(scen, 1.0)
        horiz_mult = min(1.3, (days / 365.0) ** 0.5)

        n = min(self.n_draws, 2000)   # Python cap at 2k draws
        losses = []
        for _ in range(n):
            p     = max(0.0, min(1.0, random.gauss(pr, 0.12)))
            t     = max(0.0, min(1.0, random.gauss(tr, 0.10)))
            comp  = (p * 0.6 + t * 0.4) * scen_mult * horiz_mult
            sev   = max(0.0, random.lognormvariate(-1.6, 0.65))
            losses.append(min(comp * sev * val, val * 0.95))

        losses.sort()
        mean_loss  = statistics.mean(losses)
        var95      = losses[int(n * 0.95)]
        cvar95     = statistics.mean(losses[int(n * 0.95):]) if losses[int(n * 0.95):] else var95
        composite  = min(1.0, (pr * 0.6 + tr * 0.4) * scen_mult)
        return composite, var95 / val, cvar95 / val, mean_loss, 0.82

    def _python_portfolio(self, assets: list[dict], scenario: str) -> tuple:
        """Pure Python portfolio risk — weighted average approach."""
        total = sum(a.get("value_mm", 0) for a in assets)
        if total == 0:
            return 0.0, 0.0, 0.0, 0.0, 1.0

        cr = sum(
            (a.get("physical_risk", 0.5) * 0.6 + a.get("transition_risk", 0.4) * 0.4)
            * (a.get("value_mm", 0) / total)
            for a in assets
        )
        var95  = cr * 0.18
        cvar95 = cr * 0.27
        loss   = cr * 0.25 * total
        return cr, var95, cvar95, loss, 0.78

    @staticmethod
    def _var_to_category(variable: str) -> str:
        if "fire"   in variable: return "fire"
        if "temp"   in variable: return "weather"
        if "precip" in variable: return "weather"
        if "co2"    in variable: return "carbon"
        if "carbon" in variable: return "carbon"
        if "wind"   in variable: return "weather"
        if "drought"in variable: return "weather"
        return "telemetry"

