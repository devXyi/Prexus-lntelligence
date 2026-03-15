"""
layer5/engine.py
Meteorium Engine — LAYER 5: Risk Computation Engine
Converts AssetFeatures vectors into probabilistic risk scores.
Calls Rust Monte Carlo core via PyO3. Python fallback if Rust unavailable.

Components:
  PhysicalRiskScorer    — hazard × exposure × vulnerability
  TransitionRiskScorer  — carbon price trajectory × asset intensity
  RiskEngine            — orchestrates all scorers + Rust MC
  AlertGenerator        — generates structured risk alerts
"""

import logging
import math
from datetime import datetime, timezone
from typing import Optional

from core.config import (
    SCENARIO_MULTIPLIERS, ASSET_VULNERABILITY,
    RISK_ALERT_THRESHOLDS, MONTE_CARLO_DRAWS
)
from core.models import (
    AssetFeatures, AssetRiskResult, PortfolioRiskResult, RiskAlert
)

logger = logging.getLogger("meteorium.layer5")

# ─── Try Rust core ────────────────────────────────────────────────────────────
try:
    import meteorium_engine as _rust
    RUST_AVAILABLE = True
    logger.info("✓ Rust Monte Carlo engine loaded (meteorium_engine)")
except ImportError:
    RUST_AVAILABLE = False
    logger.warning("⚠ Rust engine not found — Python fallback active. "
                   "Build: cd rust && maturin develop --release")


# ════════════════════════════════════════════════════════════════════════════
# PHYSICAL RISK SCORER
# ════════════════════════════════════════════════════════════════════════════

class PhysicalRiskScorer:
    """
    Scores physical climate hazard exposure.
    Weights calibrated against IPCC AR6 WG-II damage functions.
    """

    # Hazard weights — must sum to 1.0
    HAZARD_WEIGHTS = {
        "heat":       0.22,
        "drought":    0.20,
        "fire":       0.18,
        "flood":      0.18,
        "wind":       0.12,
        "sea_level":  0.10,
    }

    def score(self, features: AssetFeatures, asset_type: str = "infrastructure") -> float:
        scores = {}

        # ── Heat ──────────────────────────────────────────────────────────────
        heat_score = self._sigmoid(features.temp_anomaly_c, center=1.5, steepness=0.8)
        heat_score = max(heat_score, features.heat_stress_prob)
        scores["heat"] = heat_score

        # ── Drought ───────────────────────────────────────────────────────────
        drought_from_precip = max(0.0, -features.precip_anomaly_pct / 100.0) * 0.5
        scores["drought"] = min(1.0, features.drought_index + drought_from_precip)

        # ── Fire ──────────────────────────────────────────────────────────────
        scores["fire"] = min(1.0,
            features.fire_prob_100km * 0.6 +
            features.fire_hazard_score * 0.4
        )

        # ── Flood ─────────────────────────────────────────────────────────────
        precip_flood = max(0.0, features.precip_anomaly_pct / 100.0) * 0.7
        soil_sat     = max(0.0, (features.soil_moisture - 0.3) / 0.4)
        scores["flood"] = min(1.0,
            features.flood_susceptibility * 0.5 +
            precip_flood * 0.3 +
            soil_sat * 0.2
        )

        # ── Wind ──────────────────────────────────────────────────────────────
        wind_norm = min(1.0, max(0.0, (features.wind_speed_ms - 15.0) / 25.0))
        scores["wind"] = min(1.0,
            features.extreme_wind_prob * 0.7 + wind_norm * 0.3
        )

        # ── Sea level (placeholder — enhanced with CMEMS) ─────────────────────
        scores["sea_level"] = 0.0

        # ── Weighted composite ────────────────────────────────────────────────
        composite = sum(
            scores.get(h, 0.0) * w
            for h, w in self.HAZARD_WEIGHTS.items()
        )

        vuln = ASSET_VULNERABILITY.get(asset_type.lower(), 1.0)
        return min(1.0, max(0.0, composite * vuln))

    def decompose(self, features: AssetFeatures, asset_type: str = "infrastructure") -> dict:
        """Return per-hazard breakdown."""
        vuln  = ASSET_VULNERABILITY.get(asset_type.lower(), 1.0)
        return {
            "heat":      round(self._sigmoid(features.temp_anomaly_c, 1.5, 0.8) * self.HAZARD_WEIGHTS["heat"] * vuln, 4),
            "drought":   round(min(1.0, features.drought_index) * self.HAZARD_WEIGHTS["drought"] * vuln, 4),
            "fire":      round(features.fire_prob_100km * self.HAZARD_WEIGHTS["fire"] * vuln, 4),
            "flood":     round(features.flood_susceptibility * self.HAZARD_WEIGHTS["flood"] * vuln, 4),
            "wind":      round(features.extreme_wind_prob * self.HAZARD_WEIGHTS["wind"] * vuln, 4),
            "sea_level": 0.0,
        }

    @staticmethod
    def _sigmoid(x: float, center: float = 0.0, steepness: float = 1.0) -> float:
        return 1.0 / (1.0 + math.exp(-steepness * (x - center)))


# ════════════════════════════════════════════════════════════════════════════
# TRANSITION RISK SCORER
# ════════════════════════════════════════════════════════════════════════════

class TransitionRiskScorer:
    """
    Scores transition risk: financial exposure to the low-carbon transition.
    Models carbon price trajectory × asset carbon intensity.
    """

    # Carbon price in USD/tCO2 under each scenario by 2030
    CARBON_PRICE_2030 = {
        "ssp119":   250.0,
        "paris":    250.0,
        "ssp245":   100.0,
        "baseline":  80.0,
        "ssp370":    55.0,
        "ssp585":    20.0,
        "failed":    20.0,
    }

    def score(
        self,
        features:     AssetFeatures,
        scenario:     str = "baseline",
        horizon_days: int = 365,
    ) -> float:
        carbon_intensity  = features.co2_intensity_norm
        policy_risk       = features.carbon_policy_risk
        tr_raw            = features.transition_risk_score
        yoy_change        = features.emissions_yoy_pct

        # Carbon price trajectory risk
        carbon_price    = self.CARBON_PRICE_2030.get(scenario, 80.0)
        price_risk_norm = min(1.0, carbon_price / 250.0)

        # Emissions trajectory penalty
        traj_penalty = max(0.0, yoy_change / 100.0) * 0.2

        # Horizon amplifier — longer = more policy exposure
        horizon_amp = min(1.3, 1.0 + (horizon_days / 365.0) * 0.15)

        composite = (
            carbon_intensity  * 0.30 +
            policy_risk       * 0.25 +
            tr_raw            * 0.25 +
            price_risk_norm   * 0.20 +
            traj_penalty
        ) * horizon_amp

        return min(1.0, max(0.0, composite))


# ════════════════════════════════════════════════════════════════════════════
# ALERT GENERATOR
# ════════════════════════════════════════════════════════════════════════════

class AlertGenerator:

    def generate(
        self,
        asset_id:         str,
        features:         AssetFeatures,
        physical_risk:    float,
        transition_risk:  float,
        composite_risk:   float,
    ) -> list[RiskAlert]:
        alerts = []
        now    = datetime.now(timezone.utc).isoformat()

        # Composite threshold alert
        for level, threshold in sorted(
            RISK_ALERT_THRESHOLDS.items(),
            key=lambda x: x[1],
            reverse=True
        ):
            if composite_risk >= threshold and threshold > 0.25:
                alerts.append(RiskAlert(
                    alert_id  = f"AL-{asset_id}-COMP-{level[:3]}",
                    asset_id  = asset_id,
                    severity  = level,
                    risk_type = "COMPOSITE",
                    message   = self._composite_message(asset_id, level, composite_risk),
                    score     = composite_risk,
                    source    = "Meteorium Risk Engine · IPCC AR6",
                    timestamp = now,
                ))
                break

        # Fire alert
        if features.fire_prob_100km >= 0.40:
            sev = "CRITICAL" if features.fire_prob_100km >= 0.70 else "HIGH"
            alerts.append(RiskAlert(
                alert_id  = f"AL-{asset_id}-FIRE",
                asset_id  = asset_id,
                severity  = sev,
                risk_type = "PHYSICAL",
                message   = f"Active fire probability within 100km: {features.fire_prob_100km:.0%}. "
                             f"Fire Radiative Power indicates {'intense' if features.fire_hazard_score > 0.5 else 'moderate'} activity.",
                score     = features.fire_prob_100km,
                source    = "NASA FIRMS VIIRS 375m",
                timestamp = now,
            ))

        # Drought alert
        if features.drought_index >= 0.55:
            alerts.append(RiskAlert(
                alert_id  = f"AL-{asset_id}-DROUGHT",
                asset_id  = asset_id,
                severity  = "HIGH" if features.drought_index >= 0.75 else "ELEVATED",
                risk_type = "PHYSICAL",
                message   = f"Drought index {features.drought_index:.2f}. "
                             f"Precipitation anomaly {features.precip_anomaly_pct:+.1f}% vs 10-yr baseline.",
                score     = features.drought_index,
                source    = "Open-Meteo ERA5 · Soil Moisture Analysis",
                timestamp = now,
            ))

        # Heat alert
        if features.heat_stress_prob >= 0.50:
            alerts.append(RiskAlert(
                alert_id  = f"AL-{asset_id}-HEAT",
                asset_id  = asset_id,
                severity  = "HIGH" if features.heat_stress_prob >= 0.71 else "ELEVATED",
                risk_type = "PHYSICAL",
                message   = f"Heat stress probability (7-day): {features.heat_stress_prob:.0%}. "
                             f"Temperature anomaly {features.temp_anomaly_c:+.1f}°C above baseline.",
                score     = features.heat_stress_prob,
                source    = "Open-Meteo ECMWF Forecast",
                timestamp = now,
            ))

        # Transition alert
        if features.carbon_policy_risk >= 0.65:
            alerts.append(RiskAlert(
                alert_id  = f"AL-{asset_id}-TR",
                asset_id  = asset_id,
                severity  = "HIGH" if features.carbon_policy_risk >= 0.80 else "ELEVATED",
                risk_type = "TRANSITION",
                message   = f"Carbon policy risk {features.carbon_policy_risk:.2f}. "
                             f"CO₂ intensity {features.co2_intensity_norm:.2f}/1.0 normalized. "
                             f"Emissions trend: {features.emissions_yoy_pct:+.1f}% YoY.",
                score     = features.carbon_policy_risk,
                source    = "Carbon Monitor · Transition Risk Model",
                timestamp = now,
            ))

        return alerts

    @staticmethod
    def _composite_message(asset_id: str, level: str, score: float) -> str:
        msgs = {
            "CRITICAL": f"{asset_id} — CRITICAL risk threshold exceeded ({score:.0%}). Immediate board-level review required.",
            "HIGH":     f"{asset_id} — HIGH risk ({score:.0%}). Mitigation plan activation recommended.",
            "ELEVATED": f"{asset_id} — ELEVATED risk ({score:.0%}). Enhanced monitoring protocol active.",
        }
        return msgs.get(level, f"{asset_id} — Risk score {score:.0%}")


# ════════════════════════════════════════════════════════════════════════════
# MAIN RISK ENGINE
# ════════════════════════════════════════════════════════════════════════════

class RiskEngine:

    def __init__(self, n_draws: int = MONTE_CARLO_DRAWS):
        self.n_draws       = n_draws
        self.phys_scorer   = PhysicalRiskScorer()
        self.tran_scorer   = TransitionRiskScorer()
        self.alert_gen     = AlertGenerator()

    def score_asset(
        self,
        asset_id:     str,
        features:     AssetFeatures,
        asset_type:   str  = "infrastructure",
        value_mm:     float = 10.0,
        scenario:     str  = "baseline",
        horizon_days: int  = 365,
    ) -> AssetRiskResult:
        """Full risk assessment for a single asset."""

        physical_risk   = self.phys_scorer.score(features, asset_type)
        transition_risk = self.tran_scorer.score(features, scenario, horizon_days)

        # ── Monte Carlo: Rust preferred, Python fallback ──────────────────────
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
                engine_name = f"rust_pyo3_n{self.n_draws}"
            except Exception as e:
                logger.warning(f"Rust MC error: {e} — Python fallback")
                cr, var95, cvar95, loss_mm, confidence = self._python_mc(
                    physical_risk, transition_risk, value_mm, scenario, horizon_days
                )
                engine_name = "python_mc_fallback"
        else:
            cr, var95, cvar95, loss_mm, confidence = self._python_mc(
                physical_risk, transition_risk, value_mm, scenario, horizon_days
            )
            engine_name = "python_mc_fallback"

        # ── Stress test (Rust only) ───────────────────────────────────────────
        stress = []
        if RUST_AVAILABLE:
            try:
                raw = _rust.stress_test_scenarios(
                    physical_risk, transition_risk, value_mm,
                    asset_type, min(self.n_draws, 5000)
                )
                stress = [
                    {"label": label, "composite_risk": round(c, 4),
                     "var_95": round(v, 4), "expected_loss_mm": round(l, 2)}
                    for label, c, v, l in raw
                ]
            except Exception:
                pass

        # ── Hazard decomposition ──────────────────────────────────────────────
        hazard_breakdown = self.phys_scorer.decompose(features, asset_type)

        # ── Source summary ────────────────────────────────────────────────────
        source_summary: dict[str, str] = {}
        for var, src in features.sources.items():
            cat = self._var_category(var)
            if cat not in source_summary:
                source_summary[cat] = src

        # ── Alerts ────────────────────────────────────────────────────────────
        alerts = self.alert_gen.generate(
            asset_id, features, physical_risk, transition_risk, cr
        )

        # ── Feature snapshot ──────────────────────────────────────────────────
        snapshot = {
            "temp_anomaly_c":       round(features.temp_anomaly_c,        3),
            "fire_prob_100km":      round(features.fire_prob_100km,        3),
            "drought_index":        round(features.drought_index,          3),
            "heat_stress_prob":     round(features.heat_stress_prob,       3),
            "co2_intensity_norm":   round(features.co2_intensity_norm,     3),
            "carbon_policy_risk":   round(features.carbon_policy_risk,     3),
            "flood_susceptibility": round(features.flood_susceptibility,   3),
            "hazard_breakdown":     hazard_breakdown,
        }

        return AssetRiskResult(
            asset_id         = asset_id,
            composite_risk   = cr,
            physical_risk    = physical_risk,
            transition_risk  = transition_risk,
            var_95           = var95,
            cvar_95          = cvar95,
            loss_expected_mm = loss_mm,
            confidence       = min(confidence, features.confidence * 1.1),
            scenario         = scenario,
            horizon_days     = horizon_days,
            sources          = source_summary,
            feature_snapshot = snapshot,
            stress_scenarios = stress,
            alerts           = alerts,
            engine           = engine_name,
            computed_at      = datetime.now(timezone.utc).isoformat(),
        )

    def score_portfolio(
        self,
        assets:   list[dict],
        scenario: str = "baseline",
    ) -> PortfolioRiskResult:
        """Correlated portfolio risk."""
        total_value = sum(a.get("value_mm", 0) for a in assets)

        if RUST_AVAILABLE:
            try:
                rust_assets = [
                    (
                        a.get("physical_risk",   0.5),
                        a.get("transition_risk", 0.4),
                        a.get("value_mm",        10.0),
                        a.get("type",            "infrastructure"),
                    )
                    for a in assets
                ]
                cr, var95, cvar95, loss_mm, div = _rust.monte_carlo_portfolio(
                    rust_assets, scenario, min(self.n_draws, 5000)
                )
            except Exception as e:
                logger.warning(f"Rust portfolio error: {e}")
                cr, var95, cvar95, loss_mm, div = self._python_portfolio(assets, scenario)
        else:
            cr, var95, cvar95, loss_mm, div = self._python_portfolio(assets, scenario)

        return PortfolioRiskResult(
            portfolio_composite_risk = cr,
            portfolio_var_95         = var95,
            portfolio_cvar_95        = cvar95,
            loss_expected_mm         = loss_mm,
            diversification_ratio    = div,
            asset_count              = len(assets),
            total_value_mm           = total_value,
            scenario                 = scenario,
            asset_breakdown          = [
                {
                    "asset_id":        a.get("asset_id", "?"),
                    "composite_risk":  round(a.get("composite_risk", 0), 4),
                    "physical_risk":   round(a.get("physical_risk",  0), 4),
                    "transition_risk": round(a.get("transition_risk",0), 4),
                    "value_mm":        a.get("value_mm", 0),
                    "weight":          round(a.get("value_mm", 0) / max(total_value, 1), 4),
                }
                for a in assets
            ],
            computed_at = datetime.now(timezone.utc).isoformat(),
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
        import random, statistics
        s_mult = SCENARIO_MULTIPLIERS.get(scen, 1.0)
        h_mult = min(1.3, (days / 365.0) ** 0.5)
        n      = min(self.n_draws, 2000)   # cap Python draws

        losses = []
        for _ in range(n):
            p    = max(0.0, min(1.0, random.gauss(pr, 0.12)))
            t    = max(0.0, min(1.0, random.gauss(tr, 0.10)))
            comp = (p * 0.6 + t * 0.4) * s_mult * h_mult
            sev  = max(0.0, random.lognormvariate(-1.6, 0.65))
            losses.append(min(comp * sev * val, val * 0.95))

        losses.sort()
        mean_l = statistics.mean(losses)
        var95  = losses[int(n * 0.95)]
        cvar95 = statistics.mean(losses[int(n * 0.95):]) if losses[int(n * 0.95):] else var95
        cr     = min(1.0, (pr * 0.6 + tr * 0.4) * s_mult)
        return cr, var95 / val, cvar95 / val, mean_l, 0.82

    def _python_portfolio(self, assets: list[dict], scenario: str) -> tuple:
        total = sum(a.get("value_mm", 0) for a in assets)
        if total == 0:
            return 0.0, 0.0, 0.0, 0.0, 1.0
        s_mult = SCENARIO_MULTIPLIERS.get(scenario, 1.0)
        cr     = sum(
            (a.get("physical_risk", 0.5) * 0.6 + a.get("transition_risk", 0.4) * 0.4)
            * s_mult * (a.get("value_mm", 0) / total)
            for a in assets
        )
        return cr, cr * 0.18, cr * 0.27, cr * 0.25 * total, 0.78

    @staticmethod
    def _var_category(variable: str) -> str:
        if "fire"    in variable: return "fire"
        if "temp"    in variable: return "weather"
        if "precip"  in variable: return "weather"
        if "co2"     in variable: return "carbon"
        if "carbon"  in variable: return "carbon"
        if "drought" in variable: return "weather"
        if "wind"    in variable: return "weather"
        return "telemetry"
