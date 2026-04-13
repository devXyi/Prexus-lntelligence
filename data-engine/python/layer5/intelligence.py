"""
layer5/intelligence.py
Meteorium Engine — Fused Intelligence Scoring
Prexus Intelligence · v2.0.0

Bug fixes applied:
  [BUG-1] CRITICAL — Rust API mismatch: intelligence.py called _rust.monte_carlo_asset()
           and _rust.stress_test_scenarios() but the engine only exposed simulate() and
           simulate_batch(). Both calls would raise AttributeError at runtime.
           Fix: mc_asset.rs added to Rust engine; lib.rs now exposes both functions.

  [BUG-2] Double-counting in _compute_physical_risk: flood_signal was used as a
           base hazard component AND as its own satellite confirmation multiplier.
           sat_flood_confirm = 1.0 + get("flood_signal") * 0.3 applied to a flood
           score already containing flood_signal * 0.7 → quadratic self-amplification.
           Fix: confirmation multipliers now use orthogonal satellite signals only
           (burn_scar → fire, vegetation_stress → drought, flood_signal → flood base
           is weather-derived; flood_signal used once as satellite overlay).

  [BUG-3] _python_mc hardcoded confidence = 0.80 regardless of distribution spread.
           Fix: compute coefficient of variation from the loss distribution and
           derive confidence from it, matching the Rust implementation.

  [BUG-4] Physical risk weights summed to 1.0 with a 0.0 * 0.10 sea-level placeholder,
           silently dropping 10% of weight budget. Made placeholder explicit.
"""

import logging
import math
import random
import statistics as st
from datetime import datetime, timezone
from typing import Optional

from adapters.fusion import (
    SignalFusion, IntelligenceSynthesizer,
    IntelligencePacket, FusedSignal,
)
from adapters.base import TelemetryRecord
from adapters.planet import SatelliteAdapters
from core.config import SCENARIO_MULTIPLIERS, ASSET_VULNERABILITY, MONTE_CARLO_DRAWS
from core.models import AssetRiskResult, PortfolioRiskResult, RiskAlert

logger = logging.getLogger("meteorium.intelligence")

# ── Rust MC ───────────────────────────────────────────────────────────────────
try:
    import meteorium_engine as _rust
    RUST_AVAILABLE = True
    logger.info("Rust MC engine loaded — meteorium_engine v%s", _rust.__version__)
except ImportError:
    RUST_AVAILABLE = False
    logger.warning("Rust MC engine not available — falling back to Python MC")


class FusedRiskEngine:
    """
    Complete intelligence engine.
    Layer 1 telemetry → fusion → amplified risk scoring → structured result.
    """

    def __init__(self, n_draws: int = MONTE_CARLO_DRAWS):
        self.n_draws     = n_draws
        self.fusion      = SignalFusion()
        self.synthesizer = IntelligenceSynthesizer()
        self.satellites  = SatelliteAdapters()

    async def score_with_satellites(
        self,
        asset_id:     str,
        lat:          float,
        lon:          float,
        base_records: list[TelemetryRecord],
        asset_type:   str   = "infrastructure",
        value_mm:     float = 10.0,
        scenario:     str   = "baseline",
        horizon_days: int   = 365,
        country_code: str   = "IND",
    ) -> dict:
        """
        Full fused intelligence pipeline:
          1. Fetch satellite imagery signals
          2. Merge with base telemetry (weather + fire + carbon)
          3. Fuse all signals
          4. Detect compound events
          5. Score with amplification
          6. Monte Carlo — Rust if available, Python fallback
          7. Stress test
          8. Return complete intelligence packet
        """

        # Step 1: Satellite signals
        satellite_records = await self.satellites.fetch_all(lat, lon)
        all_records       = base_records + satellite_records

        logger.info(
            "[Intelligence] %s: %d base + %d satellite = %d total signals",
            asset_id, len(base_records), len(satellite_records), len(all_records),
        )

        # Step 2: Fuse
        fused_signals = self.fusion.fuse(all_records)

        # Step 3: Synthesize intelligence
        packet = self.synthesizer.synthesize(fused_signals, asset_type, country_code)

        # Step 4: Compute risk scores from fused signals
        physical_risk   = self._compute_physical_risk(fused_signals, packet, asset_type)
        transition_risk = self._compute_transition_risk(fused_signals, scenario, horizon_days)

        # Step 5: Compound amplification
        base_physical = physical_risk
        if packet.has_compound_event:
            amp           = packet.max_compound_amplifier
            physical_risk = min(1.0, physical_risk * amp)
            logger.info(
                "[Intelligence] %s: compound amplifier %.2f× → physical %.3f → %.3f",
                asset_id, amp, base_physical, physical_risk,
            )

        # Step 6: Monte Carlo
        # [FIX-BUG-1] Rust functions now exist (mc_asset.rs + updated lib.rs)
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
                engine_tag = f"rust_fused_n{self.n_draws}"
            except Exception as e:
                logger.warning("Rust MC error (falling back to Python): %s", e)
                cr, var95, cvar95, loss_mm, confidence = self._python_mc(
                    physical_risk, transition_risk, value_mm, scenario, horizon_days,
                )
                engine_tag = "python_fused_fallback"
        else:
            cr, var95, cvar95, loss_mm, confidence = self._python_mc(
                physical_risk, transition_risk, value_mm, scenario, horizon_days,
            )
            engine_tag = "python_fused_fallback"

        # Step 7: Alerts
        alerts = self._generate_alerts(asset_id, packet, cr, physical_risk, transition_risk)

        # Step 8: Stress test
        # [FIX-BUG-1] _rust.stress_test_scenarios now exists
        stress = []
        if RUST_AVAILABLE:
            try:
                raw = _rust.stress_test_scenarios(
                    physical_risk,
                    transition_risk,
                    value_mm,
                    asset_type,
                    min(self.n_draws, 5000),
                )
                stress = [
                    {
                        "label":           label,
                        "composite_risk":  round(c, 4),
                        "var_95":          round(v, 4),
                        "expected_loss_mm":round(loss, 2),
                    }
                    for label, c, v, loss in raw
                ]
            except Exception as e:
                logger.warning("Rust stress test error: %s", e)

        return {
            "asset_id":          asset_id,
            "composite_risk":    round(cr,             4),
            "physical_risk":     round(physical_risk,  4),
            "transition_risk":   round(transition_risk,4),
            "var_95":            round(var95,           4),
            "cvar_95":           round(cvar95,          4),
            "loss_expected_mm":  round(loss_mm,         2),
            "confidence":        round(confidence,      4),
            "scenario":          scenario,
            "horizon_days":      horizon_days,
            "intelligence":      packet.to_dict(),
            "compound_events":   packet.compound_events,
            "compound_amplifier":round(packet.max_compound_amplifier, 3),
            "critical_signals":  packet.critical_signals,
            "satellite_signals": len(satellite_records),
            "stress_scenarios":  stress,
            "alerts":            [a.to_dict() for a in alerts],
            "engine":            engine_tag,
            "sources": {
                "base_telemetry":  len(base_records),
                "satellite":       len(satellite_records),
                "total_signals":   len(all_records),
                "fused_variables": len(fused_signals),
                "active_sources":  packet.active_sources,
            },
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── Risk scoring ──────────────────────────────────────────────────────────

    def _compute_physical_risk(
        self,
        fused:      dict[str, FusedSignal],
        packet:     IntelligencePacket,
        asset_type: str,
    ) -> float:
        """
        Physical risk from fused signals with satellite confirmation.

        Satellite signals CONFIRM weather-model signals, boosting weight
        when both agree. They are NOT applied as self-referential multipliers.

        [FIX-BUG-2] Previous version applied flood_signal both as a base
        component (weight 0.7) and as its own confirmation multiplier (+0.3),
        effectively squaring it. Fixed: confirmation multipliers use
        orthogonal satellite signals only.
        """
        vuln = ASSET_VULNERABILITY.get(asset_type.lower(), 1.0)

        def get(var: str, default: float = 0.0) -> float:
            return fused[var].value if var in fused else default

        # ── Base hazard scores from weather / fire models ──────────────────
        heat = max(
            self._sigmoid(get("temp_anomaly_c"), center=1.5, steepness=0.8),
            get("heat_stress_prob_7d"),
        )
        drought = min(
            1.0,
            get("drought_index")
            + max(0.0, -get("precip_anomaly_pct") / 100.0) * 0.3,
        )
        fire    = min(1.0, get("fire_prob_100km") * 0.6 + get("fire_hazard_score") * 0.4)
        wind    = min(
            1.0,
            get("extreme_wind_prob_7d") * 0.7
            + max(0.0, (get("wind_speed_ms") - 15.0) / 25.0) * 0.3,
        )

        # ── Flood: weather base + satellite overlay (no double-count) ──────
        # Base: precipitation excess as a flood proxy
        precip_flood = max(0.0, get("precip_anomaly_pct") / 80.0)   # 80% anomaly → full signal
        # Satellite: independent confirmation via SAR/NDWI
        sat_flood    = get("flood_signal")                            # orthogonal source
        flood        = min(1.0, precip_flood * 0.50 + sat_flood * 0.50)

        # ── Satellite confirmation multipliers (orthogonal signals only) ───
        # burn_scar confirms past fire → boost current fire score
        fire    = min(1.0, fire    * (1.0 + get("burn_scar_signal")    * 0.40))
        # vegetation_stress confirms drought (satellite-derived, not weather)
        drought = min(1.0, drought * (1.0 + get("vegetation_stress")   * 0.30))
        # ndvi loss confirms heat or drought stress (inverted: low NDVI = high stress)
        ndvi_stress = max(0.0, 0.5 - get("ndvi", 0.5))              # 0 at NDVI=0.5, 0.5 at NDVI=0
        heat    = min(1.0, heat    * (1.0 + ndvi_stress             * 0.20))

        # ── Weighted composite ─────────────────────────────────────────────
        # [FIX-BUG-4] Sea-level placeholder made explicit with zero weight
        # so the weight budget is transparent and sum-to-1 is verified.
        sea_level = 0.0   # TODO: integrate NOAA SLR projections (Raksha engine)

        composite = (
            heat      * 0.22
            + drought * 0.20
            + fire    * 0.18
            + flood   * 0.18
            + wind    * 0.12
            + sea_level * 0.10   # placeholder — weights sum to 1.0
        )

        return min(1.0, max(0.0, composite * vuln))

    def _compute_transition_risk(
        self,
        fused:        dict[str, FusedSignal],
        scenario:     str,
        horizon_days: int,
    ) -> float:
        def get(var: str, default: float = 0.0) -> float:
            return fused[var].value if var in fused else default

        carbon_price_by_scenario = {
            "ssp119": 250.0, "paris": 250.0,
            "ssp245": 100.0, "baseline": 80.0,
            "ssp370":  55.0, "ssp585": 20.0, "failed": 20.0,
        }
        carbon_price = carbon_price_by_scenario.get(scenario, 80.0)
        price_norm   = min(1.0, carbon_price / 250.0)
        horizon_amp  = min(1.3, 1.0 + (horizon_days / 365.0) * 0.15)

        composite = (
            get("co2_intensity_norm")    * 0.30
            + get("carbon_policy_risk")  * 0.25
            + get("transition_risk_score")* 0.25
            + price_norm                 * 0.20
        ) * horizon_amp

        return min(1.0, max(0.0, composite))

    def _generate_alerts(
        self,
        asset_id:        str,
        packet:          IntelligencePacket,
        composite_risk:  float,
        physical_risk:   float,
        transition_risk: float,
    ) -> list[RiskAlert]:
        alerts = []
        now    = datetime.now(timezone.utc).isoformat()

        for event in packet.compound_events:
            alerts.append(RiskAlert(
                alert_id  = f"AL-{asset_id}-COMPOUND-{event['type'][:8].upper()}",
                asset_id  = asset_id,
                severity  = event["severity"],
                risk_type = "PHYSICAL",
                message   = (
                    event["description"]
                    + f" Damage amplifier: {event['amplifier']:.1f}×"
                ),
                score     = composite_risk,
                source    = "Meteorium Fusion Engine · Compound Event Detection",
                timestamp = now,
            ))

        for sig in packet.critical_signals:
            alerts.append(RiskAlert(
                alert_id  = f"AL-{asset_id}-SIG-{sig['variable'][:8].upper()}",
                asset_id  = asset_id,
                severity  = "CRITICAL" if sig["anomaly_score"] > 0.7 else "HIGH",
                risk_type = "PHYSICAL" if sig["category"] != "transition_risk" else "TRANSITION",
                message   = (
                    f"Critical signal: {sig['variable']} = {sig['value']:.3f} "
                    f"(anomaly score: {sig['anomaly_score']:.2f})"
                ),
                score     = float(sig["value"]),
                source    = ", ".join(sig["sources"]),
                timestamp = now,
            ))

        return alerts

    # ── Python MC fallback ────────────────────────────────────────────────────

    def _python_mc(
        self,
        pr:   float,
        tr:   float,
        val:  float,
        scen: str,
        days: int,
    ) -> tuple[float, float, float, float, float]:
        """
        Pure-Python Monte Carlo fallback.
        Mirrors Rust mc_asset.rs logic exactly so results are comparable.

        [FIX-BUG-3] Confidence was hardcoded 0.80.
        Now computed from coefficient of variation of the loss distribution,
        matching Rust implementation.
        """
        s = SCENARIO_MULTIPLIERS.get(scen, 1.0)
        h = min(1.3, 1.0 + (days / 365.0) * 0.15)
        n = min(self.n_draws, 2_000)

        losses = []
        for _ in range(n):
            p    = max(0.0, min(1.0, random.gauss(pr, 0.12)))
            t    = max(0.0, min(1.0, random.gauss(tr, 0.10)))
            comp = (p * 0.60 + t * 0.40) * s * h
            sev  = max(0.0, random.lognormvariate(-1.60, 0.65))
            losses.append(min(comp * sev * val, val * 0.95))

        losses.sort()
        mean_l = st.mean(losses)
        idx95  = int(n * 0.95)
        v95    = losses[idx95]
        cv95   = st.mean(losses[idx95:]) if losses[idx95:] else v95

        # [FIX-BUG-3] Confidence from distribution tightness, not hardcoded
        if mean_l > 1e-9:
            std_l      = st.stdev(losses)
            cv         = std_l / mean_l
            confidence = min(0.97, max(0.60, 1.0 - cv * 0.25))
        else:
            confidence = 0.60   # no data confidence floor

        cr = min(1.0, (pr * 0.60 + tr * 0.40) * s)
        return cr, v95 / val, cv95 / val, mean_l, confidence

    @staticmethod
    def _sigmoid(x: float, center: float = 0.0, steepness: float = 1.0) -> float:
        return 1.0 / (1.0 + math.exp(-steepness * (x - center)))
