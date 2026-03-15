"""
adapters/fusion.py
Meteorium Engine — Multi-Signal Data Fusion
This is what your research correctly identified as the decisive advantage.
Not the satellite data. The fusion of satellite + weather + carbon +
economic + geopolitical signals into one coherent intelligence picture.

"Satellites are the eyes. The brain is what interprets them."

Fusion pipeline:
  Raw signals (N sources, M variables)
      ↓
  Signal normalization (0-1 scale per variable)
      ↓
  Confidence weighting (freshness × source reliability)
      ↓
  Cross-signal correlation detection
      ↓
  Fused feature vector (single authoritative value per variable)
      ↓
  Anomaly detection (deviation from historical baseline)
      ↓
  Intelligence summary (structured narrative + scores)
"""

import logging
import math
import statistics
from datetime import datetime, timezone, timedelta
from typing import Optional

from adapters.base import TelemetryRecord

logger = logging.getLogger("meteorium.fusion")


# ════════════════════════════════════════════════════════════════════════════
# SIGNAL REGISTRY
# Maps variable names to their fusion parameters.
# Defines: how to weight, which sources are authoritative,
# and what the normal range is for anomaly detection.
# ════════════════════════════════════════════════════════════════════════════

SIGNAL_REGISTRY = {

    # ── Weather signals ───────────────────────────────────────────────────
    "temp_anomaly_c": {
        "authoritative_sources": ["ECMWF ERA5 Reanalysis", "Open-Meteo / ECMWF"],
        "fallback_sources":      ["NOAA GFS"],
        "normal_range":          (-2.0, 2.0),
        "critical_threshold":    3.5,
        "fusion_method":         "confidence_weighted_mean",
        "category":              "physical_hazard",
        "weight":                0.18,
    },
    "precip_anomaly_pct": {
        "authoritative_sources": ["Open-Meteo / ECMWF", "ECMWF ERA5 Reanalysis"],
        "normal_range":          (-30.0, 30.0),
        "critical_threshold":    80.0,
        "fusion_method":         "confidence_weighted_mean",
        "category":              "physical_hazard",
        "weight":                0.15,
    },
    "drought_index": {
        "authoritative_sources": ["Open-Meteo / ECMWF"],
        "normal_range":          (0.0, 0.3),
        "critical_threshold":    0.75,
        "fusion_method":         "max_confidence",
        "category":              "physical_hazard",
        "weight":                0.14,
    },
    "heat_stress_prob_7d": {
        "authoritative_sources": ["Open-Meteo / ECMWF"],
        "normal_range":          (0.0, 0.2),
        "critical_threshold":    0.7,
        "fusion_method":         "max_confidence",
        "category":              "physical_hazard",
        "weight":                0.12,
    },

    # ── Fire signals ──────────────────────────────────────────────────────
    "fire_prob_100km": {
        "authoritative_sources": ["NASA FIRMS VIIRS 375m"],
        "fallback_sources":      ["NASA FIRMS MODIS 1km"],
        "normal_range":          (0.0, 0.1),
        "critical_threshold":    0.5,
        "fusion_method":         "max_value",
        "category":              "physical_hazard",
        "weight":                0.13,
    },
    "fire_hazard_score": {
        "authoritative_sources": ["NASA FIRMS VIIRS 375m"],
        "normal_range":          (0.0, 0.15),
        "critical_threshold":    0.6,
        "fusion_method":         "max_value",
        "category":              "physical_hazard",
        "weight":                0.08,
    },

    # ── Vegetation / land cover signals (satellite-derived) ───────────────
    "ndvi": {
        "authoritative_sources": ["Sentinel Hub / ESA Copernicus", "Microsoft Planetary Computer"],
        "fallback_sources":      ["NASA FIRMS VIIRS 375m"],
        "normal_range":          (0.2, 0.8),
        "critical_threshold":    0.1,         # below this = severe stress
        "fusion_method":         "confidence_weighted_mean",
        "category":              "satellite_intelligence",
        "weight":                0.06,
        "invert":                True,         # lower NDVI = higher risk
    },
    "vegetation_stress": {
        "authoritative_sources": ["Sentinel Hub / ESA Copernicus"],
        "normal_range":          (0.0, 0.2),
        "critical_threshold":    0.6,
        "fusion_method":         "max_confidence",
        "category":              "satellite_intelligence",
        "weight":                0.07,
    },
    "flood_signal": {
        "authoritative_sources": ["Sentinel Hub / ESA Copernicus"],
        "normal_range":          (0.0, 0.1),
        "critical_threshold":    0.5,
        "fusion_method":         "max_value",
        "category":              "satellite_intelligence",
        "weight":                0.09,
    },
    "burn_scar_signal": {
        "authoritative_sources": ["Sentinel Hub / ESA Copernicus"],
        "normal_range":          (0.0, 0.05),
        "critical_threshold":    0.4,
        "fusion_method":         "max_value",
        "category":              "satellite_intelligence",
        "weight":                0.06,
    },

    # ── Transition risk signals ───────────────────────────────────────────
    "co2_intensity_norm": {
        "authoritative_sources": ["Carbon Monitor"],
        "normal_range":          (0.0, 0.6),
        "critical_threshold":    0.85,
        "fusion_method":         "confidence_weighted_mean",
        "category":              "transition_risk",
        "weight":                0.10,
    },
    "carbon_policy_risk": {
        "authoritative_sources": ["Carbon Monitor"],
        "normal_range":          (0.0, 0.5),
        "critical_threshold":    0.80,
        "fusion_method":         "confidence_weighted_mean",
        "category":              "transition_risk",
        "weight":                0.08,
    },
}


# ════════════════════════════════════════════════════════════════════════════
# FUSION ENGINE
# ════════════════════════════════════════════════════════════════════════════

class SignalFusion:
    """
    Fuses TelemetryRecords from multiple sources into a single
    authoritative feature vector per variable.

    Fusion methods:
      confidence_weighted_mean — weighted average by confidence score
      max_confidence           — take value from highest-confidence source
      max_value                — take the maximum value (conservative for risk)
    """

    def __init__(self, staleness_hours: float = 48.0):
        self.staleness_hours = staleness_hours

    def fuse(self, records: list[TelemetryRecord]) -> dict[str, "FusedSignal"]:
        """
        Fuse all TelemetryRecords into one FusedSignal per variable.
        Returns dict keyed by variable name.
        """
        # Group records by variable
        by_variable: dict[str, list[TelemetryRecord]] = {}
        for rec in records:
            if rec.variable not in by_variable:
                by_variable[rec.variable] = []
            by_variable[rec.variable].append(rec)

        fused: dict[str, FusedSignal] = {}
        for variable, recs in by_variable.items():
            signal = self._fuse_variable(variable, recs)
            if signal:
                fused[variable] = signal

        return fused

    def _fuse_variable(
        self,
        variable: str,
        records:  list[TelemetryRecord],
    ) -> Optional["FusedSignal"]:
        """Fuse multiple records of the same variable."""

        # Filter out stale records
        fresh = [
            r for r in records
            if r.freshness_hours <= self.staleness_hours and r.confidence > 0.0
        ]
        if not fresh:
            # Use most recent even if stale, with degraded confidence
            fresh = sorted(records, key=lambda r: r.freshness_hours)[:1]
            if not fresh:
                return None

        reg = SIGNAL_REGISTRY.get(variable, {})
        method = reg.get("fusion_method", "confidence_weighted_mean")

        if method == "max_value":
            best = max(fresh, key=lambda r: r.value)
            value = best.value
            conf  = best.confidence
            sources = [best.source]

        elif method == "max_confidence":
            best = max(fresh, key=lambda r: r.confidence)
            value = best.value
            conf  = best.confidence
            sources = [best.source]

        else:   # confidence_weighted_mean (default)
            total_weight = sum(r.confidence for r in fresh)
            if total_weight == 0:
                value = fresh[0].value
                conf  = 0.0
            else:
                value = sum(r.value * r.confidence for r in fresh) / total_weight
                conf  = total_weight / len(fresh)
            sources = list({r.source for r in fresh})

        # Freshness penalty — reduce confidence for old data
        avg_freshness = sum(r.freshness_hours for r in fresh) / len(fresh)
        freshness_factor = max(0.3, 1.0 - avg_freshness / self.staleness_hours)
        conf = conf * freshness_factor

        # Anomaly scoring
        normal_range = reg.get("normal_range")
        anomaly_score = self._anomaly_score(value, normal_range) if normal_range else 0.0

        return FusedSignal(
            variable      = variable,
            value         = value,
            confidence    = min(1.0, conf),
            sources       = sources,
            source_count  = len(fresh),
            anomaly_score = anomaly_score,
            category      = reg.get("category", "unknown"),
            is_critical   = value >= reg.get("critical_threshold", float("inf"))
                            if not reg.get("invert") else value <= reg.get("critical_threshold", 0.0),
        )

    @staticmethod
    def _anomaly_score(value: float, normal_range: tuple) -> float:
        """How far outside the normal range is this value? 0=normal, 1=extreme."""
        low, high = normal_range
        if low <= value <= high:
            return 0.0
        width = max(high - low, 0.001)
        if value > high:
            return min(1.0, (value - high) / width)
        return min(1.0, (low - value) / width)


# ════════════════════════════════════════════════════════════════════════════
# FUSED SIGNAL
# ════════════════════════════════════════════════════════════════════════════

class FusedSignal:
    """Single fused variable with provenance and anomaly metadata."""

    def __init__(
        self,
        variable:      str,
        value:         float,
        confidence:    float,
        sources:       list[str],
        source_count:  int,
        anomaly_score: float,
        category:      str,
        is_critical:   bool,
    ):
        self.variable      = variable
        self.value         = value
        self.confidence    = confidence
        self.sources       = sources
        self.source_count  = source_count
        self.anomaly_score = anomaly_score
        self.category      = category
        self.is_critical   = is_critical

    def to_dict(self) -> dict:
        return {
            "variable":      self.variable,
            "value":         round(self.value, 5),
            "confidence":    round(self.confidence, 4),
            "sources":       self.sources,
            "source_count":  self.source_count,
            "anomaly_score": round(self.anomaly_score, 4),
            "category":      self.category,
            "is_critical":   self.is_critical,
        }


# ════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE SYNTHESIZER
# The "brain" that turns fused signals into narrative intelligence
# ════════════════════════════════════════════════════════════════════════════

class IntelligenceSynthesizer:
    """
    Transforms a dict of FusedSignals into structured intelligence:
      - Risk category scores (physical, transition, satellite)
      - Critical signal flags
      - Correlated anomaly detection (fire + drought + heat together)
      - Intelligence summary for AI narrative generation
    """

    def synthesize(
        self,
        fused:        dict[str, FusedSignal],
        asset_type:   str = "infrastructure",
        country_code: str = "IND",
    ) -> "IntelligencePacket":

        # Categorize signals
        physical    = {k: v for k, v in fused.items() if v.category == "physical_hazard"}
        satellite   = {k: v for k, v in fused.items() if v.category == "satellite_intelligence"}
        transition  = {k: v for k, v in fused.items() if v.category == "transition_risk"}

        # Compute category scores (confidence-weighted)
        phys_score  = self._category_score(physical)
        sat_score   = self._category_score(satellite)
        tr_score    = self._category_score(transition)

        # Detect compound events — when multiple hazards co-occur
        compounds   = self._detect_compound_events(fused)

        # Critical signals
        criticals   = [s for s in fused.values() if s.is_critical]

        # Signal correlation matrix (simplified)
        correlations = self._detect_correlations(fused)

        # Overall data quality
        avg_conf    = statistics.mean(s.confidence for s in fused.values()) if fused else 0.0
        source_set  = set()
        for s in fused.values():
            source_set.update(s.sources)

        return IntelligencePacket(
            physical_score       = phys_score,
            satellite_score      = sat_score,
            transition_score     = tr_score,
            compound_events      = compounds,
            critical_signals     = [s.to_dict() for s in criticals],
            correlations         = correlations,
            overall_confidence   = avg_conf,
            active_sources       = list(source_set),
            signal_count         = len(fused),
            fused_signals        = {k: v.to_dict() for k, v in fused.items()},
        )

    def _category_score(self, signals: dict[str, FusedSignal]) -> float:
        if not signals:
            return 0.0
        registry_weights = {k: SIGNAL_REGISTRY.get(k, {}).get("weight", 0.05) for k in signals}
        total_w = sum(registry_weights.values())
        if total_w == 0:
            return statistics.mean(s.value for s in signals.values())
        return min(1.0, sum(
            signals[k].value * registry_weights[k] / total_w
            for k in signals
        ))

    def _detect_compound_events(self, fused: dict[str, FusedSignal]) -> list[dict]:
        """
        Compound events are where multiple hazards amplify each other.
        Examples:
          fire + drought + heat  → compound fire-climate event (3× damage multiplier)
          flood + wind           → storm surge / cyclone signature
          drought + heat         → heat-drought nexus (agricultural collapse)
        """
        events = []

        drought  = fused.get("drought_index",       FusedSignal("",0,0,[],0,0,"",False)).value
        fire     = fused.get("fire_prob_100km",     FusedSignal("",0,0,[],0,0,"",False)).value
        heat     = fused.get("heat_stress_prob_7d", FusedSignal("",0,0,[],0,0,"",False)).value
        flood    = fused.get("flood_signal",        FusedSignal("",0,0,[],0,0,"",False)).value
        veg      = fused.get("vegetation_stress",   FusedSignal("",0,0,[],0,0,"",False)).value
        wind     = fused.get("extreme_wind_prob_7d",FusedSignal("",0,0,[],0,0,"",False)).value

        # Fire-climate compound event
        if fire >= 0.30 and drought >= 0.35 and heat >= 0.30:
            events.append({
                "type":        "fire_climate_compound",
                "severity":    "CRITICAL",
                "description": "Co-occurring fire risk, drought, and heat stress. "
                               "Compound damage multiplier: 2.5–3.5×.",
                "signals":     {"fire": round(fire,3), "drought": round(drought,3), "heat": round(heat,3)},
                "amplifier":   round(1.0 + fire * 0.8 + drought * 0.7 + heat * 0.5, 3),
            })

        # Drought-heat nexus
        elif drought >= 0.50 and heat >= 0.40:
            events.append({
                "type":        "drought_heat_nexus",
                "severity":    "HIGH",
                "description": "Simultaneous drought and heat stress. "
                               "Critical for agriculture and water-dependent infrastructure.",
                "signals":     {"drought": round(drought,3), "heat": round(heat,3)},
                "amplifier":   round(1.0 + drought * 0.6 + heat * 0.5, 3),
            })

        # Flood-wind compound
        if flood >= 0.35 and wind >= 0.35:
            events.append({
                "type":        "flood_wind_compound",
                "severity":    "HIGH",
                "description": "Co-occurring flood signal and extreme wind. "
                               "Storm surge / cyclone signature.",
                "signals":     {"flood": round(flood,3), "wind": round(wind,3)},
                "amplifier":   round(1.0 + flood * 0.7 + wind * 0.5, 3),
            })

        # Vegetation collapse (satellite signal)
        if veg >= 0.55 and drought >= 0.40:
            events.append({
                "type":        "vegetation_collapse",
                "severity":    "ELEVATED",
                "description": "Satellite-confirmed vegetation stress coincides with drought. "
                               "Land degradation / desertification signature.",
                "signals":     {"vegetation_stress": round(veg,3), "drought": round(drought,3)},
                "amplifier":   round(1.0 + veg * 0.5 + drought * 0.4, 3),
            })

        return events

    def _detect_correlations(self, fused: dict[str, FusedSignal]) -> list[dict]:
        """Simple correlation detection between signal pairs."""
        correlations = []
        keys = list(fused.keys())

        KNOWN_CORRELATIONS = [
            ("drought_index",       "fire_prob_100km",    "drought amplifies fire risk"),
            ("temp_anomaly_c",      "heat_stress_prob_7d","heat anomaly drives stress events"),
            ("vegetation_stress",   "drought_index",      "vegetation stress confirms drought"),
            ("flood_signal",        "precip_anomaly_pct", "satellite flood confirms precip excess"),
            ("co2_intensity_norm",  "carbon_policy_risk", "emissions intensity drives policy exposure"),
        ]

        for a, b, description in KNOWN_CORRELATIONS:
            if a in fused and b in fused:
                av = fused[a].value
                bv = fused[b].value
                # Both elevated = confirmed correlation
                if av >= 0.30 and bv >= 0.30:
                    correlations.append({
                        "signal_a":    a,
                        "signal_b":    b,
                        "values":      [round(av, 3), round(bv, 3)],
                        "description": description,
                        "strength":    round(min(1.0, av * bv * 3), 3),
                    })

        return correlations


# ════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE PACKET
# The final output of the fusion pipeline
# ════════════════════════════════════════════════════════════════════════════

class IntelligencePacket:
    """Structured intelligence output from signal fusion."""

    def __init__(
        self,
        physical_score:     float,
        satellite_score:    float,
        transition_score:   float,
        compound_events:    list,
        critical_signals:   list,
        correlations:       list,
        overall_confidence: float,
        active_sources:     list,
        signal_count:       int,
        fused_signals:      dict,
    ):
        self.physical_score     = physical_score
        self.satellite_score    = satellite_score
        self.transition_score   = transition_score
        self.compound_events    = compound_events
        self.critical_signals   = critical_signals
        self.correlations       = correlations
        self.overall_confidence = overall_confidence
        self.active_sources     = active_sources
        self.signal_count       = signal_count
        self.fused_signals      = fused_signals

    @property
    def has_compound_event(self) -> bool:
        return len(self.compound_events) > 0

    @property
    def max_compound_amplifier(self) -> float:
        if not self.compound_events:
            return 1.0
        return max(e.get("amplifier", 1.0) for e in self.compound_events)

    def to_dict(self) -> dict:
        return {
            "scores": {
                "physical":   round(self.physical_score,   4),
                "satellite":  round(self.satellite_score,  4),
                "transition": round(self.transition_score, 4),
            },
            "compound_events":      self.compound_events,
            "critical_signals":     self.critical_signals,
            "correlations":         self.correlations,
            "compound_amplifier":   round(self.max_compound_amplifier, 3),
            "overall_confidence":   round(self.overall_confidence, 4),
            "active_sources":       self.active_sources,
            "signal_count":         self.signal_count,
            "fused_signals":        self.fused_signals,
        }
