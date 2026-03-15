"""
core/models.py
Meteorium Engine — Shared Data Models
Dataclasses and enums used across all 7 layers.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, List, Any


# ─── Enums ────────────────────────────────────────────────────────────────────

class SourceStatus(str, Enum):
    NOMINAL  = "nominal"
    DEGRADED = "degraded"
    OFFLINE  = "offline"
    UNKNOWN  = "unknown"


class RiskLevel(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH     = "HIGH"
    ELEVATED = "ELEVATED"
    MODERATE = "MODERATE"
    LOW      = "LOW"


class Scenario(str, Enum):
    SSP119   = "ssp119"
    PARIS    = "paris"
    SSP245   = "ssp245"
    BASELINE = "baseline"
    SSP370   = "ssp370"
    SSP585   = "ssp585"
    FAILED   = "failed"


class RiskType(str, Enum):
    PHYSICAL   = "PHYSICAL"
    TRANSITION = "TRANSITION"
    COMPOSITE  = "COMPOSITE"


# ─── Layer 0: Source descriptor ───────────────────────────────────────────────

@dataclass
class DataSource:
    """Describes an external data source (Layer 0)."""
    id:                str
    name:              str
    agency:            str
    type:              str           # 'weather', 'fire', 'carbon', 'satellite'
    format:            str           # 'csv', 'netcdf', 'geotiff', 'json'
    update_freq_hours: float
    requires_key:      bool
    key_env_var:       Optional[str]
    base_url:          str
    coverage:          str           # 'global', 'regional'
    resolution_km:     float
    latency_hours:     float         # data lag after real-world event
    notes:             str = ""


# ─── Layer 1: Ingestion record ────────────────────────────────────────────────

@dataclass
class IngestRecord:
    """Single ingestion job result (Layer 1)."""
    source_id:    str
    file_path:    str
    bbox:         tuple[float, float, float, float]   # (minlon, minlat, maxlon, maxlat)
    time_start:   datetime
    time_end:     datetime
    file_size_mb: float
    file_hash:    str
    record_count: int
    ingested_at:  datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status:       str = "success"
    error:        Optional[str] = None


# ─── Layer 2: Lake manifest entry ────────────────────────────────────────────

@dataclass
class LakeEntry:
    """Metadata record in the lake manifest (Layer 2)."""
    lake_id:      str              # unique ID
    source_id:    str
    variable:     str
    file_path:    str
    bbox:         str              # JSON string
    time_start:   datetime
    time_end:     datetime
    resolution:   float            # km
    file_hash:    str
    file_size_mb: float
    deposited_at: datetime
    expires_at:   Optional[datetime] = None


# ─── Layer 3: Preprocessed tile ──────────────────────────────────────────────

@dataclass
class ProcessedTile:
    """Geospatial feature tile after preprocessing (Layer 3)."""
    h3_index:    int               # H3 cell index
    variable:    str
    value:       float
    unit:        str
    scenario:    str = "observed"
    horizon:     Optional[datetime] = None
    source:      str = ""
    confidence:  float = 1.0
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ─── Layer 4: Asset feature vector ───────────────────────────────────────────

@dataclass
class AssetFeatures:
    """Complete feature vector for a single asset (Layer 4)."""
    asset_id:     str
    h3_index:     int
    lat:          float
    lon:          float
    country_code: str

    # Physical hazard features
    temp_anomaly_c:       float = 0.0
    precip_anomaly_pct:   float = 0.0
    heat_stress_prob:     float = 0.0
    drought_index:        float = 0.0
    extreme_wind_prob:    float = 0.0
    fire_prob_25km:       float = 0.0
    fire_prob_100km:      float = 0.0
    fire_hazard_score:    float = 0.0
    flood_susceptibility: float = 0.0
    soil_moisture:        float = 0.25
    wind_speed_ms:        float = 5.0

    # Transition risk features
    co2_intensity_norm:    float = 0.5
    transition_risk_score: float = 0.4
    carbon_policy_risk:    float = 0.4
    emissions_yoy_pct:     float = 0.0

    # Scenario projections
    temp_delta_2050_c:         float = 0.0
    extreme_heat_days_2050:    float = 0.0
    precip_change_2050_pct:    float = 0.0

    # Source metadata
    sources:      Dict[str, str] = field(default_factory=dict)
    computed_at:  datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    confidence:   float = 0.5

    @property
    def freshness_score(self) -> float:
        delta_h = (datetime.now(timezone.utc) - self.computed_at).total_seconds() / 3600
        return max(0.0, 1.0 - delta_h / 24.0)


# ─── Layer 5: Risk results ────────────────────────────────────────────────────

@dataclass
class AssetRiskResult:
    """Complete risk assessment for a single asset (Layer 5)."""
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
    sources:          Dict[str, str] = field(default_factory=dict)
    feature_snapshot: Dict[str, float] = field(default_factory=dict)
    stress_scenarios: List[Dict] = field(default_factory=list)
    alerts:           List['RiskAlert'] = field(default_factory=list)
    engine:           str = "python_fallback"
    computed_at:      str = ""

    @property
    def risk_level(self) -> RiskLevel:
        if self.composite_risk >= 0.85: return RiskLevel.CRITICAL
        if self.composite_risk >= 0.65: return RiskLevel.HIGH
        if self.composite_risk >= 0.45: return RiskLevel.ELEVATED
        if self.composite_risk >= 0.25: return RiskLevel.MODERATE
        return RiskLevel.LOW

    def to_dict(self) -> dict:
        return {
            "asset_id":         self.asset_id,
            "composite_risk":   round(self.composite_risk,   4),
            "physical_risk":    round(self.physical_risk,    4),
            "transition_risk":  round(self.transition_risk,  4),
            "risk_level":       self.risk_level.value,
            "var_95":           round(self.var_95,           4),
            "cvar_95":          round(self.cvar_95,          4),
            "loss_expected_mm": round(self.loss_expected_mm, 2),
            "confidence":       round(self.confidence,       4),
            "scenario":         self.scenario,
            "horizon_days":     self.horizon_days,
            "sources":          self.sources,
            "feature_snapshot": {k: round(v, 4) for k, v in self.feature_snapshot.items()},
            "stress_scenarios": self.stress_scenarios,
            "alerts":           [a.to_dict() for a in self.alerts],
            "engine":           self.engine,
            "computed_at":      self.computed_at or datetime.now(timezone.utc).isoformat(),
        }


@dataclass
class PortfolioRiskResult:
    """Correlated portfolio risk result (Layer 5)."""
    portfolio_composite_risk: float
    portfolio_var_95:         float
    portfolio_cvar_95:        float
    loss_expected_mm:         float
    diversification_ratio:    float
    asset_count:              int
    total_value_mm:           float
    scenario:                 str
    asset_breakdown:          List[dict] = field(default_factory=list)
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
            "asset_breakdown":          self.asset_breakdown,
            "computed_at":              self.computed_at or datetime.now(timezone.utc).isoformat(),
        }


@dataclass
class RiskAlert:
    """Generated risk alert for an asset."""
    alert_id:  str
    asset_id:  str
    severity:  str       # CRITICAL | HIGH | ELEVATED
    risk_type: str       # PHYSICAL | TRANSITION
    message:   str
    score:     float
    source:    str
    timestamp: str

    def to_dict(self) -> dict:
        return {
            "alert_id":  self.alert_id,
            "asset_id":  self.asset_id,
            "severity":  self.severity,
            "risk_type": self.risk_type,
            "message":   self.message,
            "score":     self.score,
            "source":    self.source,
            "timestamp": self.timestamp,
        }


# ─── Telemetry record (adapters output) ──────────────────────────────────────

@dataclass
class TelemetryRecord:
    """Single environmental measurement from any data source."""
    source:          str
    variable:        str
    lat:             float
    lon:             float
    value:           float
    unit:            str
    timestamp:       datetime
    confidence:      float
    freshness_hours: float
    metadata:        Dict[str, Any] = field(default_factory=dict)

    @property
    def is_fresh(self) -> bool:
        return self.freshness_hours < 24.0
