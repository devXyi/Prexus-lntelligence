// =============================================================================
// Meteorium Engine — mc_asset.rs
// Prexus Intelligence · v2.0.0
//
// Asset-level Monte Carlo simulation and scenario stress testing.
// Called from intelligence.py via PyO3 as:
//   _rust.monte_carlo_asset(...)
//   _rust.stress_test_scenarios(...)
//
// Mirrors the Python fallback logic in intelligence.py exactly,
// but runs 10–40× faster via rayon parallel draws.
// =============================================================================

use rayon::prelude::*;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand_distr::{Normal, LogNormal, Distribution};

// ── Scenario registry ─────────────────────────────────────────────────────────
// (display_label, scenario_key) — mirrors Python SCENARIO_MULTIPLIERS
const STRESS_SCENARIOS: &[(&str, &str)] = &[
    ("Baseline",       "baseline"),
    ("Paris 1.5°C",    "paris"),
    ("SSP2-4.5",       "ssp245"),
    ("SSP3-7.0",       "ssp370"),
    ("SSP5-8.5",       "ssp585"),
    ("Policy Failure", "failed"),
];

/// Scenario loss multiplier — matches Python SCENARIO_MULTIPLIERS
pub fn scenario_multiplier(scenario: &str) -> f64 {
    match scenario.to_lowercase().as_str() {
        "ssp119" | "paris"  => 1.40,
        "ssp245"            => 1.20,
        "baseline"          => 1.00,
        "ssp370"            => 0.85,
        "ssp585" | "failed" => 0.70,
        _                   => 1.00,
    }
}

/// Asset vulnerability coefficient — matches Python ASSET_VULNERABILITY
pub fn asset_vulnerability(asset_type: &str) -> f64 {
    match asset_type.to_lowercase().as_str() {
        "agriculture"    => 1.30,
        "coastal"        => 1.20,
        "infrastructure" => 1.00,
        "real_estate"    => 0.90,
        "technology"     => 0.70,
        _                => 1.00,
    }
}

// ── Asset Monte Carlo ─────────────────────────────────────────────────────────

/// Returns (composite_risk, var95, cvar95, mean_loss_mm, confidence)
///
/// var95 and cvar95 are normalised to [0, 1] as fractions of asset_value_mm
/// so they can be stored directly in the risk result.
pub fn run_asset_mc(
    physical_risk:   f64,
    transition_risk: f64,
    asset_value_mm:  f64,
    scenario:        &str,
    asset_type:      &str,
    horizon_days:    i64,
    n_draws:         usize,
    seed:            u64,
) -> (f64, f64, f64, f64, f64) {
    let s_mult      = scenario_multiplier(scenario);
    let vuln        = asset_vulnerability(asset_type);
    let horizon_amp = (1.0_f64 + (horizon_days as f64 / 365.0) * 0.15).min(1.30);
    let master_seed = if seed == 0 { rand::random::<u64>() } else { seed };

    // ── Parallel draw ─────────────────────────────────────────────────────────
    let mut losses: Vec<f64> = (0..n_draws)
        .into_par_iter()
        .map(|i| {
            let seed_i  = master_seed ^ (i as u64).wrapping_mul(0x9e3779b97f4a7c15);
            let mut rng = SmallRng::seed_from_u64(seed_i);

            // Physical and transition perturbation
            let p = Normal::new(physical_risk,   0.12)
                .unwrap()
                .sample(&mut rng)
                .clamp(0.0, 1.0);
            let t = Normal::new(transition_risk, 0.10)
                .unwrap()
                .sample(&mut rng)
                .clamp(0.0, 1.0);

            // Composite draw with scenario + horizon + vulnerability
            let composite = (p * 0.60 + t * 0.40) * s_mult * horizon_amp * vuln;

            // Loss severity — log-normal tail matching empirical asset loss distributions
            let severity  = LogNormal::new(-1.60_f64, 0.65_f64)
                .unwrap()
                .sample(&mut rng);

            (composite * severity * asset_value_mm).min(asset_value_mm * 0.95_f64)
        })
        .collect();

    losses.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mean_loss = losses.iter().sum::<f64>() / n_draws as f64;
    let idx95     = ((n_draws as f64 * 0.95) as usize).min(n_draws - 1);
    let var95_mm  = losses[idx95];
    let cvar95_mm = {
        let tail = &losses[idx95..];
        if tail.is_empty() { var95_mm } else { tail.iter().sum::<f64>() / tail.len() as f64 }
    };

    // Deterministic composite risk (point estimate, not sampled)
    let composite_risk = ((physical_risk * 0.60 + transition_risk * 0.40) * s_mult * vuln)
        .clamp(0.0, 1.0);

    // Confidence: inverse of coefficient of variation — tight dist = high confidence
    let variance   = losses.iter()
        .map(|l| (l - mean_loss).powi(2))
        .sum::<f64>()
        / n_draws as f64;
    let cv         = if mean_loss > 1e-9 { variance.sqrt() / mean_loss } else { 1.0 };
    let confidence = (1.0 - cv * 0.25).clamp(0.60, 0.97);

    (
        composite_risk,
        var95_mm  / asset_value_mm.max(1e-9),
        cvar95_mm / asset_value_mm.max(1e-9),
        mean_loss,
        confidence,
    )
}

// ── Stress test ───────────────────────────────────────────────────────────────

/// Run all 6 standard scenarios. Returns Vec<(label, composite_risk, var95, mean_loss_mm)>
/// var95 is normalised as a fraction of asset_value_mm.
pub fn run_stress_scenarios(
    physical_risk:   f64,
    transition_risk: f64,
    asset_value_mm:  f64,
    asset_type:      &str,
    n_draws:         usize,
) -> Vec<(String, f64, f64, f64)> {
    STRESS_SCENARIOS
        .par_iter()
        .map(|(label, scenario_key)| {
            let (cr, var95, _cvar, loss, _conf) = run_asset_mc(
                physical_risk,
                transition_risk,
                asset_value_mm,
                scenario_key,
                asset_type,
                365,        // standard 1-year horizon for stress test
                n_draws,
                42,
            );
            (label.to_string(), cr, var95, loss)
        })
        .collect()
}
