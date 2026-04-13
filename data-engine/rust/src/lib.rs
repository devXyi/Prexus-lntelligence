// =============================================================================
// Meteorium Engine — lib.rs
// Prexus Intelligence · v2.0.0
//
// PyO3 module. Four Python-callable functions:
//
//   meteorium_engine.simulate(json_str)             → json_str
//   meteorium_engine.simulate_batch(json_str)       → json_str
//   meteorium_engine.monte_carlo_asset(...)         → tuple(5)
//   meteorium_engine.stress_test_scenarios(...)     → list[tuple(4)]
// =============================================================================

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;

mod distributions;
mod engine;
mod mc_asset;
mod stats;

use engine::{SimulationParams, BatchSimulationParams, run_simulation, run_batch};
use mc_asset::{run_asset_mc, run_stress_scenarios};

// ── Single simulation ─────────────────────────────────────────────────────────

/// Run one Monte Carlo simulation.
///
/// Parameters
/// ----------
/// json_params : str
///     JSON-encoded SimulationParams. See README for schema.
///
/// Returns
/// -------
/// str
///     JSON-encoded RiskStats bundle.
///
/// Example (Python)
/// ----------------
/// ```python
/// import meteorium_engine, json
///
/// params = {
///     "distribution": {"type": "gbm", "mu": 0.07, "sigma": 0.15, "s0": 100.0, "dt": 0.003968},
///     "n_paths": 50000,
///     "n_steps": 252,
///     "seed": 42,
///     "label": "carbon_price"
/// }
/// result = json.loads(meteorium_engine.simulate(json.dumps(params)))
/// print(result["var_95"], result["cvar_99"])
/// ```
#[pyfunction]
fn simulate(json_params: &str) -> PyResult<String> {
    let params: SimulationParams = serde_json::from_str(json_params)
        .map_err(|e| PyValueError::new_err(format!("Invalid params JSON: {}", e)))?;

    let stats = run_simulation(&params)
        .map_err(|e| PyValueError::new_err(e))?;

    serde_json::to_string(&stats)
        .map_err(|e| PyValueError::new_err(format!("Serialisation error: {}", e)))
}

// ── Batch simulation ──────────────────────────────────────────────────────────

/// Run multiple independent simulations in one native call.
///
/// Parameters
/// ----------
/// json_params : str
///     JSON-encoded BatchSimulationParams: {"scenarios": [...]}
///
/// Returns
/// -------
/// str
///     JSON array of {"label": str, "stats": RiskStats} objects.
///     Failed scenarios carry {"label": str, "error": str}.
///
/// Example (Python)
/// ----------------
/// ```python
/// batch = {
///     "scenarios": [
///         {"distribution": {"type": "normal", "mean": 0.0, "std_dev": 1.0},
///          "n_paths": 20000, "n_steps": 90, "seed": 1, "label": "temp_anomaly"},
///         {"distribution": {"type": "lognormal", "mu": 0.5, "sigma": 0.3},
///          "n_paths": 20000, "n_steps": 90, "seed": 2, "label": "precipitation"},
///     ]
/// }
/// results = json.loads(meteorium_engine.simulate_batch(json.dumps(batch)))
/// ```
#[pyfunction]
fn simulate_batch(json_params: &str) -> PyResult<String> {
    let batch: BatchSimulationParams = serde_json::from_str(json_params)
        .map_err(|e| PyValueError::new_err(format!("Invalid batch JSON: {}", e)))?;

    let results = run_batch(&batch);

    // Serialise — failed scenarios become {label, error} objects
    let json_arr: Vec<serde_json::Value> = results
        .into_iter()
        .enumerate()
        .map(|(i, res)| match res {
            Ok(br) => serde_json::json!({
                "label": br.label,
                "stats": br.stats,
            }),
            Err(e) => serde_json::json!({
                "label": format!("scenario_{}", i),
                "error": e,
            }),
        })
        .collect();

    serde_json::to_string(&json_arr)
        .map_err(|e| PyValueError::new_err(format!("Serialisation error: {}", e)))
}

// ── Asset Monte Carlo API (called from intelligence.py) ───────────────────────

/// Run asset-level Monte Carlo with compound-amplified risk inputs.
///
/// Returns tuple: (composite_risk, var_95, cvar_95, expected_loss_mm, confidence)
/// var_95 and cvar_95 are normalised fractions of asset_value_mm.
#[pyfunction]
#[pyo3(signature = (
    physical_risk,
    transition_risk,
    asset_value_mm,
    scenario,
    asset_type,
    horizon_days,
    n_draws = 10000,
))]
fn monte_carlo_asset(
    physical_risk:   f64,
    transition_risk: f64,
    asset_value_mm:  f64,
    scenario:        &str,
    asset_type:      &str,
    horizon_days:    i64,
    n_draws:         usize,
) -> PyResult<(f64, f64, f64, f64, f64)> {
    if !(0.0..=1.0).contains(&physical_risk) {
        return Err(PyValueError::new_err("physical_risk must be in [0, 1]"));
    }
    if !(0.0..=1.0).contains(&transition_risk) {
        return Err(PyValueError::new_err("transition_risk must be in [0, 1]"));
    }
    if asset_value_mm <= 0.0 {
        return Err(PyValueError::new_err("asset_value_mm must be > 0"));
    }
    if n_draws == 0 {
        return Err(PyValueError::new_err("n_draws must be > 0"));
    }

    Ok(run_asset_mc(
        physical_risk, transition_risk, asset_value_mm,
        scenario, asset_type, horizon_days, n_draws,
        0,  // seed=0 → random per call for production variance
    ))
}

/// Run all 6 standard stress scenarios in parallel.
/// Returns list of (label, composite_risk, var_95, expected_loss_mm).
#[pyfunction]
#[pyo3(signature = (
    physical_risk,
    transition_risk,
    asset_value_mm,
    asset_type,
    n_draws = 5000,
))]
fn stress_test_scenarios(
    physical_risk:   f64,
    transition_risk: f64,
    asset_value_mm:  f64,
    asset_type:      &str,
    n_draws:         usize,
) -> PyResult<Vec<(String, f64, f64, f64)>> {
    Ok(run_stress_scenarios(
        physical_risk, transition_risk, asset_value_mm, asset_type, n_draws,
    ))
}

// ── Module registration ───────────────────────────────────────────────────────

/// Meteorium Monte Carlo Risk Engine — Prexus Intelligence · v2.0.0
#[pymodule]
fn meteorium_engine(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(simulate, m)?)?;
    m.add_function(wrap_pyfunction!(simulate_batch, m)?)?;
    m.add_function(wrap_pyfunction!(monte_carlo_asset, m)?)?;
    m.add_function(wrap_pyfunction!(stress_test_scenarios, m)?)?;
    m.add("__version__", "2.0.0")?;
    m.add("__author__", "Prexus Intelligence")?;
    Ok(())
}
