// =============================================================================
// Meteorium Engine — engine.rs
// Prexus Intelligence · v2.0.0
//
// Parallel Monte Carlo runner. Each simulation path runs independently on
// a Rayon thread. Seeds are derived deterministically from a master seed +
// path index so results are reproducible while being thread-safe.
// =============================================================================

use rayon::prelude::*;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use serde::{Deserialize, Serialize};

use crate::distributions::{DistributionConfig, Sampler};
use crate::stats::{RiskStats, compute_stats};

/// Top-level simulation request — this is what Python sends as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationParams {
    /// Stochastic process configuration
    pub distribution: DistributionConfig,
    /// Number of Monte Carlo paths
    pub n_paths: usize,
    /// Number of time steps per path
    pub n_steps: usize,
    /// Master RNG seed for reproducibility (0 = random)
    pub seed: u64,
    /// Optional label for logging / audit
    pub label: Option<String>,
}

impl Default for SimulationParams {
    fn default() -> Self {
        SimulationParams {
            distribution: DistributionConfig::Normal { mean: 0.0, std_dev: 1.0 },
            n_paths: 10_000,
            n_steps: 252,
            seed: 42,
            label: None,
        }
    }
}

/// Run the full Monte Carlo simulation.
/// Returns a [RiskStats] aggregated across all paths.
pub fn run_simulation(params: &SimulationParams) -> Result<RiskStats, String> {
    // ── Validate ─────────────────────────────────────────────────────────────
    if params.n_paths == 0 {
        return Err("n_paths must be > 0".into());
    }
    if params.n_steps == 0 {
        return Err("n_steps must be > 0".into());
    }

    let master_seed = if params.seed == 0 {
        rand::random::<u64>()
    } else {
        params.seed
    };

    // ── Parallel path generation ──────────────────────────────────────────────
    // Each path gets a unique seed = master_seed XOR path_index so paths
    // are independent yet fully deterministic given master_seed.
    let paths_data: Vec<Vec<f64>> = (0..params.n_paths)
        .into_par_iter()
        .map(|path_idx| {
            let seed = master_seed ^ (path_idx as u64).wrapping_mul(0x9e3779b97f4a7c15);
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut sampler = Sampler::new(params.distribution.clone());
            sampler.reset();

            let mut path = Vec::with_capacity(params.n_steps);
            for _ in 0..params.n_steps {
                path.push(sampler.sample(&mut rng));
            }
            path
        })
        .collect();

    // ── Aggregate ─────────────────────────────────────────────────────────────
    Ok(compute_stats(&paths_data))
}

// ── Multi-variable batch run ──────────────────────────────────────────────────

/// Run multiple independent simulations in one call.
/// Useful for Meteorium's multi-factor risk scenarios
/// (e.g. temperature + precipitation + crop yield simultaneously).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSimulationParams {
    pub scenarios: Vec<SimulationParams>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchResult {
    pub label:  String,
    pub stats:  RiskStats,
}

pub fn run_batch(batch: &BatchSimulationParams) -> Vec<Result<BatchResult, String>> {
    batch.scenarios
        .par_iter()
        .enumerate()
        .map(|(i, params)| {
            let label = params.label
                .clone()
                .unwrap_or_else(|| format!("scenario_{}", i));
            run_simulation(params).map(|stats| BatchResult { label, stats })
        })
        .collect()
}
