// =============================================================================
// Meteorium Engine — stats.rs
// Prexus Intelligence · v2.0.0
//
// Risk statistics computed over completed simulation paths.
// All functions operate on sorted or unsorted f64 slices.
// =============================================================================

use serde::{Deserialize, Serialize};

/// Full risk statistics bundle returned per simulation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskStats {
    /// Number of simulation paths
    pub paths: usize,
    /// Number of time steps per path
    pub steps: usize,

    // ── Terminal value statistics (final step across all paths) ──────────────
    pub terminal_mean:   f64,
    pub terminal_std:    f64,
    pub terminal_min:    f64,
    pub terminal_max:    f64,

    // ── Percentile bands of terminal distribution ────────────────────────────
    pub p1:  f64,
    pub p5:  f64,
    pub p10: f64,
    pub p25: f64,
    pub p50: f64,   // median
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,

    // ── Tail-risk measures on terminal values ────────────────────────────────
    /// Value-at-Risk at confidence level α (loss not exceeded with prob α)
    pub var_95: f64,
    pub var_99: f64,
    /// Conditional Value-at-Risk (Expected Shortfall) — mean of tail losses
    pub cvar_95: f64,
    pub cvar_99: f64,

    // ── Path-level envelope (mean trajectory across time steps) ─────────────
    /// Mean value at each time step — length == steps
    pub mean_path:   Vec<f64>,
    /// Lower 5th percentile envelope — length == steps
    pub lower_5_path: Vec<f64>,
    /// Upper 95th percentile envelope — length == steps
    pub upper_95_path: Vec<f64>,
}

/// Compute the full RiskStats from a 2D matrix of shape [paths × steps].
/// `paths_data[i][t]` = value of path i at time step t.
pub fn compute_stats(paths_data: &[Vec<f64>]) -> RiskStats {
    let n_paths = paths_data.len();
    assert!(n_paths > 0, "No simulation paths");
    let n_steps = paths_data[0].len();
    assert!(n_steps > 0, "No time steps");

    // ── Terminal values ──────────────────────────────────────────────────────
    let mut terminals: Vec<f64> = paths_data
        .iter()
        .map(|p| *p.last().unwrap())
        .collect();
    terminals.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let terminal_mean = mean(&terminals);
    let terminal_std  = std_dev(&terminals, terminal_mean);
    let terminal_min  = terminals[0];
    let terminal_max  = terminals[n_paths - 1];

    // ── Percentiles ──────────────────────────────────────────────────────────
    let p1  = percentile(&terminals, 1.0);
    let p5  = percentile(&terminals, 5.0);
    let p10 = percentile(&terminals, 10.0);
    let p25 = percentile(&terminals, 25.0);
    let p50 = percentile(&terminals, 50.0);
    let p75 = percentile(&terminals, 75.0);
    let p90 = percentile(&terminals, 90.0);
    let p95 = percentile(&terminals, 95.0);
    let p99 = percentile(&terminals, 99.0);

    // ── VaR & CVaR ──────────────────────────────────────────────────────────
    // We treat losses as (terminal_mean - value) so positive = loss.
    // VaR_α = loss not exceeded at α confidence.
    let losses: Vec<f64> = terminals.iter().map(|v| terminal_mean - v).collect();
    // losses sorted ascending → worst losses at the end
    let mut sorted_losses = losses.clone();
    sorted_losses.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let var_95  = percentile(&sorted_losses, 95.0);
    let var_99  = percentile(&sorted_losses, 99.0);
    let cvar_95 = cvar(&sorted_losses, 95.0);
    let cvar_99 = cvar(&sorted_losses, 99.0);

    // ── Time-step envelope ───────────────────────────────────────────────────
    let mut mean_path       = Vec::with_capacity(n_steps);
    let mut lower_5_path    = Vec::with_capacity(n_steps);
    let mut upper_95_path   = Vec::with_capacity(n_steps);

    for t in 0..n_steps {
        let mut step_vals: Vec<f64> = paths_data.iter().map(|p| p[t]).collect();
        step_vals.sort_by(|a, b| a.partial_cmp(b).unwrap());

        mean_path.push(mean(&step_vals));
        lower_5_path.push(percentile(&step_vals, 5.0));
        upper_95_path.push(percentile(&step_vals, 95.0));
    }

    RiskStats {
        paths: n_paths,
        steps: n_steps,
        terminal_mean,
        terminal_std,
        terminal_min,
        terminal_max,
        p1, p5, p10, p25, p50, p75, p90, p95, p99,
        var_95, var_99,
        cvar_95, cvar_99,
        mean_path,
        lower_5_path,
        upper_95_path,
    }
}

// ── Internal helpers ─────────────────────────────────────────────────────────

fn mean(sorted: &[f64]) -> f64 {
    sorted.iter().sum::<f64>() / sorted.len() as f64
}

fn std_dev(sorted: &[f64], mean: f64) -> f64 {
    let variance = sorted.iter()
        .map(|x| (x - mean).powi(2))
        .sum::<f64>()
        / sorted.len() as f64;
    variance.sqrt()
}

/// Linear-interpolation percentile on a *sorted* slice.
fn percentile(sorted: &[f64], pct: f64) -> f64 {
    let n = sorted.len();
    if n == 1 {
        return sorted[0];
    }
    let rank = pct / 100.0 * (n - 1) as f64;
    let lo   = rank.floor() as usize;
    let hi   = rank.ceil()  as usize;
    let frac = rank - lo as f64;
    sorted[lo] + frac * (sorted[hi] - sorted[lo])
}

/// Conditional VaR (Expected Shortfall) on a *sorted* loss slice.
fn cvar(sorted_losses: &[f64], confidence: f64) -> f64 {
    let n        = sorted_losses.len();
    let cutoff_i = ((confidence / 100.0) * n as f64).floor() as usize;
    let tail     = &sorted_losses[cutoff_i..];
    if tail.is_empty() {
        return *sorted_losses.last().unwrap();
    }
    tail.iter().sum::<f64>() / tail.len() as f64
}
