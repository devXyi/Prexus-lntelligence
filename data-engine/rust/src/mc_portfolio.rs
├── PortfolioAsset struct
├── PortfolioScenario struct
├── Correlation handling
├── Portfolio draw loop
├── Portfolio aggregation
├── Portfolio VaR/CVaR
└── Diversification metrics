// =============================================================================
// Meteorium Engine — mc_portfolio.rs
// Prexus Intelligence · v2.1.0
//
// Portfolio-level Monte Carlo simulation with correlated asset draws,
// scenario stress testing, and diversification analytics.
//
// Called from intelligence.py via PyO3 as:
//   _rust.monte_carlo_portfolio(...)
//   _rust.stress_test_portfolio(...)
//
// Architecture — four composable internal kernels:
//   1. generate_correlated_draws()  iid normals → correlated shocks
//   2. compute_asset_loss()         shock + severity → capped standalone loss
//   3. simulate_draw()              one full draw across all assets
//   4. aggregate_draws()            loss matrix → VaR / CVaR / marginal ES
//
// Weighting semantics (v2.1 clarification):
//   PortfolioAsset.value_mm  = standalone notional exposure (USD mm)
//   PortfolioAsset.weight    = portfolio allocation fraction ∈ (0, 1]
//   compute_asset_loss()     operates on value_mm ONLY → standalone loss
//   simulate_draw()          applies weight at aggregation → no double-counting
//
// Copula roadmap:
//   v2.1 — Gaussian copula (production)
//   v2.2 — Student-t copula (fat-tail systemic shocks)     ← hooks present
//   v3.x — Dynamic ρ(t) via stochastic correlation regime  ← hooks present
// =============================================================================

use rayon::prelude::*;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand_distr::{Normal, LogNormal, Distribution};

use crate::mc_asset::{scenario_multiplier, asset_vulnerability};

// ── Scenario registry ─────────────────────────────────────────────────────────
const PORTFOLIO_STRESS_SCENARIOS: &[(&str, &str)] = &[
    ("Baseline",       "baseline"),
    ("Paris 1.5°C",    "paris"),
    ("SSP2-4.5",       "ssp245"),
    ("SSP3-7.0",       "ssp370"),
    ("SSP5-8.5",       "ssp585"),
    ("Policy Failure", "failed"),
];

// ── Copula type ───────────────────────────────────────────────────────────────

/// Copula family governing tail behaviour of correlated asset shocks.
///
/// `Gaussian` — standard correlated-normal draws. Tail events (3σ+) are
/// structurally underweighted for systemic climate scenarios.
///
/// `StudentT { df }` — Student-t copula. Shared chi-squared factor across
/// all assets in each draw produces simultaneous tail events, replicating
/// contagion clustering observed in sovereign default chains and systemic
/// infrastructure failure. Recommended df = 4–6 for climate-financial risk.
///
/// Copula only controls the dependence structure (how assets co-move).
/// Marginal severity is always log-normal regardless of copula choice.
#[derive(Clone, Debug)]
pub enum CopulaType {
    Gaussian,
    StudentT { df: u32 },
}

impl Default for CopulaType {
    fn default() -> Self { CopulaType::Gaussian }
}

// ── Correlation regime ────────────────────────────────────────────────────────

/// Correlation structure for the portfolio simulation.
///
/// v2.1: `base` is used for all draws. `stressed` and `threshold_pct` are
/// present as first-class hooks for v3.x dynamic correlation:
///
///   ρ(t) = ρ_base + (ρ_stress − ρ_base) · I[port_loss > threshold]
///
/// This models correlation breakdown / contagion clustering — the empirical
/// observation that asset correlations surge during systemic crises.
/// BlackRock Aladdin, MSCI BarraOne, and similar institutional systems
/// apply regime-conditional correlation in exactly this way.
#[derive(Clone, Debug)]
pub struct CorrelationRegime {
    /// Baseline correlation matrix — n×n row-major, used in all current draws.
    pub base:            Vec<f64>,
    /// Stressed correlation matrix — applied when portfolio loss exceeds
    /// `threshold_pct` of total value. `None` disables stress switching.
    /// Hook for v3.x: populate via calibrated empirical stress periods.
    pub stressed:        Option<Vec<f64>>,
    /// Portfolio loss fraction (of total_value_mm) that triggers stressed ρ.
    pub threshold_pct:   f64,
    /// Pre-computed Cholesky factor of `base`.
    pub cholesky_base:   Vec<f64>,
    /// Pre-computed Cholesky factor of `stressed` (if present).
    pub cholesky_stress: Option<Vec<f64>>,
}

impl CorrelationRegime {
    pub fn new(base: Vec<f64>, n: usize) -> Result<Self, &'static str> {
        let l = cholesky(&base, n).ok_or("base correlation matrix is not positive-definite")?;
        Ok(Self {
            base,
            stressed:        None,
            threshold_pct:   0.20,
            cholesky_base:   l,
            cholesky_stress: None,
        })
    }

    pub fn with_stress(
        mut self,
        stressed:      Vec<f64>,
        n:             usize,
        threshold_pct: f64,
    ) -> Result<Self, &'static str> {
        let l = cholesky(&stressed, n)
            .ok_or("stressed correlation matrix is not positive-definite")?;
        self.stressed        = Some(stressed);
        self.cholesky_stress = Some(l);
        self.threshold_pct   = threshold_pct;
        Ok(self)
    }

    /// Select active Cholesky factor.
    /// v3.x hook: switch on `port_loss_prev / total_value > threshold_pct`.
    #[inline]
    pub fn active_cholesky(&self) -> &[f64] {
        &self.cholesky_base
    }
}

// ── Structs ───────────────────────────────────────────────────────────────────

/// Single asset within a portfolio simulation run.
///
/// Weighting semantics (v2.1):
///   `value_mm`  — standalone notional exposure (USD mm). Used in loss computation.
///   `weight`    — portfolio allocation fraction ∈ (0, 1]. Used at aggregation only.
///   These are INDEPENDENT. Do not pre-multiply value_mm by weight.
#[derive(Clone, Debug)]
pub struct PortfolioAsset {
    pub id:              String,
    pub asset_type:      String,
    pub physical_risk:   f64,
    pub transition_risk: f64,
    /// Standalone notional exposure (USD mm). NOT pre-weighted.
    pub value_mm:        f64,
    /// Portfolio allocation fraction ∈ (0, 1]. Engine normalises internally.
    pub weight:          f64,
}

/// Aggregated result for one scenario across the full portfolio.
#[derive(Clone, Debug)]
pub struct PortfolioScenarioResult {
    pub scenario_label:   String,
    pub scenario_key:     String,
    /// Weight-averaged composite risk ∈ [0, 1]
    pub composite_risk:   f64,
    /// Portfolio VaR at 95th percentile (USD mm, weighted portfolio basis)
    pub var95_mm:         f64,
    /// Portfolio CVaR / Expected Shortfall at 95th percentile (USD mm)
    pub cvar95_mm:        f64,
    /// Mean portfolio loss (USD mm)
    pub mean_loss_mm:     f64,
    /// VaR as fraction of total portfolio notional
    pub var95_pct:        f64,
    /// CVaR as fraction of total portfolio notional
    pub cvar95_pct:       f64,
    /// Component ES: each asset's expected weighted loss given portfolio > VaR95.
    /// Euler allocation — coherent, additive to 1.0.
    pub marginal_contrib: Vec<(String, f64)>,
    /// Simulation confidence ∈ [0.60, 0.97]
    pub confidence:       f64,
}

/// Full portfolio simulation output returned to Python / PyO3.
#[derive(Clone, Debug)]
pub struct PortfolioResult {
    pub scenarios:             Vec<PortfolioScenarioResult>,
    /// VaR reduction vs. naive sum of standalone VaRs — fraction ∈ [0, 1]
    pub diversification_ratio: f64,
    /// Herfindahl-Hirschman Index of weight concentration ∈ (0, 1]
    pub hhi:                   f64,
    /// Effective number of uncorrelated bets = 1 / HHI
    pub effective_n:           f64,
    /// Correlation matrix used (base regime, n×n row-major)
    pub corr_matrix:           Vec<f64>,
    /// Sum of all asset.value_mm (standalone notional, USD mm)
    pub total_notional_mm:     f64,
    /// Copula type used in this run
    pub copula:                CopulaType,
}

// ── Simulation config ─────────────────────────────────────────────────────────

/// Immutable parameters for one simulation run. Passed by reference through
/// the call chain so each kernel stays a pure function.
pub struct SimConfig {
    pub scenario_key:  String,
    pub horizon_days:  i64,
    pub n_draws:       usize,
    pub seed:          u64,
    pub copula:        CopulaType,
}

impl SimConfig {
    #[inline] pub fn horizon_amp(&self)    -> f64 {
        (1.0 + (self.horizon_days as f64 / 365.0) * 0.15).min(1.30)
    }
    #[inline] pub fn scenario_mult(&self)  -> f64 { scenario_multiplier(&self.scenario_key) }
    #[inline] pub fn effective_seed(&self) -> u64  {
        if self.seed == 0 { rand::random::<u64>() } else { self.seed }
    }
}

// =============================================================================
// KERNEL 1 — generate_correlated_draws
// Responsibility: iid normals → correlated shocks (Gaussian or Student-t)
// =============================================================================

/// Transform i.i.d. standard normals into correlated shocks via Cholesky.
///
/// Gaussian copula: `chi_factor = 1.0` → output is multivariate normal L·z.
///
/// Student-t copula: `chi_factor = sqrt(df / χ²_df)`. Dividing by √(χ²/df)
/// inflates all shocks simultaneously for one draw, producing fat-tailed
/// co-movement that characterises systemic climate crises. chi_factor is
/// computed in `simulate_draw` and passed here to keep this function pure.
///
/// v2.1 — Direct shock (no CDF compression):
///   Shocks are applied linearly to risk scores (clamped at [0,1]).
///   v2.0 used: z → erf → CDF → bounded perturbation, which compressed
///   extreme tail events. A 3σ systemic shock now produces a materially
///   larger risk exceedance than a 1σ shock, as required.
#[inline]
fn generate_correlated_draws(
    l:          &[f64],
    z_iid:      &[f64],
    n:          usize,
    chi_factor: f64,
) -> Vec<f64> {
    let mut shocks = vec![0.0_f64; n];
    for i in 0..n {
        let mut v = 0.0_f64;
        for j in 0..=i {
            v += l[i * n + j] * z_iid[j];
        }
        shocks[i] = v * chi_factor;
    }
    shocks
}

// =============================================================================
// KERNEL 2 — compute_asset_loss
// Responsibility: shock + severity → capped STANDALONE loss
// Pure function — no RNG, no weight applied.
// =============================================================================

/// Compute standalone loss for one asset in one draw.
///
/// Weighting: operates purely on `asset.value_mm`. Portfolio weights are
/// applied ONLY in `simulate_draw`. This prevents the v2.0 double-weighting
/// where `raw_loss.min(value_mm × weight × 0.95)` conflated two concepts.
///
/// The standalone cap is `value_mm × 0.95` — weight-independent.
///
/// # Arguments
/// * `shock`    — correlated shock from `generate_correlated_draws`
/// * `severity` — pre-sampled log-normal severity (drawn in `simulate_draw`)
#[inline]
fn compute_asset_loss(
    asset:         &PortfolioAsset,
    shock:         f64,
    severity:      f64,
    scenario_mult: f64,
    horizon_amp:   f64,
) -> f64 {
    let vuln = asset_vulnerability(&asset.asset_type);
    let p    = (asset.physical_risk   + 0.12 * shock).clamp(0.0, 1.0);
    let t    = (asset.transition_risk + 0.10 * shock).clamp(0.0, 1.0);
    let composite = (p * 0.60 + t * 0.40) * scenario_mult * horizon_amp * vuln;
    // Cap uses value_mm only — weight applied separately in simulate_draw
    (composite * severity * asset.value_mm).min(asset.value_mm * 0.95)
}

// =============================================================================
// KERNEL 3 — simulate_draw
// Responsibility: one full draw across all n assets
// Owns all RNG for this draw index. Returns (portfolio_loss, standalone_losses).
// =============================================================================

/// Execute one Monte Carlo draw across the entire portfolio.
///
/// Sequence:
///   1. Seed per-draw SmallRng deterministically from master_seed ^ draw_index.
///   2. Sample n i.i.d. N(0,1) for Cholesky transform.
///   3. If Student-t: sample df normals → chi_sq → chi_factor.
///   4. Call `generate_correlated_draws` → shocks.
///   5. Per-asset: sample log-normal severity → call `compute_asset_loss`.
///   6. Portfolio loss = Σ (standalone_loss_i × weight_i).
///
/// Returns `(weighted_portfolio_loss_mm, standalone_asset_losses_mm)`.
/// Weights are applied here once, at aggregation — not inside `compute_asset_loss`.
fn simulate_draw(
    assets:        &[PortfolioAsset],
    regime:        &CorrelationRegime,
    weights_n:     &[f64],
    scenario_mult: f64,
    horizon_amp:   f64,
    copula:        &CopulaType,
    seed_i:        u64,
) -> (f64, Vec<f64>) {
    let n       = assets.len();
    let mut rng = SmallRng::seed_from_u64(seed_i);
    let z_std   = Normal::new(0.0_f64, 1.0_f64).unwrap();

    // Step 1 — n i.i.d. standard normals
    let z_iid: Vec<f64> = (0..n).map(|_| z_std.sample(&mut rng)).collect();

    // Step 2 — chi_factor for copula
    let chi_factor: f64 = match copula {
        CopulaType::Gaussian => 1.0,
        CopulaType::StudentT { df } => {
            // χ²(df) = Σ_{k=1..df} Z_k²  where Z_k ~ N(0,1)
            // chi_factor = √(df / χ²) — shared across all assets.
            // Small χ² (tail of chi-squared) → chi_factor > 1 → all shocks
            // inflated simultaneously → contagion clustering.
            let chi_sq: f64 = (0..*df)
                .map(|_| { let z = z_std.sample(&mut rng); z * z })
                .sum::<f64>()
                .max(1e-9);
            (*df as f64 / chi_sq).sqrt()
        }
    };

    // Step 3 — correlated shocks
    let shocks = generate_correlated_draws(
        regime.active_cholesky(),
        &z_iid,
        n,
        chi_factor,
    );

    // Step 4 — per-asset severity + standalone loss
    let lognorm = LogNormal::new(-1.60_f64, 0.65_f64).unwrap();
    let standalone_losses: Vec<f64> = assets.iter().enumerate().map(|(i, asset)| {
        let severity = lognorm.sample(&mut rng);
        compute_asset_loss(asset, shocks[i], severity, scenario_mult, horizon_amp)
    }).collect();

    // Step 5 — portfolio loss: weight applied once here
    let portfolio_loss: f64 = standalone_losses.iter()
        .zip(weights_n.iter())
        .map(|(loss, w)| loss * w)
        .sum();

    (portfolio_loss, standalone_losses)
}

// =============================================================================
// KERNEL 4 — aggregate_draws
// Responsibility: loss matrix → VaR, CVaR, component ES, confidence
// Pure aggregation — no RNG, no correlation ops.
// =============================================================================

/// Aggregate all draws into a `PortfolioScenarioResult`.
///
/// Marginal contribution — Component Expected Shortfall (Euler allocation):
///   contrib_i = E[weighted_loss_i | portfolio_loss > VaR95]
///
/// Euler allocation is coherent and additive: contributions sum to 1.0.
/// It attributes draw-down to assets that are large during the worst portfolio
/// outcomes — correct for systemic contagion scenarios where co-movement matters.
///
/// # Arguments
/// * `draws` — (portfolio_loss, standalone_asset_losses) per draw
fn aggregate_draws(
    draws:          &[(f64, Vec<f64>)],
    assets:         &[PortfolioAsset],
    weights_n:      &[f64],
    total_notional: f64,
    scenario_key:   &str,
    scenario_label: &str,
    scenario_mult:  f64,
) -> PortfolioScenarioResult {
    let n_draws = draws.len();
    let n       = assets.len();

    // Sort by portfolio loss; keep original indices for tail ES computation
    let mut indexed: Vec<(usize, f64)> = draws.iter()
        .enumerate()
        .map(|(i, (pl, _))| (i, *pl))
        .collect();
    indexed.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    let sorted_losses: Vec<f64> = indexed.iter().map(|(_, l)| *l).collect();
    let mean_loss = sorted_losses.iter().sum::<f64>() / n_draws as f64;

    let idx95     = ((n_draws as f64 * 0.95) as usize).min(n_draws - 1);
    let var95_mm  = sorted_losses[idx95];
    let tail      = &indexed[idx95..];

    let cvar95_mm = if tail.is_empty() {
        var95_mm
    } else {
        tail.iter().map(|(_, l)| l).sum::<f64>() / tail.len() as f64
    };

    // Component ES — Euler allocation over tail draws
    let mut component_es = vec![0.0_f64; n];
    for (orig_idx, _) in tail {
        let (_, ref asset_losses) = draws[*orig_idx];
        for (i, loss) in asset_losses.iter().enumerate() {
            component_es[i] += loss * weights_n[i]; // weighted contribution
        }
    }
    let total_ces: f64 = component_es.iter().sum();
    let marginal_contrib: Vec<(String, f64)> = assets.iter().enumerate().map(|(i, a)| {
        let frac = if total_ces > 1e-9 { component_es[i] / total_ces } else { weights_n[i] };
        (a.id.clone(), frac)
    }).collect();

    // Deterministic composite risk (weight-averaged point estimate)
    let composite_risk: f64 = assets.iter().enumerate().fold(0.0, |acc, (i, a)| {
        let vuln = asset_vulnerability(&a.asset_type);
        let cr   = ((a.physical_risk * 0.60 + a.transition_risk * 0.40) * scenario_mult * vuln)
            .clamp(0.0, 1.0);
        acc + cr * weights_n[i]
    }).clamp(0.0, 1.0);

    // Confidence — inverse CV
    let variance = sorted_losses.iter()
        .map(|l| (l - mean_loss).powi(2))
        .sum::<f64>()
        / n_draws as f64;
    let cv         = if mean_loss > 1e-9 { variance.sqrt() / mean_loss } else { 1.0 };
    let confidence = (1.0 - cv * 0.25).clamp(0.60, 0.97);

    let safe_notional = total_notional.max(1e-9);

    PortfolioScenarioResult {
        scenario_label:  scenario_label.to_string(),
        scenario_key:    scenario_key.to_string(),
        composite_risk,
        var95_mm,
        cvar95_mm,
        mean_loss_mm:    mean_loss,
        var95_pct:       var95_mm  / safe_notional,
        cvar95_pct:      cvar95_mm / safe_notional,
        marginal_contrib,
        confidence,
    }
}

// ── Parallel draw orchestration ───────────────────────────────────────────────

/// Execute n_draws in parallel via Rayon, calling `simulate_draw` per path.
///
/// Parallelism is isolated here so:
///   - Kernels 1-4 are single-threaded → trivially SIMD-vectorisable.
///   - v4.x GPU acceleration replaces this function without touching kernels.
///   - Profiling isolates simulation cost from aggregation cost.
fn simulate_portfolio_losses(
    assets:    &[PortfolioAsset],
    regime:    &CorrelationRegime,
    cfg:       &SimConfig,
    weights_n: &[f64],
) -> Vec<(f64, Vec<f64>)> {
    let master_seed   = cfg.effective_seed();
    let scenario_mult = cfg.scenario_mult();
    let horizon_amp   = cfg.horizon_amp();

    (0..cfg.n_draws)
        .into_par_iter()
        .map(|draw_i| {
            let seed_i = master_seed ^ (draw_i as u64).wrapping_mul(0x9e3779b97f4a7c15);
            simulate_draw(assets, regime, weights_n, scenario_mult, horizon_amp, &cfg.copula, seed_i)
        })
        .collect()
}

// ── Correlation helpers ───────────────────────────────────────────────────────

/// Build default inter-asset correlation matrix.
/// Same-type: ρ = 0.65. Cross-type: ρ = 0.25 + shared climate-beta.
/// Ledoit-Wolf shrinkage α = 0.05 → guaranteed PD.
pub fn build_correlation_matrix(assets: &[PortfolioAsset]) -> Vec<f64> {
    let n   = assets.len();
    let mut corr = vec![0.0_f64; n * n];

    for i in 0..n {
        for j in 0..n {
            corr[i * n + j] = if i == j {
                1.0
            } else {
                let base    = if assets[i].asset_type == assets[j].asset_type { 0.65 } else { 0.25 };
                let climate = (assets[i].physical_risk * assets[j].physical_risk).sqrt() * 0.20;
                (base + climate).min(0.95)
            };
        }
    }

    // Ledoit-Wolf shrinkage — off-diagonals × (1 − α)
    for i in 0..n {
        for j in 0..n {
            if i != j { corr[i * n + j] *= 0.95; }
        }
    }

    corr
}

/// Merge caller-supplied partial correlation matrix with the default.
/// Invalid entries (NaN, |v| > 1) are silently replaced with default values.
pub fn merge_correlation_matrix(
    assets:    &[PortfolioAsset],
    user_corr: Option<&[f64]>,
) -> Vec<f64> {
    let mut base = build_correlation_matrix(assets);
    let n        = assets.len();

    if let Some(uc) = user_corr {
        if uc.len() == n * n {
            for i in 0..n {
                for j in 0..n {
                    let v = uc[i * n + j];
                    if i == j {
                        base[i * n + j] = 1.0;
                    } else if v.is_finite() && (-1.0..=1.0).contains(&v) {
                        base[i * n + j] = v;
                    }
                }
            }
        }
    }

    base
}

/// Cholesky-Banachiewicz decomposition — returns L such that L·Lᵀ = Σ.
/// Pivot clamped to ≥ 1e-10 (should never trigger after Ledoit-Wolf shrinkage).
pub fn cholesky(corr: &[f64], n: usize) -> Option<Vec<f64>> {
    let mut l = vec![0.0_f64; n * n];

    for i in 0..n {
        for j in 0..=i {
            let sum: f64 = (0..j).map(|k| l[i * n + k] * l[j * n + k]).sum();
            if i == j {
                let diag = corr[i * n + i] - sum;
                if diag < 1e-10 { return None; }
                l[i * n + j] = diag.sqrt();
            } else {
                let pivot = l[j * n + j];
                if pivot.abs() < 1e-14 { return None; }
                l[i * n + j] = (corr[i * n + j] - sum) / pivot;
            }
        }
    }

    Some(l)
}

// ── Diversification metrics ───────────────────────────────────────────────────

/// HHI over portfolio weights. ∈ (0, 1]. → 0: diversified. = 1: concentrated.
pub fn herfindahl(weights_n: &[f64]) -> f64 {
    weights_n.iter().map(|w| w * w).sum()
}

/// Diversification ratio: 1 − portfolio_VaR / Σ(standalone_VaR_i × weight_i).
pub fn diversification_ratio(
    assets:    &[PortfolioAsset],
    weights_n: &[f64],
    port_var:  f64,
    scenario:  &str,
) -> f64 {
    let s_mult    = scenario_multiplier(scenario);
    let h_amp     = 1.10_f64;
    let e_lognorm = (-1.60_f64 + 0.65_f64.powi(2) / 2.0).exp();

    let standalone_sum: f64 = assets.iter().enumerate().map(|(i, a)| {
        let vuln = asset_vulnerability(&a.asset_type);
        let cr   = (a.physical_risk * 0.60 + a.transition_risk * 0.40) * s_mult * h_amp * vuln;
        cr * e_lognorm * a.value_mm * weights_n[i] * 1.65
    }).sum();

    if standalone_sum < 1e-9 { return 0.0; }
    (1.0 - port_var / standalone_sum).clamp(0.0, 1.0)
}

// ── Weight normalisation ──────────────────────────────────────────────────────

fn normalise_weights(assets: &[PortfolioAsset]) -> Vec<f64> {
    let raw_sum: f64 = assets.iter().map(|a| a.weight).sum();
    if raw_sum > 1e-9 {
        assets.iter().map(|a| a.weight / raw_sum).collect()
    } else {
        let val_sum: f64 = assets.iter().map(|a| a.value_mm).sum::<f64>().max(1e-9);
        assets.iter().map(|a| a.value_mm / val_sum).collect()
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Run the full portfolio simulation across all 6 standard scenarios.
///
/// # Arguments
/// * `assets`       — `value_mm` = standalone notional, `weight` = allocation fraction.
/// * `user_corr`    — optional n×n flat correlation matrix (row-major).
/// * `copula`       — `Gaussian` for production; `StudentT { df: 4 }` for systemic tails.
/// * `horizon_days` — risk horizon.
/// * `n_draws`      — ≥ 10_000 recommended for stable 95th pct VaR.
/// * `seed`         — 0 → random (non-reproducible).
pub fn run_portfolio(
    assets:       &[PortfolioAsset],
    user_corr:    Option<&[f64]>,
    copula:       CopulaType,
    horizon_days: i64,
    n_draws:      usize,
    seed:         u64,
) -> PortfolioResult {
    assert!(!assets.is_empty(), "portfolio must contain at least one asset");

    let n              = assets.len();
    let total_notional = assets.iter().map(|a| a.value_mm).sum::<f64>();
    let weights_n      = normalise_weights(assets);

    let corr_matrix = merge_correlation_matrix(assets, user_corr);
    let regime      = CorrelationRegime::new(corr_matrix.clone(), n)
        .expect("correlation matrix is not positive-definite");

    let scenarios: Vec<PortfolioScenarioResult> = PORTFOLIO_STRESS_SCENARIOS
        .iter()
        .map(|(label, key)| {
            let cfg = SimConfig {
                scenario_key: key.to_string(),
                horizon_days,
                n_draws,
                seed,
                copula: copula.clone(),
            };
            let s_mult = cfg.scenario_mult();
            let draws  = simulate_portfolio_losses(assets, &regime, &cfg, &weights_n);
            aggregate_draws(&draws, assets, &weights_n, total_notional, key, label, s_mult)
        })
        .collect();

    let baseline_var = scenarios.iter()
        .find(|s| s.scenario_key == "baseline")
        .map(|s| s.var95_mm)
        .unwrap_or(0.0);

    let div_ratio   = diversification_ratio(assets, &weights_n, baseline_var, "baseline");
    let hhi         = herfindahl(&weights_n);
    let effective_n = if hhi > 1e-9 { 1.0 / hhi } else { n as f64 };

    PortfolioResult {
        scenarios,
        diversification_ratio: div_ratio,
        hhi,
        effective_n,
        corr_matrix,
        total_notional_mm: total_notional,
        copula,
    }
}

/// Single-scenario portfolio simulation — for interactive dashboard updates.
pub fn run_portfolio_scenario(
    assets:       &[PortfolioAsset],
    user_corr:    Option<&[f64]>,
    copula:       CopulaType,
    scenario_key: &str,
    horizon_days: i64,
    n_draws:      usize,
    seed:         u64,
) -> PortfolioScenarioResult {
    assert!(!assets.is_empty(), "portfolio must contain at least one asset");

    let n              = assets.len();
    let total_notional = assets.iter().map(|a| a.value_mm).sum::<f64>();
    let weights_n      = normalise_weights(assets);

    let corr_matrix = merge_correlation_matrix(assets, user_corr);
    let regime      = CorrelationRegime::new(corr_matrix, n)
        .expect("correlation matrix not positive-definite");

    let label = PORTFOLIO_STRESS_SCENARIOS
        .iter()
        .find(|(_, k)| *k == scenario_key)
        .map(|(l, _)| *l)
        .unwrap_or(scenario_key);

    let cfg = SimConfig {
        scenario_key: scenario_key.to_string(),
        horizon_days, n_draws, seed, copula,
    };
    let s_mult = cfg.scenario_mult();
    let draws  = simulate_portfolio_losses(assets, &regime, &cfg, &weights_n);

    aggregate_draws(&draws, assets, &weights_n, total_notional, scenario_key, label, s_mult)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_assets() -> Vec<PortfolioAsset> {
        vec![
            PortfolioAsset {
                id: "coastal_port".to_string(), asset_type: "coastal".to_string(),
                physical_risk: 0.72, transition_risk: 0.45, value_mm: 500.0, weight: 0.40,
            },
            PortfolioAsset {
                id: "agri_brazil".to_string(), asset_type: "agriculture".to_string(),
                physical_risk: 0.65, transition_risk: 0.30, value_mm: 300.0, weight: 0.30,
            },
            PortfolioAsset {
                id: "data_centre".to_string(), asset_type: "technology".to_string(),
                physical_risk: 0.20, transition_risk: 0.55, value_mm: 200.0, weight: 0.30,
            },
        ]
    }

    // ── Kernel 1 ─────────────────────────────────────────────────────────────

    #[test]
    fn k1_identity_cholesky_is_passthrough() {
        let n = 3;
        let l = vec![1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0_f64];
        let z = vec![0.5, -1.2, 0.8_f64];
        let o = generate_correlated_draws(&l, &z, n, 1.0);
        for i in 0..n { assert!((o[i] - z[i]).abs() < 1e-12); }
    }

    #[test]
    fn k1_student_t_chi_factor_amplifies_shocks() {
        let n  = 2;
        let l  = vec![1.0, 0.0, 0.0, 1.0_f64];
        let z  = vec![1.0, 1.0_f64];
        let g  = generate_correlated_draws(&l, &z, n, 1.0);
        let t  = generate_correlated_draws(&l, &z, n, 3.0);
        assert!(t[0] > g[0] + 1e-9);
    }

    // ── Kernel 2 ─────────────────────────────────────────────────────────────

    #[test]
    fn k2_cap_at_95pct_of_value_mm() {
        let a = PortfolioAsset {
            id: "t".to_string(), asset_type: "coastal".to_string(),
            physical_risk: 0.99, transition_risk: 0.99, value_mm: 100.0, weight: 1.0,
        };
        let loss = compute_asset_loss(&a, 0.0, 1000.0, 1.0, 1.0);
        assert!(loss <= 95.0 + 1e-9, "cap must be 95% of value_mm, got {loss}");
    }

    #[test]
    fn k2_zero_risk_yields_zero_loss() {
        let a = PortfolioAsset {
            id: "t".to_string(), asset_type: "technology".to_string(),
            physical_risk: 0.0, transition_risk: 0.0, value_mm: 200.0, weight: 0.5,
        };
        let loss = compute_asset_loss(&a, 0.0, 1.0, 1.0, 1.0);
        assert!(loss.abs() < 1e-9);
    }

    #[test]
    fn k2_weight_independent() {
        // Same value_mm, different weights → identical standalone loss
        let base = PortfolioAsset {
            id: "a".to_string(), asset_type: "coastal".to_string(),
            physical_risk: 0.5, transition_risk: 0.3, value_mm: 100.0, weight: 0.10,
        };
        let other = PortfolioAsset { weight: 0.90, ..base.clone() };
        let l1 = compute_asset_loss(&base,  0.5, 0.5, 1.0, 1.0);
        let l2 = compute_asset_loss(&other, 0.5, 0.5, 1.0, 1.0);
        assert!((l1 - l2).abs() < 1e-12,
            "compute_asset_loss must be independent of weight");
    }

    // ── Kernel 3 ─────────────────────────────────────────────────────────────

    #[test]
    fn k3_deterministic_with_same_seed() {
        let assets  = sample_assets();
        let weights = normalise_weights(&assets);
        let corr    = build_correlation_matrix(&assets);
        let regime  = CorrelationRegime::new(corr, assets.len()).unwrap();
        let r1 = simulate_draw(&assets, &regime, &weights, 1.0, 1.1, &CopulaType::Gaussian, 999);
        let r2 = simulate_draw(&assets, &regime, &weights, 1.0, 1.1, &CopulaType::Gaussian, 999);
        assert!((r1.0 - r2.0).abs() < 1e-12);
    }

    #[test]
    fn k3_portfolio_loss_equals_weighted_sum_of_standalone() {
        let assets  = sample_assets();
        let weights = normalise_weights(&assets);
        let corr    = build_correlation_matrix(&assets);
        let regime  = CorrelationRegime::new(corr, assets.len()).unwrap();
        let (port, standalone) =
            simulate_draw(&assets, &regime, &weights, 1.0, 1.0, &CopulaType::Gaussian, 77);
        let expected: f64 = standalone.iter().zip(weights.iter()).map(|(l, w)| l * w).sum();
        assert!((port - expected).abs() < 1e-9,
            "portfolio_loss must equal Σ(standalone × weight)");
    }

    #[test]
    fn k3_student_t_produces_fatter_tails_than_gaussian() {
        let assets  = sample_assets();
        let weights = normalise_weights(&assets);
        let corr    = build_correlation_matrix(&assets);
        let regime  = CorrelationRegime::new(corr, assets.len()).unwrap();
        let n       = 5_000usize;

        let mut g_losses: Vec<f64> = (0..n).map(|i| {
            simulate_draw(&assets, &regime, &weights, 1.0, 1.0,
                &CopulaType::Gaussian, i as u64).0
        }).collect();
        let mut t_losses: Vec<f64> = (0..n).map(|i| {
            simulate_draw(&assets, &regime, &weights, 1.0, 1.0,
                &CopulaType::StudentT { df: 4 }, i as u64).0
        }).collect();

        g_losses.sort_by(|a, b| a.partial_cmp(b).unwrap());
        t_losses.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let cut   = n * 99 / 100;
        let tail  = (n - cut).max(1) as f64;
        let top_g = g_losses[cut..].iter().sum::<f64>() / tail;
        let top_t = t_losses[cut..].iter().sum::<f64>() / tail;
        assert!(top_t > top_g, "Student-t df=4 must exceed Gaussian tail: t={top_t:.2} g={top_g:.2}");
    }

    // ── Kernel 4 ─────────────────────────────────────────────────────────────

    #[test]
    fn k4_cvar_dominates_var() {
        let assets  = sample_assets();
        let weights = normalise_weights(&assets);
        let corr    = build_correlation_matrix(&assets);
        let regime  = CorrelationRegime::new(corr, assets.len()).unwrap();
        let cfg     = SimConfig {
            scenario_key: "baseline".to_string(),
            horizon_days: 365, n_draws: 10_000, seed: 42, copula: CopulaType::Gaussian,
        };
        let draws  = simulate_portfolio_losses(&assets, &regime, &cfg, &weights);
        let result = aggregate_draws(&draws, &assets, &weights, 1000.0, "baseline", "Baseline", 1.0);
        assert!(result.cvar95_mm >= result.var95_mm - 1e-9);
    }

    #[test]
    fn k4_marginal_contrib_sums_to_one() {
        let assets  = sample_assets();
        let weights = normalise_weights(&assets);
        let corr    = build_correlation_matrix(&assets);
        let regime  = CorrelationRegime::new(corr, assets.len()).unwrap();
        let cfg     = SimConfig {
            scenario_key: "ssp585".to_string(),
            horizon_days: 365, n_draws: 5_000, seed: 7, copula: CopulaType::Gaussian,
        };
        let draws  = simulate_portfolio_losses(&assets, &regime, &cfg, &weights);
        let result = aggregate_draws(&draws, &assets, &weights, 1000.0, "ssp585", "SSP5-8.5", 0.70);
        let sum: f64 = result.marginal_contrib.iter().map(|(_, v)| v).sum();
        assert!((sum - 1.0).abs() < 1e-9, "component ES must sum to 1.0, got {sum:.10}");
    }

    // ── Integration ───────────────────────────────────────────────────────────

    #[test]
    fn integration_full_result_structure() {
        let assets = sample_assets();
        let result = run_portfolio(&assets, None, CopulaType::Gaussian, 365, 5_000, 42);
        assert_eq!(result.scenarios.len(), PORTFOLIO_STRESS_SCENARIOS.len());
        assert!(result.hhi > 0.0 && result.hhi <= 1.0);
        assert!(result.effective_n >= 1.0);
        assert!(result.diversification_ratio >= 0.0 && result.diversification_ratio <= 1.0);
        for s in &result.scenarios {
            assert!(s.cvar95_mm >= s.var95_mm - 1e-9);
            assert!(s.composite_risk >= 0.0 && s.composite_risk <= 1.0);
            let sum: f64 = s.marginal_contrib.iter().map(|(_, v)| v).sum();
            assert!((sum - 1.0).abs() < 1e-9);
        }
    }

    #[test]
    fn integration_single_scenario_matches_full_run() {
        let assets = sample_assets();
        let full   = run_portfolio(&assets, None, CopulaType::Gaussian, 365, 5_000, 42);
        let single = run_portfolio_scenario(
            &assets, None, CopulaType::Gaussian, "ssp585", 365, 5_000, 42
        );
        let full_s = full.scenarios.iter().find(|s| s.scenario_key == "ssp585").unwrap();
        assert!((single.var95_mm  - full_s.var95_mm ).abs() < 1e-6);
        assert!((single.cvar95_mm - full_s.cvar95_mm).abs() < 1e-6);
    }

    #[test]
    fn integration_cholesky_roundtrip() {
        let assets = sample_assets();
        let corr   = build_correlation_matrix(&assets);
        let n      = assets.len();
        let l      = cholesky(&corr, n).unwrap();
        for i in 0..n {
            for j in 0..n {
                let r: f64 = (0..n).map(|k| l[i * n + k] * l[j * n + k]).sum();
                assert!((r - corr[i * n + j]).abs() < 1e-9, "L·Lᵀ[{i},{j}] mismatch");
            }
        }
    }

    #[test]
    fn integration_correlation_regime_psd() {
        let assets = sample_assets();
        let corr   = build_correlation_matrix(&assets);
        assert!(CorrelationRegime::new(corr, assets.len()).is_ok());
    }

    #[test]
    fn integration_hhi_uniform_weights() {
        let w = vec![1.0 / 3.0; 3];
        assert!((herfindahl(&w) - 1.0 / 3.0).abs() < 1e-10);
    }
}
