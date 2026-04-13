// =============================================================================
// Meteorium Engine — distributions.rs
// Prexus Intelligence · v2.0.0
//
// Distribution enum covering climate, yield, and financial risk variables.
// Each variant owns its parameters and implements per-step sampling.
// =============================================================================

use rand::Rng;
use rand_distr::{Distribution, Normal, LogNormal, Poisson, Beta, Uniform};
use serde::{Deserialize, Serialize};

/// Supported stochastic process / distribution types.
/// Designed for Meteorium's multi-domain risk variables:
///   - Climate  → Normal, LogNormal, Poisson (event frequency)
///   - Yield    → Beta (bounded [0,1] loss fractions), LogNormal
///   - Financial → GBM (Geometric Brownian Motion), MeanReverting (OU)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DistributionConfig {
    /// Additive Gaussian shocks — temperature anomalies, normalised indices
    Normal {
        mean: f64,
        std_dev: f64,
    },

    /// Multiplicative log-normal — precipitation, wind speed, energy demand
    LogNormal {
        mu: f64,    // mean of the underlying normal
        sigma: f64, // std-dev of the underlying normal
    },

    /// Poisson count process — extreme-event frequency (storms, floods)
    Poisson {
        lambda: f64, // expected events per time step
    },

    /// Beta — bounded loss fraction in [0, 1] (crop failure ratio, damage index)
    Beta {
        alpha: f64,
        beta: f64,
    },

    /// Geometric Brownian Motion — financial assets, carbon price, commodity price.
    /// S(t+dt) = S(t) · exp((mu - 0.5·sigma²)·dt + sigma·√dt·Z)
    Gbm {
        mu: f64,      // drift (annualised)
        sigma: f64,   // volatility (annualised)
        s0: f64,      // initial value
        dt: f64,      // time step in years (e.g. 1/252 for daily)
    },

    /// Ornstein–Uhlenbeck mean-reverting process — temperature, interest rates.
    /// dX = theta·(mu - X)·dt + sigma·dW
    MeanReverting {
        mu: f64,      // long-run mean
        theta: f64,   // speed of reversion
        sigma: f64,   // volatility
        x0: f64,      // initial value
        dt: f64,      // time step
    },

    /// Uniform — scenario sweep / stress testing
    Uniform {
        low: f64,
        high: f64,
    },
}

/// Stateful sampler wrapping a DistributionConfig.
/// Holds mutable state for path-dependent processes (GBM, OU).
#[derive(Debug, Clone)]
pub struct Sampler {
    config: DistributionConfig,
    /// Current level for path-dependent processes
    state: f64,
}

impl Sampler {
    pub fn new(config: DistributionConfig) -> Self {
        let state = match &config {
            DistributionConfig::Gbm  { s0, .. } => *s0,
            DistributionConfig::MeanReverting { x0, .. } => *x0,
            _ => 0.0,
        };
        Sampler { config, state }
    }

    /// Draw one sample for the current time step.
    /// For path-dependent processes this advances `self.state`.
    pub fn sample<R: Rng>(&mut self, rng: &mut R) -> f64 {
        match &self.config {
            DistributionConfig::Normal { mean, std_dev } => {
                Normal::new(*mean, *std_dev)
                    .expect("Invalid Normal params")
                    .sample(rng)
            }

            DistributionConfig::LogNormal { mu, sigma } => {
                LogNormal::new(*mu, *sigma)
                    .expect("Invalid LogNormal params")
                    .sample(rng)
            }

            DistributionConfig::Poisson { lambda } => {
                Poisson::new(*lambda)
                    .expect("Invalid Poisson params")
                    .sample(rng) as f64
            }

            DistributionConfig::Beta { alpha, beta } => {
                Beta::new(*alpha, *beta)
                    .expect("Invalid Beta params")
                    .sample(rng)
            }

            DistributionConfig::Uniform { low, high } => {
                Uniform::new(*low, *high).sample(rng)
            }

            DistributionConfig::Gbm { mu, sigma, dt, .. } => {
                let z: f64 = Normal::new(0.0_f64, 1.0_f64)
                    .unwrap()
                    .sample(rng);
                let prev = self.state;
                let next = prev * ((mu - 0.5 * sigma * sigma) * dt
                    + sigma * dt.sqrt() * z).exp();
                self.state = next;
                next
            }

            DistributionConfig::MeanReverting { mu, theta, sigma, dt, .. } => {
                let z: f64 = Normal::new(0.0_f64, 1.0_f64)
                    .unwrap()
                    .sample(rng);
                let prev = self.state;
                let next = prev
                    + theta * (mu - prev) * dt
                    + sigma * dt.sqrt() * z;
                self.state = next;
                next
            }
        }
    }

    /// Reset path-dependent state (called before each simulation path).
    pub fn reset(&mut self) {
        self.state = match &self.config {
            DistributionConfig::Gbm { s0, .. } => *s0,
            DistributionConfig::MeanReverting { x0, .. } => *x0,
            _ => 0.0,
        };
    }
}
