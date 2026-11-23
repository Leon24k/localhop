use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Fixed artificial delay in milliseconds added before forwarding.
    #[serde(default)]
    pub delay_ms: u64,
    /// Random extra jitter in milliseconds (`0..=jitter_ms`).
    #[serde(default)]
    pub jitter_ms: u64,
    /// Fault injection percentage (`0.0..=100.0`).
    #[serde(default)]
    pub fail_rate_pct: f64,
    /// HTTP status returned when fault injection triggers (default 503).
    #[serde(default = "default_fail_status")]
    pub fail_status: u16,
    /// Optional path prefix filter (e.g. `"/api"`). Empty matches all paths.
    #[serde(default)]
    pub path_prefix: String,
}

fn default_fail_status() -> u16 {
    503
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            delay_ms: 0,
            jitter_ms: 0,
            fail_rate_pct: 0.0,
            fail_status: default_fail_status(),
            path_prefix: String::new(),
        }
    }
}

impl ChaosConfig {
    pub fn sanitized(mut self) -> Self {
        self.fail_rate_pct = self.fail_rate_pct.clamp(0.0, 100.0);
        if !(400..=599).contains(&self.fail_status) {
            self.fail_status = 503;
        }
        self.path_prefix = self.path_prefix.trim().to_string();
        self
    }

    pub fn has_effect(&self) -> bool {
        self.enabled && (self.delay_ms > 0 || self.jitter_ms > 0 || self.fail_rate_pct > 0.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChaosAction {
    Pass,
    Delay(Duration),
    Fault {
        delay: Option<Duration>,
        status: u16,
    },
}

/// Lock-free-when-disabled chaos and fault injection controller.
///
/// When disabled, `is_active()` compiles down to a single relaxed atomic load
/// so the normal tunnel hot path pays zero lock or allocation cost.
pub struct ChaosController {
    active: AtomicBool,
    config: RwLock<ChaosConfig>,
}

impl ChaosController {
    pub fn new(initial: ChaosConfig) -> Arc<Self> {
        let cfg = initial.sanitized();
        let active = cfg.has_effect();
        Arc::new(Self {
            active: AtomicBool::new(active),
            config: RwLock::new(cfg),
        })
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub async fn get(&self) -> ChaosConfig {
        self.config.read().await.clone()
    }

    pub async fn update(&self, next: ChaosConfig) -> ChaosConfig {
        let clean = next.sanitized();
        let active = clean.has_effect();
        *self.config.write().await = clean.clone();
        self.active.store(active, Ordering::Relaxed);
        clean
    }

    pub async fn evaluate(&self, path: &str) -> ChaosAction {
        if !self.is_active() {
            return ChaosAction::Pass;
        }
        let cfg = self.config.read().await.clone();
        if !cfg.has_effect() {
            return ChaosAction::Pass;
        }
        if !cfg.path_prefix.is_empty() && !path.starts_with(&cfg.path_prefix) {
            return ChaosAction::Pass;
        }

        let mut rng = rand::rng();
        let extra = if cfg.jitter_ms > 0 {
            rng.random_range(0..=cfg.jitter_ms)
        } else {
            0
        };
        let total_ms = cfg.delay_ms.saturating_add(extra);
        let delay = (total_ms > 0).then(|| Duration::from_millis(total_ms));

        if cfg.fail_rate_pct > 0.0 {
            let roll: f64 = rng.random_range(0.0..100.0);
            if roll < cfg.fail_rate_pct {
                return ChaosAction::Fault {
                    delay,
                    status: cfg.fail_status,
                };
            }
        }

        match delay {
            Some(d) => ChaosAction::Delay(d),
            None => ChaosAction::Pass,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_is_pass_and_inactive() {
        let ctl = ChaosController::new(ChaosConfig::default());
        assert!(!ctl.is_active());
        assert_eq!(ctl.evaluate("/api/test").await, ChaosAction::Pass);
    }

    #[tokio::test]
    async fn path_filter_and_fault_injection() {
        let ctl = ChaosController::new(ChaosConfig {
            enabled: true,
            delay_ms: 25,
            jitter_ms: 0,
            fail_rate_pct: 100.0,
            fail_status: 429,
            path_prefix: "/api".to_string(),
        });
        assert!(ctl.is_active());
        assert_eq!(ctl.evaluate("/static/app.js").await, ChaosAction::Pass);
        assert_eq!(
            ctl.evaluate("/api/orders").await,
            ChaosAction::Fault {
                delay: Some(Duration::from_millis(25)),
                status: 429,
            }
        );

        // Live toggle off via update()
        ctl.update(ChaosConfig::default()).await;
        assert!(!ctl.is_active());
        assert_eq!(ctl.evaluate("/api/orders").await, ChaosAction::Pass);
    }
}
