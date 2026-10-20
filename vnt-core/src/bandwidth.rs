//! Versioned wire types shared by the client and VNTS bandwidth controller.
//! Keep this file in sync with vnts/src/bandwidth_wire.rs.
use serde::{Deserialize, Serialize};

pub const VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BandwidthMode {
    #[default]
    Off,
    Observe,
    Auto,
}

impl std::str::FromStr for BandwidthMode {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "observe" => Ok(Self::Observe),
            "auto" => Ok(Self::Auto),
            _ => Err("bandwidth_mode must be off, observe or auto".into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Status,
    Permit {
        id: String,
    },
    ReleasePermit {
        id: String,
    },
    Reserve {
        id: String,
        epoch: u64,
        upload_bps: u64,
        download_bps: u64,
        select_egress: bool,
    },
    Commit {
        id: String,
    },
    Cancel {
        id: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Status {
    pub version: u32,
    pub instance: Vec<u8>,
    pub automatic: bool,
    pub fresh: bool,
    pub inbound_capacity_bps: u64,
    pub outbound_capacity_bps: u64,
    pub inbound_bps: u64,
    pub outbound_bps: u64,
    pub inbound_available_bps: u64,
    pub outbound_available_bps: u64,
    pub client_upload_bps: u64,
    pub client_download_bps: u64,
    pub egress_epoch: u64,
    pub pending: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub status: Status,
    /// disabled, status, queued, approved, committed, cancelled, expired or rejected.
    pub result: String,
    pub id: String,
}

/// Learns capability from separate sustained transfers, never from one peak.
#[derive(Debug, Default)]
pub struct LearnedBandwidth {
    pub estimate_bps: u64,
    pub transfers: u32,
    pub active_seconds: u64,
    peak_sustained_bps: u64,
    valid_seconds: u64,
    idle_seconds: u64,
    unverified_seconds: u64,
    history: std::collections::VecDeque<u64>,
}

impl LearnedBandwidth {
    pub fn sample(&mut self, bps: u64, seconds: u64, constrained: bool) {
        self.unverified_seconds = self.unverified_seconds.saturating_add(seconds);
        if self.unverified_seconds >= 1800 {
            self.history.clear();
            self.transfers = 0;
            self.estimate_bps = 0;
            self.valid_seconds = 0;
            self.peak_sustained_bps = 0;
        }
        if bps < 64_000 {
            self.idle_seconds = self.idle_seconds.saturating_add(seconds);
            if self.idle_seconds >= 10 {
                if self.valid_seconds >= 15 {
                    self.history.push_back(self.peak_sustained_bps);
                    if self.history.len() > 8 {
                        self.history.pop_front();
                    }
                    self.transfers = self.history.len() as u32;
                    let mut history = self.history.iter().copied().collect::<Vec<_>>();
                    history.sort_unstable();
                    self.estimate_bps = history[(history.len() - 1) * 3 / 4];
                }
                self.active_seconds = 0;
                self.valid_seconds = 0;
                self.peak_sustained_bps = 0;
            }
            return;
        }
        self.idle_seconds = 0;
        self.active_seconds = self.active_seconds.saturating_add(seconds);
        if !constrained && self.active_seconds >= 10 {
            self.unverified_seconds = 0;
            self.valid_seconds = self.valid_seconds.saturating_add(seconds);
            // A window, rather than a single instantaneous byte counter.
            self.peak_sustained_bps = if self.peak_sustained_bps == 0 {
                bps
            } else {
                (self
                    .peak_sustained_bps
                    .saturating_mul(3)
                    .saturating_add(bps))
                    / 4
            };
        }
    }

    pub fn demand(&self, current_bps: u64, constrained: bool) -> u64 {
        if constrained && self.transfers >= 3 {
            // History bounds a modest growth allowance; it cannot prove that
            // a currently small transfer is requesting the full line rate.
            current_bps.max(self.estimate_bps.min(current_bps.saturating_mul(2)))
        } else {
            current_bps
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_bursts_do_not_teach_capacity() {
        let mut learned = LearnedBandwidth::default();
        learned.sample(100_000_000, 5, false);
        learned.sample(0, 10, false);
        assert_eq!(learned.transfers, 0);
        assert_eq!(learned.estimate_bps, 0);
    }

    #[test]
    fn several_transfers_teach_without_turning_history_into_idle_demand() {
        let mut learned = LearnedBandwidth::default();
        for _ in 0..3 {
            for _ in 0..6 {
                learned.sample(40_000_000, 5, false);
            }
            learned.sample(0, 10, false);
        }
        assert_eq!(learned.transfers, 3);
        assert_eq!(learned.demand(10_000_000, true), 20_000_000);
        assert_eq!(learned.demand(100_000, false), 100_000);
        assert_eq!(learned.demand(100_000, true), 200_000);
        assert_eq!(learned.demand(0, true), 0);
        for _ in 0..10 {
            learned.sample(5_000_000, 5, true);
        }
        learned.sample(0, 10, false);
        assert_eq!(learned.estimate_bps, 40_000_000);
    }

    #[test]
    fn several_slower_transfers_and_expiry_adapt_the_profile() {
        let mut learned = LearnedBandwidth::default();
        for rate in [40_000_000; 3].into_iter().chain([10_000_000; 8]) {
            for _ in 0..6 {
                learned.sample(rate, 5, false);
            }
            learned.sample(0, 10, false);
        }
        assert_eq!(learned.estimate_bps, 10_000_000);
        learned.sample(0, 1800, true);
        assert_eq!(learned.transfers, 0);
        assert_eq!(learned.demand(1_000_000, true), 1_000_000);
    }
}
