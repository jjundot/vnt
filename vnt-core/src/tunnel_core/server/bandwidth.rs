//! Slow control loop; never invoked by a packet's fast path.
use crate::bandwidth::{BandwidthMode, LearnedBandwidth, Request, Status};
use crate::context::ServerInfoCollection;
use crate::tunnel_core::server::rpc::ServerRPC;
use futures::{StreamExt, stream::FuturesUnordered};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const TICK: Duration = Duration::from_secs(5);
const DWELL: Duration = Duration::from_secs(300);
const PRESSURE: Duration = Duration::from_secs(30);

struct Pending {
    target: u32,
    source: u32,
    egress: bool,
    id: String,
    instance: Vec<u8>,
    epoch: u64,
    budget: u64,
    started: Instant,
    commit_attempted: bool,
}

#[derive(Default)]
struct Direction {
    selected: Option<u32>,
    pressure_since: Option<Instant>,
}

fn utilization(used: u64, capacity: u64) -> u64 {
    if capacity == 0 {
        0
    } else {
        ((used as u128 * 100) / capacity as u128).min(1000) as u64
    }
}

fn source(statuses: &HashMap<u32, Status>, direction: &Direction, egress: bool) -> Option<u32> {
    direction
        .selected
        .filter(|id| statuses.contains_key(id))
        .or_else(|| {
            statuses
                .iter()
                .max_by_key(|(id, s)| {
                    (
                        if egress {
                            s.client_download_bps
                        } else {
                            s.client_upload_bps
                        },
                        std::cmp::Reverse(**id),
                    )
                })
                .map(|(id, _)| *id)
        })
}

fn pressured(status: &Status, egress: bool) -> bool {
    if egress {
        utilization(status.outbound_bps, status.outbound_capacity_bps) >= 85
    } else {
        utilization(status.inbound_bps, status.inbound_capacity_bps) >= 85
    }
}

fn candidate(status: &Status, budget: u64, egress: bool, current: &Status) -> bool {
    // Until a complete destination resource map is available, budget both
    // public directions conservatively. They are still independent accounts.
    let fits = |available: u64| (available as u128) * 2 > budget as u128 * 3;
    if !status.fresh
        || budget == 0
        || !fits(status.inbound_available_bps)
        || !fits(status.outbound_available_bps)
    {
        return false;
    }
    let (used, capacity, source_used, source_capacity) = if egress {
        (
            status.outbound_bps,
            status.outbound_capacity_bps,
            current.outbound_bps,
            current.outbound_capacity_bps,
        )
    } else {
        (
            status.inbound_bps,
            status.inbound_capacity_bps,
            current.inbound_bps,
            current.inbound_capacity_bps,
        )
    };
    // Require at least ten percentage points of predicted utilization benefit.
    utilization(used.saturating_add(budget), capacity).saturating_add(10)
        < utilization(source_used, source_capacity)
}

pub(crate) async fn run(rpc: ServerRPC, servers: ServerInfoCollection, mode: BandwidthMode) {
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut upload = LearnedBandwidth::default();
    let mut download = LearnedBandwidth::default();
    let mut ingress = Direction::default();
    let mut egress = Direction::default();
    let mut pending: Option<Pending> = None;
    let mut next_move = Instant::now() + DWELL;
    let mut epoch = crate::utils::time::now_ts_ms().max(1) as u64;
    let mut last_sample = Instant::now();
    let mut instances = HashMap::<u32, Vec<u8>>::new();
    let mut min_rtt = HashMap::<u32, u32>::new();
    let mut next_report = Instant::now();

    loop {
        interval.tick().await;
        let nodes = servers.server_node_list();
        let requests = FuturesUnordered::new();
        for node in nodes.iter().filter(|node| node.connected) {
            let rpc = rpc.clone();
            let id = node.server_id;
            requests.push(async move { (id, rpc.bandwidth(id, Request::Status).await) });
        }
        let mut requests = requests;
        let mut statuses = HashMap::new();
        let mut seen = HashSet::new();
        while let Some((id, response)) = requests.next().await {
            if let Ok(response) = response
                && response.status.fresh
                && response.status.instance.len() == 32
            {
                if instances
                    .get(&id)
                    .is_some_and(|old| old != &response.status.instance)
                {
                    upload = LearnedBandwidth::default();
                    download = LearnedBandwidth::default();
                    if ingress.selected == Some(id) {
                        ingress.selected = None;
                        servers.clear_bandwidth_ingress();
                    }
                    if egress.selected == Some(id) {
                        egress.selected = None;
                    }
                    ingress.pressure_since = None;
                    egress.pressure_since = None;
                    next_move = Instant::now() + DWELL;
                }
                instances.insert(id, response.status.instance.clone());
                statuses.insert(id, response.status);
            }
        }
        let mut ids = statuses.keys().copied().collect::<Vec<_>>();
        ids.sort_unstable();
        for id in ids {
            if !seen.insert(statuses[&id].instance.clone()) {
                statuses.remove(&id);
            }
        }
        let now = Instant::now();
        let elapsed = now.duration_since(last_sample);
        last_sample = now;
        if statuses.is_empty() || elapsed > Duration::from_secs(15) {
            upload = LearnedBandwidth::default();
            download = LearnedBandwidth::default();
            ingress.pressure_since = None;
            egress.pressure_since = None;
            continue;
        }
        let up = statuses
            .values()
            .fold(0u64, |n, s| n.saturating_add(s.client_upload_bps));
        let down = statuses
            .values()
            .fold(0u64, |n, s| n.saturating_add(s.client_download_bps));
        if ingress.selected.is_some_and(|id| {
            !nodes
                .iter()
                .any(|node| node.server_id == id && node.connected)
        }) {
            ingress.selected = None;
            ingress.pressure_since = None;
            servers.clear_bandwidth_ingress();
            next_move = now + DWELL;
        }
        if egress.selected.is_some_and(|id| {
            !nodes
                .iter()
                .any(|node| node.server_id == id && node.connected)
        }) {
            egress.selected = None;
            egress.pressure_since = None;
            next_move = now + DWELL;
        }
        // Missing telemetry on a healthy selected route freezes optimization.
        if ingress
            .selected
            .is_some_and(|id| !statuses.contains_key(&id))
            || egress
                .selected
                .is_some_and(|id| !statuses.contains_key(&id))
        {
            continue;
        }
        let up_source = source(&statuses, &ingress, false);
        let down_source = source(&statuses, &egress, true);
        let up_pressure = up_source
            .and_then(|id| statuses.get(&id))
            .is_some_and(|s| pressured(s, false));
        let down_pressure = down_source
            .and_then(|id| statuses.get(&id))
            .is_some_and(|s| pressured(s, true));
        upload.sample(up, elapsed.as_secs(), up_pressure);
        download.sample(down, elapsed.as_secs(), down_pressure);
        for node in &nodes {
            if let Some(rtt) = node.rtt {
                min_rtt
                    .entry(node.server_id)
                    .and_modify(|v| *v = (*v).min(rtt))
                    .or_insert(rtt);
            }
        }
        if mode == BandwidthMode::Auto && ingress.selected != servers.bandwidth_bound_ingress() {
            ingress.selected = servers.bandwidth_bound_ingress();
            ingress.pressure_since = None;
            next_move = now + DWELL;
        }
        if now >= next_report {
            log::info!(
                "bandwidth sample mode={:?} servers={} upload_bps={} download_bps={} learned_upload_bps={} learned_download_bps={} upload_transfers={} download_transfers={}",
                mode,
                statuses.len(),
                up,
                down,
                upload.estimate_bps,
                download.estimate_bps,
                upload.transfers,
                download.transfers
            );
            next_report = now + Duration::from_secs(30);
        }
        if let Some(mut waiting) = pending.take() {
            let target = statuses.get(&waiting.target);
            let current = statuses.get(&waiting.source);
            let still_needed = current.is_some_and(|s| pressured(s, waiting.egress))
                && if waiting.egress {
                    download.active_seconds >= 15 && down >= 64_000
                } else {
                    upload.active_seconds >= 15 && up >= 64_000
                };
            if waiting.commit_attempted
                && waiting.egress
                && target.is_some_and(|s| {
                    s.instance == waiting.instance && s.egress_epoch == waiting.epoch
                })
            {
                egress.selected = Some(waiting.target);
                next_move = now + DWELL;
                ingress.pressure_since = None;
                egress.pressure_since = None;
                continue;
            }
            if (!waiting.commit_attempted
                && (now.duration_since(waiting.started) > Duration::from_secs(25) || !still_needed))
                || target.is_some_and(|s| s.instance != waiting.instance)
            {
                let _ = rpc
                    .bandwidth(
                        waiting.target,
                        Request::Cancel {
                            id: waiting.id.clone(),
                        },
                    )
                    .await;
                let _ = rpc
                    .bandwidth(waiting.source, Request::ReleasePermit { id: waiting.id })
                    .await;
                next_move = now + Duration::from_secs(60);
                continue;
            }
            if target.is_none() && waiting.commit_attempted {
                pending = Some(waiting);
                continue;
            }
            let response = rpc
                .bandwidth(
                    waiting.target,
                    Request::Reserve {
                        id: waiting.id.clone(),
                        epoch: waiting.epoch,
                        upload_bps: waiting.budget,
                        download_bps: waiting.budget,
                        select_egress: waiting.egress,
                    },
                )
                .await;
            match response {
                Ok(response) if response.result == "approved" || response.result == "committed" => {
                    // A successful RPC verifies the same authenticated client
                    // connection that will carry this direction's business data.
                    waiting.commit_attempted = true;
                    let committed = rpc
                        .bandwidth(
                            waiting.target,
                            Request::Commit {
                                id: waiting.id.clone(),
                            },
                        )
                        .await;
                    if let Ok(response) = committed
                        && response.result == "committed"
                        && response.status.instance == waiting.instance
                    {
                        if waiting.egress {
                            egress.selected = Some(waiting.target);
                        } else {
                            ingress.selected = Some(waiting.target);
                            servers.bind_bandwidth_ingress(waiting.target, waiting.instance);
                        }
                        next_move = now + DWELL;
                        ingress.pressure_since = None;
                        egress.pressure_since = None;
                        log::info!(
                            "bandwidth migration committed id={} source={} target={} egress={} budget_bps={}",
                            waiting.id,
                            waiting.source,
                            waiting.target,
                            waiting.egress,
                            waiting.budget
                        );
                    } else {
                        // Resolve an uncertain commit with the same id. Never
                        // cancel a potentially committed download route.
                        pending = Some(waiting);
                    }
                }
                Ok(response) if response.result == "queued" => pending = Some(waiting),
                Err(_) if waiting.commit_attempted => pending = Some(waiting),
                _ => {
                    let _ = rpc
                        .bandwidth(waiting.source, Request::ReleasePermit { id: waiting.id })
                        .await;
                    next_move = now + Duration::from_secs(60);
                }
            }
            continue;
        }
        let mut suggestions = Vec::new();
        for (direction, learned, current_bps, current_id, pressure, is_egress) in [
            (&mut ingress, &upload, up, up_source, up_pressure, false),
            (
                &mut egress,
                &download,
                down,
                down_source,
                down_pressure,
                true,
            ),
        ] {
            if !pressure || learned.active_seconds < 15 || current_bps < 64_000 {
                direction.pressure_since = None;
                continue;
            }
            let since = *direction.pressure_since.get_or_insert(now);
            if now.duration_since(since) < PRESSURE || now < next_move {
                continue;
            }
            let Some(current_id) = current_id else {
                continue;
            };
            let current = &statuses[&current_id];
            if mode == BandwidthMode::Auto && !current.automatic {
                continue;
            }
            let budget = ((learned.demand(current_bps, true) as u128 * 115 / 100)
                .min(u64::MAX as u128)) as u64;
            let current_node = nodes.iter().find(|node| node.server_id == current_id);
            // Utilization alone is insufficient: require a learned demand gap
            // or RTT inflation relative to this path's own baseline.
            let demand_gap =
                learned.transfers >= 3 && budget as u128 > current_bps as u128 * 13 / 10;
            let delayed = current_node.and_then(|node| node.rtt).is_some_and(|rtt| {
                rtt > min_rtt
                    .get(&current_id)
                    .copied()
                    .unwrap_or(rtt)
                    .saturating_add(15)
            });
            if !delayed && (mode == BandwidthMode::Auto || !demand_gap) {
                continue;
            }
            let current_rtt = current_node.and_then(|node| node.rtt).unwrap_or(500);
            let target = statuses
                .iter()
                .filter(|(id, status)| {
                    if **id == current_id
                        || (mode == BandwidthMode::Auto && !status.automatic)
                        || !candidate(status, budget, is_egress, current)
                    {
                        return false;
                    }
                    let Some(node) = nodes.iter().find(|node| node.server_id == **id) else {
                        return false;
                    };
                    if node
                        .rtt
                        .is_none_or(|rtt| rtt > current_rtt.saturating_add(50))
                    {
                        return false;
                    }
                    // Retain reachability for destinations currently advertised by
                    // the source; an entry pin must not strand another active peer.
                    is_egress
                        || current_node.is_some_and(|old| {
                            old.client_map
                                .values()
                                .filter(|client| client.online)
                                .all(|client| {
                                    node.client_map.get(&client.ip).is_some_and(|c| c.online)
                                })
                        })
                })
                .max_by_key(|(id, s)| {
                    (
                        if is_egress {
                            s.outbound_available_bps
                        } else {
                            s.inbound_available_bps
                        },
                        std::cmp::Reverse(**id),
                    )
                });
            if let Some((&target, status)) = target {
                suggestions.push((
                    current_id,
                    target,
                    is_egress,
                    budget,
                    status.instance.clone(),
                ));
            }
        }
        if let Some((source, target, is_egress, budget, instance)) = suggestions.into_iter().next()
        {
            log::info!(
                "bandwidth migration suggestion source={} target={} egress={} budget_bps={} mode={:?} upload_bps={} download_bps={} learned_upload_bps={} learned_download_bps={}",
                source,
                target,
                is_egress,
                budget,
                mode,
                up,
                down,
                upload.estimate_bps,
                download.estimate_bps
            );
            if mode == BandwidthMode::Auto {
                epoch = epoch.saturating_add(1);
                let id = format!("{:032x}-{epoch}", rand::random::<u128>());
                if rpc
                    .bandwidth(source, Request::Permit { id: id.clone() })
                    .await
                    .is_ok_and(|r| r.result == "permitted")
                {
                    pending = Some(Pending {
                        source,
                        target,
                        egress: is_egress,
                        id,
                        epoch,
                        budget,
                        instance,
                        started: now,
                        commit_attempted: false,
                    });
                } else {
                    next_move = now + Duration::from_secs(30);
                }
            } else {
                next_move = now + Duration::from_secs(60);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_checks_both_directions_and_requires_predicted_gain() {
        let current = Status {
            inbound_bps: 90,
            inbound_capacity_bps: 100,
            ..Default::default()
        };
        let mut target = Status {
            automatic: true,
            fresh: true,
            inbound_capacity_bps: 100,
            outbound_capacity_bps: 100,
            inbound_available_bps: 80,
            outbound_available_bps: 10,
            ..Default::default()
        };
        assert!(!candidate(&target, 20, false, &current));
        target.outbound_available_bps = 80;
        assert!(candidate(&target, 20, false, &current));
        target.inbound_bps = 70;
        assert!(!candidate(&target, 20, false, &current));
        target.inbound_bps = 0;
        target.fresh = false;
        assert!(!candidate(&target, 20, false, &current));
    }
    #[test]
    fn direction_binding_stays_stable_when_other_servers_become_idle() {
        let statuses = HashMap::from([
            (
                1,
                Status {
                    client_upload_bps: 10,
                    ..Default::default()
                },
            ),
            (
                2,
                Status {
                    client_upload_bps: 100,
                    ..Default::default()
                },
            ),
        ]);
        assert_eq!(
            source(
                &statuses,
                &Direction {
                    selected: Some(1),
                    ..Default::default()
                },
                false
            ),
            Some(1)
        );
        assert_eq!(source(&statuses, &Direction::default(), false), Some(2));
    }
}
