//! What the core's decisions cost per unit, and whether they contend.
//!
//! Every unit through the proxy makes the same calls: `route`, `admit`, the
//! permit's drop, and `classify` when something fails. This runs that sequence
//! in a tight loop on 1..N threads over many tenants and prints nanoseconds
//! per unit. Flat per-unit cost as threads rise means no shared lock or
//! contended cache line on the decision path.
//!
//! `cargo run --release -p turbolay-proxy-core --example decision_bench`

use std::sync::Arc;
use std::time::Instant;

use hydradb_scope::{GraphId, GraphScope, NamespacePath};
use turbolay_proxy_core::types::{Access, NodeId, NodeInfo, Operation, Outcome, Protocol, Roles, RouteKey, RouteSnapshot};
use turbolay_proxy_core::{ReadPolicy, RendezvousPlacement, RouteOutcome, Router, RouterConfig};

fn main() {
    let snapshot = Arc::new(RouteSnapshot {
        nodes: (0..3)
            .map(|index| NodeInfo {
                id: NodeId::new(format!("graph-node-{index}")),
                bolt_addr: format!("10.0.0.{index}:7687"),
                http_addr: None,
                roles: Roles::default(),
            })
            .collect(),
        proxies: vec!["p0:7687".into(), "p1:7687".into(), "p2:7687".into()],
    });
    let keys: Vec<RouteKey> = (0..4096)
        .map(|tenant| {
            let scope = GraphScope::new(
                NamespacePath::parse(&format!("hydradb/dGVuYW50LT{tenant}")).unwrap(),
                GraphId::new("knowledge").unwrap(),
            );
            RouteKey::new(&scope, "cell-0").unwrap()
        })
        .collect();
    let keys = Arc::new(keys);
    let max_threads = std::thread::available_parallelism().map_or(4, usize::from).min(8);
    for policy in [ReadPolicy::Fleet, ReadPolicy::Owner] {
        let (router, _writer) = Router::new(
            Arc::clone(&snapshot),
            RendezvousPlacement,
            RouterConfig {
                read_policy: policy,
                node_budget: 1 << 20,
                ..RouterConfig::default()
            },
        );
        let mut threads = 1;
        while threads <= max_threads {
            let units_per_thread = 400_000;
            let started = Instant::now();
            let handles: Vec<_> = (0..threads)
                .map(|thread| {
                    let router = Arc::clone(&router);
                    let keys = Arc::clone(&keys);
                    std::thread::spawn(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
                        runtime.block_on(async move {
                            for unit in 0..units_per_thread {
                                let key = &keys[(unit * 7 + thread * 131) % keys.len()];
                                let operation = Operation {
                                    access: if unit % 2 == 0 { Access::Read } else { Access::Write },
                                    protocol: Protocol::Bolt,
                                };
                                let route = router.route(key, operation).await.unwrap();
                                let permit = router.admit(&route.candidates[0]).await.unwrap();
                                std::hint::black_box(router.classify(operation, &Outcome::ConnectFailed, 1));
                                drop(permit);
                            }
                        });
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap();
            }
            let elapsed = started.elapsed();
            let units = (units_per_thread * threads) as f64;
            println!(
                "{{\"bench\":\"decisions\",\"policy\":\"{}\",\"threads\":{threads},\"ns_per_unit_per_thread\":{:.0},\"units_per_sec\":{:.0}}}",
                policy.as_str(),
                elapsed.as_nanos() as f64 * threads as f64 / units,
                units / elapsed.as_secs_f64()
            );
            threads *= 2;
        }
    }
    http_request_path(&snapshot, max_threads);
}

/// Everything the HTTP adapter does per request, minus the I/O: parse the scope from the namespace header and path, build the route
/// key, route, admit, look the target's address up in the snapshot, record the
/// outcome twice (the response filter and logging), release the permit. The
/// number to compare with the proxy's CPU per request.
fn http_request_path(snapshot: &Arc<RouteSnapshot>, max_threads: usize) {
    let headers: Arc<Vec<String>> = Arc::new((0..4096).map(|tenant| format!("hydradb/dGVuYW50LT{tenant}")).collect());
    let (router, _writer) = Router::new(
        Arc::clone(snapshot),
        RendezvousPlacement,
        RouterConfig {
            read_policy: ReadPolicy::Fleet,
            node_budget: 1 << 20,
            ..RouterConfig::default()
        },
    );
    let mut threads = 1;
    while threads <= max_threads {
        let requests_per_thread = 400_000;
        let started = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                let router = Arc::clone(&router);
                let headers = Arc::clone(&headers);
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
                    runtime.block_on(async move {
                        for request in 0..requests_per_thread {
                            let header = &headers[(request * 7 + thread * 131) % headers.len()];
                            let scope = hydradb_scope::http::http_graph_scope(Some(header), "knowledge", false).unwrap();
                            let key = RouteKey::new(&scope, "cell-0").unwrap();
                            let operation = Operation {
                                access: if request % 2 == 0 { Access::Read } else { Access::Write },
                                protocol: Protocol::Http,
                            };
                            router.metrics().unit(operation.protocol, operation.access);
                            let route = router.route(&key, operation).await.unwrap();
                            let target = route.candidates[0].clone();
                            let permit = router.admit(&target).await.unwrap();
                            {
                                let snapshot = router.snapshot();
                                std::hint::black_box(snapshot.node(target.as_str()).map(|(_, node)| node.bolt_addr.len()));
                            }
                            router
                                .nodes()
                                .record(&target, operation.protocol, operation.access, RouteOutcome::Ok);
                            drop(permit);
                        }
                    });
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        let elapsed = started.elapsed();
        let requests = (requests_per_thread * threads) as f64;
        println!(
            "{{\"bench\":\"http_request_path\",\"threads\":{threads},\"ns_per_request_per_thread\":{:.0},\"requests_per_sec\":{:.0}}}",
            elapsed.as_nanos() as f64 * threads as f64 / requests,
            requests / elapsed.as_secs_f64()
        );
        threads *= 2;
    }
}
