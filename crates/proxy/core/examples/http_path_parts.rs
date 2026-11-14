//! The HTTP adapter's per request path, one step at a time, single thread.
//! Shows which step of `decision_bench`'s `http_request_path` costs what.
//!
//! `cargo run --release -p turbolay-proxy-core --example http_path_parts`

use std::sync::Arc;
use std::time::Instant;

use hydradb_scope::GraphScope;
use turbolay_proxy_core::types::{Access, NodeId, NodeInfo, Operation, Protocol, Roles, RouteKey, RouteSnapshot};
use turbolay_proxy_core::{ReadPolicy, RendezvousPlacement, RouteOutcome, Router, RouterConfig};

const N: usize = 1_000_000;

fn time<T>(name: &str, mut step: impl FnMut(usize) -> T) {
    let started = Instant::now();
    for index in 0..N {
        std::hint::black_box(step(index));
    }
    println!(
        "{{\"step\":\"{name}\",\"ns\":{:.0}}}",
        started.elapsed().as_nanos() as f64 / N as f64
    );
}

fn main() {
    let snapshot = Arc::new(RouteSnapshot {
        nodes: (0..3)
            .map(|index| NodeInfo {
                id: NodeId::new(format!("graph-node-{index}")),
                bolt_addr: format!("10.0.0.{index}:7687"),
                http_addr: Some(format!("10.0.0.{index}:8443")),
                roles: Roles::default(),
            })
            .collect(),
        proxies: vec!["p0:7687".into()],
    });
    let (router, _writer) = Router::new(
        snapshot,
        RendezvousPlacement,
        RouterConfig {
            read_policy: ReadPolicy::Fleet,
            node_budget: 1 << 20,
            ..RouterConfig::default()
        },
    );
    let headers: Vec<String> = (0..4096).map(|tenant| format!("hydradb/dGVuYW50LT{tenant}")).collect();
    let scopes: Vec<GraphScope> = headers
        .iter()
        .map(|header| hydradb_scope::http::http_graph_scope(Some(header), "knowledge", false).unwrap())
        .collect();
    let keys: Vec<RouteKey> = scopes.iter().map(|scope| RouteKey::new(scope, "cell-0").unwrap()).collect();
    let read = Operation {
        access: Access::Read,
        protocol: Protocol::Http,
    };
    let write = Operation {
        access: Access::Write,
        protocol: Protocol::Http,
    };
    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    let target = NodeId::new("graph-node-1");

    time("scope_parse", |i| {
        hydradb_scope::http::http_graph_scope(Some(&headers[i % 4096]), "knowledge", false).unwrap()
    });
    time("route_key", |i| RouteKey::new(&scopes[i % 4096], "cell-0").unwrap());
    time("route_key_clone", |i| keys[i % 4096].clone());
    time("node_id_clone", |_| target.clone());
    time("route_read_fleet", |i| {
        runtime.block_on(router.route(&keys[i % 4096], read)).unwrap()
    });
    time("route_write_owner", |i| {
        runtime.block_on(router.route(&keys[i % 4096], write)).unwrap()
    });
    time("admit_release", |_| drop(runtime.block_on(router.admit(&target)).unwrap()));
    time("snapshot_lookup", |_| {
        router.snapshot().node(target.as_str()).map(|(_, node)| node.bolt_addr.len())
    });
    time("record", |_| {
        router.nodes().record(&target, Protocol::Http, Access::Read, RouteOutcome::Ok)
    });
    time("unit_counter", |_| router.metrics().unit(Protocol::Http, Access::Read));
}
