//! `turbolay-proxy`: one Bolt and HTTP endpoint in front of every graph-node.
//!
//! Wiring only. Topology publishes the route snapshot, the core decides, the
//! two adapters translate, and pingora runs the listeners.

mod admin;
mod bolt_app;
mod config;

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use pingora_core::server::configuration::ServerConf;
use pingora_core::server::{Server, ShutdownWatch};
use pingora_core::services::background::{background_service, BackgroundService};
use pingora_core::services::listening::Service;
use tokio::sync::watch;
use turbolay_proxy_bolt::{Advertise, BoltConfig, BoltProxy, PoolConfig};
use turbolay_proxy_core::types::RetryBudget;
use turbolay_proxy_core::{Lifecycle, RendezvousPlacement, RouteWriter, Router, RouterConfig};
use turbolay_proxy_http::{HttpConfig, HttpProxy};
use turbolay_proxy_topology::{FileProxySet, MembershipSource, ProxySetSource, StaticMembership, StaticProxySet, Topology};

use crate::config::{Config, ProxySet};

#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// The topology, its sources and the router's writer: the one publisher and
/// the one writer, handed to the background task once.
struct Feed {
    topology: Topology,
    membership: Box<dyn MembershipSource>,
    proxies: Box<dyn ProxySetSource>,
    writer: RouteWriter,
}

/// Runs the topology sources and applies every snapshot they publish to the
/// router.
struct TopologyTask {
    feed: Mutex<Option<Feed>>,
    lifecycle: Lifecycle,
}

#[async_trait]
impl BackgroundService for TopologyTask {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let feed = self.feed.lock().expect("topology feed").take();
        if let Some(Feed {
            topology,
            membership,
            proxies,
            writer,
        }) = feed
        {
            let updates = topology.subscribe();
            tokio::select! {
                _ = topology.run(membership, proxies) => {}
                _ = writer.follow(updates) => {}
                _ = shutdown.changed() => {}
            }
        }
        self.lifecycle.stop();
    }
}

/// A replica source that marks `known` once it has reported, so the proxy
/// is not ready while its share of each node's budget is still a guess.
///
/// With `myself`, this replica always counts itself under that address. Only
/// the EndpointSlice source needs it: a starting replica is not yet listed
/// as ready there, and without itself it would divide each node's budget
/// among one replica too few. A static or file list is taken as written.
struct Reported {
    source: Box<dyn ProxySetSource>,
    known: Arc<AtomicBool>,
    myself: Option<String>,
}

#[async_trait]
impl ProxySetSource for Reported {
    async fn run(self: Box<Self>, proxies: watch::Sender<Vec<String>>) {
        let (inner, mut reports) = watch::channel(Vec::new());
        tokio::spawn(self.source.run(inner));
        while reports.changed().await.is_ok() {
            let mut set = reports.borrow_and_update().clone();
            if let Some(myself) = self.myself.as_ref().filter(|myself| !set.contains(myself)) {
                set.push(myself.clone());
            }
            proxies.send_replace(set);
            self.known.store(true, Ordering::Release);
        }
    }
}

/// Serve HTTPS on `addr`. With TLS configured, HTTP never falls back to
/// plaintext: bearer tokens travel in its headers.
#[cfg(feature = "boringssl")]
fn add_https<A>(service: &mut Service<A>, addr: &str, certificate: &Path, key: &Path) {
    if let Err(error) = service.add_tls(addr, &certificate.to_string_lossy(), &key.to_string_lossy()) {
        eprintln!("turbolay-proxy: HTTPS on {addr}: {error}");
        std::process::exit(2);
    }
}

#[cfg(not(feature = "boringssl"))]
fn add_https<A>(_service: &mut Service<A>, _addr: &str, _certificate: &Path, _key: &Path) {
    eprintln!("turbolay-proxy: TLS is configured but this build serves HTTP without it; build with the `boringssl` feature or set GRAPH_ALLOW_PLAINTEXT=true");
    std::process::exit(2);
}

fn proxy_set_source(set: &ProxySet) -> Box<dyn ProxySetSource> {
    match set {
        ProxySet::Static(peers) => Box::new(StaticProxySet::new(peers.clone())),
        ProxySet::File { path, interval } => Box::new(FileProxySet::new(path, *interval)),
        #[cfg(feature = "k8s")]
        ProxySet::K8s { namespace, service, port } => Box::new(turbolay_proxy_k8s::EndpointSliceProxySet {
            namespace: namespace.clone(),
            service: service.clone(),
            port: *port,
        }),
        #[cfg(not(feature = "k8s"))]
        ProxySet::K8s { .. } => {
            eprintln!("TURBOLAY_PROXY_SET=k8s needs a build with the `k8s` feature");
            std::process::exit(2);
        }
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = match Config::from_env() {
        Ok(config) => config,
        // `--help`, `--version` and flag errors print the way clap prints them.
        Err(error) if error.is::<clap::Error>() => error.downcast_ref::<clap::Error>().expect("checked").exit(),
        Err(error) => {
            eprintln!("turbolay-proxy: {error}");
            std::process::exit(2);
        }
    };

    let initial_proxies = match &config.proxy_set {
        ProxySet::Static(peers) => peers.clone(),
        _ => vec![config.advertised_bolt_addr.clone()],
    };
    let topology = Topology::new(config.nodes.clone(), initial_proxies);
    let (router, writer) = Router::new(
        topology.current(),
        RendezvousPlacement,
        RouterConfig {
            read_policy: config.read_policy,
            node_budget: config.node_budget,
            admit_wait: config.admit_wait,
            retry: RetryBudget {
                max_attempts: config.max_attempts,
            },
            ..RouterConfig::default()
        },
    );
    router
        .metrics()
        .registry()
        .register(Box::new(admin::ProcessCpu::default()))
        .expect("metric registered once");
    let bolt = Arc::new(BoltProxy::new(
        Arc::clone(&router),
        BoltConfig {
            base_database: config.base_database.clone(),
            root_scope: config.root_scope.clone(),
            cell_id: config.cell_id.clone(),
            token: config.token.as_str().into(),
            route_ttl_secs: config.route_ttl_secs,
            advertise: match &config.advertise_service {
                Some(service) => Advertise::Service(service.clone()),
                None => Advertise::Replicas,
            },
            self_address: config.advertised_bolt_addr.clone(),
            max_message_bytes: 16 * bytesize::MIB as usize,
            authentication_timeout: Duration::from_secs(10),
            // Admission holds this replica to `node_budget / replicas` units
            // per node, never more than `node_budget`, so checkout never
            // waits on the pool.
            pool: PoolConfig {
                max_per_node: config.node_budget,
                ..PoolConfig::default()
            },
        },
    ));
    let tls = match &config.tls {
        Some((certificate, key)) => match bolt_app::tls_acceptor(certificate, key) {
            Ok(acceptor) => Some(acceptor),
            Err(error) => {
                eprintln!("turbolay-proxy: TLS: {error}");
                std::process::exit(2);
            }
        },
        None => None,
    };

    let conf = ServerConf {
        threads: config.threads,
        work_stealing: true,
        grace_period_seconds: Some(1),
        graceful_shutdown_timeout_seconds: Some(5),
        upstream_keepalive_pool_size: config.node_budget.max(128) * config.nodes.len().max(1),
        ..ServerConf::default()
    };
    let mut server = Server::new_with_opt_and_conf(None, conf);
    server.bootstrap();

    let lifecycle = Lifecycle::default();
    let peers_known = Arc::new(AtomicBool::new(false));
    let mut bolt_service = Service::new(
        "turbolay-proxy bolt".to_string(),
        bolt_app::BoltApp {
            proxy: Arc::clone(&bolt),
            tls,
        },
    );
    bolt_service.add_tcp(&config.bolt_addr);

    let mut admin_service = Service::new(
        "turbolay-proxy admin".to_string(),
        admin::AdminApp {
            proxy: Arc::clone(&bolt),
            lifecycle: lifecycle.clone(),
            peers_known: Arc::clone(&peers_known),
        },
    );
    admin_service.add_tcp(&config.admin_addr);
    admin_service.threads = Some(1);

    let mut topology_service = background_service(
        "turbolay-proxy topology",
        TopologyTask {
            feed: Mutex::new(Some(Feed {
                topology,
                membership: Box::new(StaticMembership::new(config.nodes.clone())),
                proxies: Box::new(Reported {
                    source: proxy_set_source(&config.proxy_set),
                    known: Arc::clone(&peers_known),
                    // The chart advertises the pod IP, the same spelling
                    // the EndpointSlices list it under.
                    myself: matches!(config.proxy_set, ProxySet::K8s { .. }).then(|| config.advertised_bolt_addr.clone()),
                }),
                writer,
            })),
            lifecycle,
        },
    );
    topology_service.threads = Some(1);

    server.add_service(bolt_service);
    server.add_service(admin_service);
    server.add_service(topology_service);
    if config.http_enabled {
        let mut http_service = pingora_proxy::http_proxy_service(
            &server.configuration,
            HttpProxy::new(
                Arc::clone(&router),
                HttpConfig {
                    cell_id: config.cell_id.clone(),
                    allow_default_namespace: config.root_scope.is_default(),
                    upstream_h2: config.upstream_h2,
                    max_h2_streams: config.max_h2_streams,
                    ..HttpConfig::default()
                },
            ),
        );
        match &config.tls {
            Some((certificate, key)) => add_https(&mut http_service, &config.http_addr, certificate, key),
            None => http_service.add_tcp(&config.http_addr),
        }
        server.add_service(http_service);
    }
    tracing::info!(
        bolt = %config.bolt_addr,
        http = config.http_enabled.then_some(config.http_addr.as_str()),
        admin = %config.admin_addr,
        nodes = config.nodes.len(),
        read_policy = config.read_policy.as_str(),
        "turbolay-proxy starting"
    );
    server.run_forever();
}
