#[allow(dead_code)]
#[path = "graph_node/admin.rs"]
mod admin;
#[path = "graph_node/buddy_wal.rs"]
mod buddy_wal;
#[allow(dead_code)]
#[path = "graph_node/config.rs"]
mod config;
#[path = "graph_node/dial9_trace.rs"]
mod dial9_trace;
#[path = "graph_node/listeners.rs"]
mod listeners;
#[allow(dead_code)]
#[path = "graph_node/otel_metrics.rs"]
mod otel_metrics;
#[allow(dead_code)]
#[path = "graph_node/readiness.rs"]
mod readiness;
#[allow(dead_code)]
#[path = "graph_node/tls.rs"]
mod tls;
#[path = "graph_node/usage_report.rs"]
mod usage_report;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use config::RuntimeConfig;
use hydradb::{
    object_store_from_env, probe_create_only_puts, BoltServerConfig, ClientQueryService, ClientQueryServiceConfig, ClientQueryTarget,
    HierarchicalClientDatabaseResolver, HttpQueryServerConfig, ObjectStoreBoltRoutingTableProvider, ObjectStoreNodeDirectory,
    PlacementConfig, PlacementView, QueryTransportAction, QueryTransportScopeGrant, ScopedRoutedGraphCluster,
    StaticQueryTransportScopeAuthorizer, WriterGate,
};
use hydradb_placement::heartbeat::{delete_heartbeat, put_heartbeat, validate_node_id, Heartbeat};
use hydradb_placement::liveness::HeartbeatAction;
use hydradb_telemetry::{ServiceIdentity, TelemetryConfig};
use listeners::{ClientListenerAddrs, ClientListeners};
use readiness::NodeReadiness;
use slatedb::object_store::{path::Path, ObjectStore};

type RuntimeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const INDEXER_NOTIFY_TIMEOUT: Duration = Duration::from_secs(1);

/// Concurrent PUTs in the create-only probe: one run, 32 small PUTs.
const CREATE_ONLY_PROBE_CONTENDERS: usize = 32;
/// Backoff between create-only probes after a failure: doubling from the first
/// to the last, then repeating the last.
const CREATE_ONLY_PROBE_RETRY_FIRST: Duration = Duration::from_secs(5);
const CREATE_ONLY_PROBE_RETRY_LAST: Duration = Duration::from_secs(300);

/// Close `gate` and probe the store in the background until a probe passes,
/// then open it. The node boots and serves reads meanwhile; a slow or failing
/// store delays only writes and readiness, never the listeners. Returns the
/// probe task for shutdown to abort.
///
/// Each attempt uses its own key: a run id plus the attempt number, so neither
/// a restart nor a retry contends with an earlier probe's leftover object.
fn start_create_only_probe(
    object_store: Arc<dyn ObjectStore>,
    data_path: String,
    node_id: String,
    started_at: DateTime<Utc>,
    gate: WriterGate,
) -> tokio::task::JoinHandle<()> {
    gate.set_open(false);
    let run = started_at.timestamp_nanos_opt().unwrap_or_default();
    tokio::spawn(async move {
        let mut delay = CREATE_ONLY_PROBE_RETRY_FIRST;
        let mut attempt: u64 = 0;
        loop {
            let prefix = Path::from(format!("{data_path}/_create_only_probe/{node_id}-{run}-{attempt}"));
            match probe_create_only_puts(object_store.as_ref(), &prefix, CREATE_ONLY_PROBE_CONTENDERS).await {
                Ok(()) => {
                    gate.set_open(true);
                    tracing::info!(attempt, "object store passed the create-only probe: writers allowed");
                    return;
                }
                Err(error) if attempt == 0 => tracing::error!(
                    %error,
                    retry_in = ?delay,
                    "object store failed the create-only probe: serving reads only, not ready, retrying"
                ),
                Err(error) => tracing::warn!(%error, attempt, retry_in = ?delay, "create-only probe failed again"),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(CREATE_ONLY_PROBE_RETRY_LAST);
            attempt += 1;
        }
    })
}

/// Join the kernel's trace-context hook to the OpenTelemetry implementation.
///
/// This adapter exists here, in the binary, because it is the only place
/// entitled to name both sides. The kernel declares `TraceContextBridge` but
/// cannot implement it — a `tracing` span id is an internal subscriber handle,
/// not an OpenTelemetry trace id. `hydradb-telemetry` can perform both
/// operations but must not name the kernel's trait, because depending on the
/// kernel would reverse the arrow that keeps `opentelemetry-*` out of
/// `cargo test`. So neither library depends on the other and the composition
/// root does twelve lines of glue.
///
/// Without the `otlp` feature there is nothing to install: no exporter means no
/// OpenTelemetry ids, and a fabricated `traceparent` would join to nothing. The
/// kernel then sends no trace context and each node starts its own trace, which
/// is exactly the pre-5b behaviour.
#[cfg(feature = "otlp")]
fn install_trace_context_bridge() {
    struct Bridge;

    impl hydradb::TraceContextBridge for Bridge {
        fn current_traceparent(&self) -> Option<String> {
            hydradb_telemetry::bridge::current_traceparent()
        }

        fn adopt_remote_parent(&self, span: &tracing::Span, traceparent: &str) {
            hydradb_telemetry::bridge::adopt_remote_parent(span, traceparent);
        }
    }

    // Failure means one was already installed, which in a single `main` cannot
    // happen — and if it somehow did, the first one is as good as this one.
    if let Err(error) = hydradb::install_trace_context_bridge(&Bridge) {
        tracing::warn!(error, "trace context bridge was already installed");
    }
}

#[cfg(not(feature = "otlp"))]
fn install_trace_context_bridge() {}

fn main() -> RuntimeResult<()> {
    // Ahead of the subscriber on purpose: `--version` is asked from a shell,
    // usually `docker run --rm <image> --version` against an image nobody can
    // otherwise identify, and the answer has to be one plain block on stdout
    // rather than a JSON log line wrapped around it.
    if hydradb_telemetry::build_info::version_flag_requested() {
        println!(
            "{} {}",
            ServiceIdentity::GraphNode.binary(),
            hydradb_telemetry::BUILD_INFO.long_version()
        );
        return Ok(());
    }

    // The runtime `#[tokio::main]` would build, recorded by dial9 only when
    // `DIAL9_ENABLED` asks for it. `finish` runs after the node has stopped, so
    // the last trace segment is sealed and shipped on the way out.
    let runtime = dial9_trace::NodeRuntime::from_env()?;
    let result = runtime.block_on(serve());
    runtime.finish();
    result
}

async fn serve() -> RuntimeResult<()> {
    // First statement on the runtime: everything after it, including a config
    // error, is logged rather than lost. `init` is total — with no
    // `OTEL_EXPORTER_OTLP_ENDPOINT` set it installs the fmt layer alone, so a
    // missing collector is never why a node fails to boot.
    let telemetry_config = TelemetryConfig::from_env(ServiceIdentity::GraphNode);
    // Read off the config before `init` consumes it, because the metric
    // collection task and the SDK's `PeriodicReader` must use the *same*
    // number and this is the only place both can see it. Re-reading
    // `OTEL_METRIC_EXPORT_INTERVAL` further down would be a second parse of one
    // value, and the two parses differ: `TelemetryConfig` rejects a zero that
    // the SDK silently ignores.
    let metric_export_interval = telemetry_config.metric_export_interval;
    let telemetry = hydradb_telemetry::init(telemetry_config)?;
    install_trace_context_bridge();
    hydradb_telemetry::build_info::log();

    let result = boot(&telemetry, metric_export_interval).await;

    // Explicitly, and last. The guard flushes on drop too, but the ordering of
    // a destructor against the rest of `main` is easy to get wrong, and what
    // this flush protects is the final few seconds before a pod restart —
    // precisely the window that matters when diagnosing why it restarted.
    // Note it runs on the error path as well: a node that died during startup
    // is the case whose logs are least affordable to drop.
    telemetry.shutdown();
    result
}

/// Everything between the subscriber being installed and it being torn down.
///
/// Split out of `main` only so the shutdown above cannot be skipped by a `?`.
///
/// The guard is borrowed rather than moved because it is also what owns the
/// meter provider: the metric collection task registers its instruments against
/// `TelemetryGuard::providers()`, and the guard has to outlive that task so the
/// meter's final collection at shutdown happens after the task has stopped
/// publishing into it.
async fn boot(telemetry: &hydradb_telemetry::TelemetryGuard, metric_export_interval: Duration) -> RuntimeResult<()> {
    let config = RuntimeConfig::from_env()?;
    tracing::info!(scope = %config.scope, "starting graph node");
    run_node(config, telemetry, metric_export_interval).await
}

async fn run_node(
    config: RuntimeConfig,
    telemetry: &hydradb_telemetry::TelemetryGuard,
    metric_export_interval: Duration,
) -> RuntimeResult<()> {
    let started_at = Utc::now();
    // Rejected here rather than at the first PUT: an id the object layer cannot
    // name is a node that runs, serves, and never appears in anyone's live set —
    // a failure with no symptom other than a warning every heartbeat interval.
    validate_node_id(&config.node_id)?;
    std::fs::create_dir_all(&config.data_cache_dir)?;
    let remote_store = object_store_from_env(None)?;
    let token = config.read_auth_token()?;
    let mut open_options = config.graph_open_options();
    // One disk cache for every scope, with the node's whole budget, instead of
    // one per scope with a fixed slice of it: the scopes read through
    // `object_store`, and nothing else on the node does anything a cache
    // would change (placement, leases and fencing see only manifests and
    // conditional writes, which pass straight through).
    let shared_disk_cache = hydradb::SharedDiskCache::take(Arc::clone(&remote_store), &mut open_options.cache).await?;
    let object_store = shared_disk_cache
        .as_ref()
        .map_or_else(|| Arc::clone(&remote_store), hydradb::SharedDiskCache::object_store);
    // The replica side of buddy WAL: follow peers' feeds so this node's readers
    // catch up from their writers' frontiers instead of polling object storage.
    // Peers serve the feed on their public HTTP listener, so it travels over
    // the same TLS as client queries whenever the node has TLS.
    // One set of buddy WAL metrics for the node: the feed it serves, the peers
    // it follows, its stores' readers, and /metrics all share it.
    let buddy_wal_metrics = config.buddy_wal.then(|| Arc::new(hydradb_buddy_wal::BuddyWalMetrics::new()));
    let buddy_wal_followers = if let Some(metrics) = &buddy_wal_metrics {
        let transport = match &config.tls_ca_certificate {
            Some(ca_certificate) => buddy_wal::PeerTransport::Tls { ca_certificate },
            None => buddy_wal::PeerTransport::Plaintext,
        };
        buddy_wal::start_replica(&config.buddy_wal_peers(), &token, &transport, Arc::clone(metrics)).map(|(feed, followers)| {
            open_options.wal_feed = Some(feed);
            followers
        })
    } else {
        None
    };
    let memory_config = config.graph_memory_config();
    let directory = ObjectStoreNodeDirectory::new(config.cells.iter().cloned(), config.bolt_node_addresses.keys().cloned())?;
    // One placement handle for the whole process, cloned into the routed
    // cluster and the Bolt routing provider. A second handle over the same
    // store would be a second live set, and rendezvous only converges while
    // every reader answers from the same one — see `engine::placement`.
    let placement = PlacementView::from_directory(
        config.node_id.clone(),
        &directory,
        PlacementConfig {
            heartbeat_interval: config.heartbeat_interval,
            heartbeat_timeout: config.heartbeat_timeout,
        },
    )?;
    // Refreshed before the listeners open, so the first request cannot race the
    // task's first tick and be answered from the assumed-fleet grace view.
    let placement_base = Path::from(config.data_path.as_str());
    let _ = placement.refresh(object_store.as_ref(), &placement_base).await;
    let placement_refresh = placement.spawn_refresh(Arc::clone(&object_store), placement_base);
    let indexer_change_wake = config.indexer_notify_url.as_ref().map(|_| Arc::new(tokio::sync::Notify::new()));
    let mut node = ScopedRoutedGraphCluster::new_with_writer_lease_duration(
        config.data_path.clone(),
        config.scope.namespace.clone(),
        config.scope.graph_id.clone(),
        config.node_id.clone(),
        directory.clone(),
        placement.clone(),
        Arc::clone(&object_store),
        open_options,
        memory_config,
        config.max_open_scopes,
        config.writer_lease_duration,
    )?;
    if let Some(wake) = &indexer_change_wake {
        node = node.with_indexer_change_wake(Arc::clone(wake));
    }
    if let Some(cache) = shared_disk_cache {
        node = node.with_shared_disk_cache(cache);
    }
    let node = Arc::new(node);
    // Before any listener opens: WAL fencing needs create-only PUTs, and a store
    // that ignores them loses acknowledged writes on a takeover without any
    // error. Until the store passes the probe this node serves reads, takes no
    // writer, and is not ready (`/readyz`, heartbeat): all three read the one
    // gate, closed here and opened by the background probe.
    let writer_gate = node.writer_gate();
    let storage_probe = start_create_only_probe(
        Arc::clone(&object_store),
        config.data_path.clone(),
        config.node_id.clone(),
        started_at,
        writer_gate.clone(),
    );
    let indexer_notifier = match (config.indexer_notify_url.clone(), indexer_change_wake) {
        (Some(url), Some(wake)) => Some(start_indexer_change_notifier(url, wake, token.clone())?),
        _ => None,
    };
    let (writer_reconcile_stop, writer_reconcile_task) = start_writer_ownership_reconciler(Arc::clone(&node), config.heartbeat_interval);
    let (writer_lease_stop, writer_lease_task) =
        start_writer_lease_reconciler(Arc::clone(&node), (config.writer_lease_duration / 3).max(Duration::from_secs(1)));
    let (index_discovery_stop, index_discovery_task) =
        start_index_discovery(Arc::clone(&node), config.cells.clone(), config.index_discovery_interval);

    let authorizer = StaticQueryTransportScopeAuthorizer::new().with_bearer_grant(
        token.clone(),
        QueryTransportScopeGrant::graph_namespace(
            config.scope.namespace.clone(),
            config.scope.graph_id.clone(),
            true,
            [
                QueryTransportAction::Read,
                QueryTransportAction::Write,
                QueryTransportAction::Cancel,
                QueryTransportAction::Admin,
            ],
        ),
    )?;
    #[cfg(feature = "query-telemetry")]
    let failure_reporter = if let Some(url) = &config.query_telemetry_url {
        let token = config
            .query_telemetry_token_file
            .as_ref()
            .map(std::fs::read_to_string)
            .transpose()?;
        Some(hydradb::QueryFailureReporter::new(url.clone(), token.as_deref())?)
    } else {
        None
    };
    let service_config = ClientQueryServiceConfig::default()
        .with_required_bearer_token(token.clone())
        .with_scope_authorizer(Arc::new(authorizer))
        .with_max_concurrent_queries(config.max_concurrent_queries)
        .with_max_query_runtime_ms(config.max_query_runtime_ms)
        .with_server_cursor_limits(
            config.max_server_cursors,
            config.max_cursor_buffer_bytes,
            config.cursor_ttl.as_millis().try_into()?,
        )
        .with_cypher_engine(config.cypher_engine)
        .with_mutation_atomicity_policy(config.mutation_atomicity_policy);
    #[cfg(feature = "query-telemetry")]
    let service_config = match &failure_reporter {
        Some(reporter) => service_config.with_failure_reporter(config.node_id.clone(), reporter.clone()),
        None => service_config,
    };
    let service = ClientQueryService::new(Arc::clone(&node) as Arc<dyn hydradb::QueryCellClient>, service_config)?;
    let target = ClientQueryTarget::new(config.scope.clone(), config.cell_id.clone())?;
    let resolver = Arc::new(HierarchicalClientDatabaseResolver::new(config.database.clone(), target)?);
    let tls_reloader = if config.allow_plaintext {
        None
    } else {
        Some(tls::FileTlsReloader::start(
            config.tls_certificate.as_ref().expect("validated TLS certificate"),
            config.tls_private_key.as_ref().expect("validated TLS private key"),
            Duration::from_secs(1),
        )?)
    };
    let tls_provider = tls_reloader.as_ref().map(tls::FileTlsReloader::provider);

    let mut bolt_config = BoltServerConfig::new(resolver)
        .with_default_database(config.database.clone())
        .with_max_connections(config.max_bolt_connections)
        .with_authentication_timeout(config.bolt_authentication_timeout)
        .with_idle_timeout(config.bolt_idle_timeout)
        .with_max_connection_age(config.bolt_max_connection_age)
        .with_graceful_shutdown_timeout(config.graceful_shutdown_timeout);
    // No `/readyz` fan-out any more (decision 4): readiness rides the heartbeat
    // the publisher below writes, and the routing table is derived from the same
    // live set `ensure_local_writer` enforces.
    //
    // `GRAPH_READ_ROUTING` decides whether `READ` names the whole live fleet or
    // just the cell's owner. Default `fleet`, i.e. inert, so the flip is an env
    // change per environment rather than a fleet-wide behaviour change carried
    // by an image (the cell-affine read routing design, change 2).
    let routing = ObjectStoreBoltRoutingTableProvider::new(config.bolt_node_addresses.clone(), 30, placement.clone())?
        .with_read_routing(config.read_routing)
        .with_writer_lease_directory(node.writer_lease_directory());
    bolt_config = bolt_config.with_routing_table_provider(Arc::new(routing));

    let mut http_config = HttpQueryServerConfig::default()
        .with_graceful_shutdown_timeout(config.graceful_shutdown_timeout)
        .with_default_page_size(config.default_page_size);
    if config.scope.is_default() {
        http_config = http_config.allow_default_namespace();
    }
    let buddy_wal_feed = buddy_wal_metrics
        .as_ref()
        .map(|metrics| buddy_wal::feed_routes(&node, &token, Arc::clone(metrics)));
    let listeners = ClientListeners::bind(
        ClientListenerAddrs::from(&config),
        &service,
        bolt_config,
        http_config,
        tls_provider,
        move |stopping| buddy_wal_feed.map_or_else(axum::Router::new, |feed| feed(stopping)),
    )
    .await?;

    // One readiness answer for `/readyz` and the publisher, and it accounts for
    // placement: a node that has shed its view of the fleet (decision 7) is not
    // ready and must withdraw its heartbeat, or it stays the computed owner for
    // every peer while refusing every write they route to it.
    let ready = NodeReadiness::new(placement.clone()).with_writer_gate(writer_gate);
    let admin = admin::AdminServer::bind_scoped(
        config.admin_addr,
        ready.clone(),
        service.clone(),
        Arc::clone(&node),
        token,
        buddy_wal_metrics.clone(),
    )
    .await?;
    ready.mark_ready();
    // The OTel half of the same numbers `/metrics` serves. Inert without the
    // `otlp` feature and inert with it when no collector endpoint is
    // configured, so the ordinary build spawns nothing and takes no lock.
    let metric_collection = otel_metrics::MetricCollection::start(telemetry, metric_export_interval, service.clone(), Arc::clone(&node));
    // Resolve the install ID in the background so store retries cannot delay startup.
    let usage_reporter = match &config.usage_report_url {
        Some(url) => Some(usage_report::UsageReporter::start(usage_report::ReporterInputs {
            url: url.clone(),
            interval: config.usage_report_interval,
            store: Arc::clone(&object_store),
            data_path: config.data_path.clone(),
            process_started_unix_ms: u64::try_from(started_at.timestamp_millis()).unwrap_or_default(),
            service: service.clone(),
            node: Arc::clone(&node),
            placement: placement.clone(),
            cells: config.cells.len(),
        })?),
        None => None,
    };
    let (heartbeat_stop, heartbeat_task) = start_heartbeat_publisher(
        Arc::clone(&object_store),
        Path::from(config.data_path.as_str()),
        config.node_id.clone(),
        config.cells.clone(),
        ready.clone(),
        started_at,
        config.heartbeat_interval,
    );
    tracing::info!(
        node_id = %config.node_id,
        bolt_addr = %listeners.bolt_addr(),
        http_addr = %listeners.http_addr(),
        admin_addr = %admin.local_addr(),
        // Absent from the line when unset.
        internal_bolt_addr = listeners.internal_bolt_addr().map(tracing::field::display),
        internal_http_addr = listeners.internal_http_addr().map(tracing::field::display),
        "graph node listeners started"
    );

    tokio::select! {
        result = shutdown_signal() => result?,
    }
    ready.mark_unready();
    storage_probe.abort();
    // Before anything drains. The publisher's last act is to DELETE this node's
    // heartbeat, and awaiting it here is what makes a graceful restart hand the
    // node's cells over immediately instead of costing peers a full
    // `heartbeat_timeout` to notice — decision 4.
    let _ = heartbeat_stop.send(true);
    // Bounded, because a hung object-store request must not hold the drain open.
    // Giving up on the withdrawal costs peers one `heartbeat_timeout` to notice —
    // exactly the hard-crash bound decision 4 already accepts — whereas waiting
    // forever would cost the whole shutdown.
    match tokio::time::timeout(config.heartbeat_interval, heartbeat_task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(error = %error, "heartbeat publisher task failed"),
        Err(_) => tracing::warn!(
            node_id = %config.node_id,
            "timed out withdrawing the heartbeat; peers will age this node out"
        ),
    }
    // Nothing left to route or promote, so the view need not be refreshed while
    // the listeners drain.
    placement_refresh.abort();
    admin.stop().await?;
    listeners.stop().await?;
    if let Some(followers) = buddy_wal_followers {
        followers.stop().await;
    }
    #[cfg(feature = "query-telemetry")]
    if let Some(reporter) = &failure_reporter {
        reporter.shutdown(Duration::from_secs(5)).await;
    }
    if let Some(tls_reloader) = tls_reloader {
        tls_reloader.stop().await;
    }
    let _ = index_discovery_stop.send(true);
    index_discovery_task.await??;
    let _ = writer_reconcile_stop.send(true);
    writer_reconcile_task.await??;
    let _ = writer_lease_stop.send(true);
    writer_lease_task.await??;
    if let Some((stop, task)) = indexer_notifier {
        let _ = stop.send(true);
        task.await??;
    }
    // After the listeners have drained, so the final usage report counts every
    // query this process served; before `drop(service)` and the `try_unwrap`
    // below, for the same reason as the metric collection.
    if let Some(usage_reporter) = usage_reporter {
        usage_reporter.stop().await;
    }
    // Before `drop(service)` and before the `try_unwrap` below: the task holds
    // a clone of both, so a collection still in flight would turn a clean
    // shutdown into "graph node still has active runtime references".
    metric_collection.stop().await;
    drop(service);
    let node = Arc::try_unwrap(node).map_err(|_| "graph node still has active runtime references")?;
    node.close().await?;
    tracing::info!(node_id = %config.node_id, "graph node stopped");
    Ok(())
}

fn start_indexer_change_notifier(
    url: String,
    wake: Arc<tokio::sync::Notify>,
    bearer_token: String,
) -> RuntimeResult<(tokio::sync::watch::Sender<bool>, tokio::task::JoinHandle<RuntimeResult<()>>)> {
    let url = reqwest::Url::parse(&url)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("GRAPH_INDEXER_NOTIFY_URL must use http or https".into());
    }
    let client = reqwest::Client::builder().timeout(INDEXER_NOTIFY_TIMEOUT).build()?;
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return Ok(());
                    }
                }
                _ = wake.notified() => {
                    match client
                        .post(url.clone())
                        .bearer_auth(&bearer_token)
                        .send()
                        .await
                    {
                        Ok(response) if response.status().is_success() => {}
                        Ok(response) => tracing::debug!(
                            status = %response.status(),
                            "indexer wake was rejected; durable polling will recover"
                        ),
                        Err(error) => tracing::debug!(
                            %error,
                            "indexer wake failed; durable polling will recover"
                        ),
                    }
                }
            }
        }
    });
    Ok((stop_tx, task))
}

fn start_writer_lease_reconciler(
    node: Arc<ScopedRoutedGraphCluster>,
    interval: Duration,
) -> (tokio::sync::watch::Sender<bool>, tokio::task::JoinHandle<RuntimeResult<()>>) {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return Ok(());
                    }
                }
                _ = ticker.tick() => {
                    for failure in node.renew_writer_leases().await {
                        tracing::warn!(
                            scope = %failure.scope,
                            cell_id = %failure.cell_id,
                            ownership_lost = failure.ownership_lost,
                            error = %failure.error,
                            "writer lease renewal failed"
                        );
                    }
                }
            }
        }
    });
    (stop_tx, task)
}

/// Publish this node's heartbeat while it is ready, withdraw it when it is not.
///
/// ```text
/// every heartbeat_interval:   ready?  -> PUT    <base>/_graph_nodes/v1/<id>
///                             not?    -> DELETE <base>/_graph_nodes/v1/<id>
/// SIGTERM:                            -> DELETE <base>/_graph_nodes/v1/<id>, then drain
/// ```
///
/// Decision 4 of the rendezvous placement design. **Presence of
/// the object is the readiness signal**, not merely a liveness one: it replaces
/// the per-caller `/readyz` fan-out that routing used to run, so a node that is
/// up but unready must leave *no* object behind. That is why the unready branch
/// DELETEs instead of skipping the PUT — a stale object would keep advertising a
/// node that is not serving until it aged out. This is the one place the loop
/// diverges from the heartbeat publisher it was adapted from, which publishes
/// unconditionally.
///
/// "Ready" is [`NodeReadiness`], the same value `/readyz` reports, so the two
/// answers cannot drift apart — and it folds in decision 7's obligation: a node
/// that has shed its view of the fleet withdraws even though its lifecycle is
/// healthy. See `graph_node/readiness.rs` for why that pairing is not optional.
///
/// **Every store failure is a `warn!` and nothing more.** A node whose PUTs fail
/// ages out of every peer's live set, which is the correct outcome and needs no
/// help; a node that exited because it could not write a heartbeat would turn a
/// transient object-store blip into an outage.
fn start_heartbeat_publisher(
    object_store: Arc<dyn ObjectStore>,
    base_path: Path,
    node_id: String,
    cells: Vec<String>,
    ready: NodeReadiness,
    started_at: DateTime<Utc>,
    interval: Duration,
) -> (tokio::sync::watch::Sender<bool>, tokio::task::JoinHandle<()>) {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            publish_heartbeat(
                object_store.as_ref(),
                &base_path,
                &node_id,
                &cells,
                started_at,
                ready.heartbeat_action(),
            )
            .await;
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(interval) => {}
            }
        }
        // The withdrawal that makes a graceful restart instant. It runs on the
        // way out even if the loop above just published, because `ready` is
        // cleared and the stop signal sent in the same breath and the ordering
        // between them is not worth relying on; `delete_heartbeat` is idempotent.
        withdraw_heartbeat(object_store.as_ref(), &base_path, &node_id).await;
    });
    (stop_tx, task)
}

async fn publish_heartbeat(
    store: &dyn ObjectStore,
    base_path: &Path,
    node_id: &str,
    cells: &[String],
    started_at: DateTime<Utc>,
    action: HeartbeatAction,
) {
    if matches!(action, HeartbeatAction::Withdraw) {
        withdraw_heartbeat(store, base_path, node_id).await;
        return;
    }
    // The publisher is the one place that legitimately reads a clock — decision
    // 10 keeps every function below it pure, so the two timestamps are passed
    // inward rather than read there. Both are this node's own clock and are
    // observability only: liveness is the object's `LastModified`.
    // The commit, not just the package version. A heartbeat is the only place
    // the *live set* records what each node is running, so this is what answers
    // "is every node on the same build?" during a rolling deploy — a question
    // `CARGO_PKG_VERSION` could never answer, since it reads `0.1.0` on both
    // sides of every upgrade this repository has ever shipped.
    let body = Heartbeat::new(
        node_id,
        hydradb_telemetry::BUILD_INFO.version_string(),
        started_at,
        Utc::now(),
        cells.to_vec(),
    );
    if let Err(error) = put_heartbeat(store, base_path, &body).await {
        tracing::warn!(node_id, error = %error, "failed to publish heartbeat");
    }
}

async fn withdraw_heartbeat(store: &dyn ObjectStore, base_path: &Path, node_id: &str) {
    if let Err(error) = delete_heartbeat(store, base_path, node_id).await {
        tracing::warn!(node_id, error = %error, "failed to withdraw heartbeat");
    }
}

fn start_index_discovery(
    node: Arc<ScopedRoutedGraphCluster>,
    cells: Vec<String>,
    interval: Duration,
) -> (tokio::sync::watch::Sender<bool>, tokio::task::JoinHandle<RuntimeResult<()>>) {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            // `loaded_clusters` hands back `Weak`s, and they must be upgraded one
            // at a time with each `Arc` dropped before the next. Collecting the
            // upgrades into a `Vec` first would pin every open scope for the whole
            // sweep, and because eviction only considers entries with
            // `Arc::strong_count == 1`, that made a query for a not-yet-open scope
            // fail with `AdmissionRejected` at `max_open_scopes` — this loop does
            // real object-store I/O per cell, so the window was wide.
            for cluster in node.loaded_clusters().await {
                // Gone means the scope was evicted since the snapshot. Skip it;
                // the next sweep sees whatever is open then.
                let Some(cluster) = cluster.upgrade() else {
                    continue;
                };
                for cell_id in &cells {
                    let shard = cluster.shard(cell_id)?;
                    match shard.dirty_graph_index_edge_types(cell_id).await {
                        Ok(edge_types) => {
                            for (edge_type, _) in edge_types {
                                if let Err(error) = shard.discover_graph_index(cell_id, &edge_type).await {
                                    tracing::warn!(
                                        scope = %cluster.scope(),
                                        cell_id,
                                        edge_type,
                                        error = %error,
                                        "failed to discover graph index generation"
                                    );
                                }
                            }
                        }
                        Err(error) => tracing::warn!(
                            scope = %cluster.scope(),
                            cell_id,
                            error = %error,
                            "failed to discover dirty graph index edge types"
                        ),
                    }
                }
            }
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(interval.max(Duration::from_millis(100))) => {}
            }
        }
    });
    (stop_tx, task)
}

fn start_writer_ownership_reconciler(
    node: Arc<ScopedRoutedGraphCluster>,
    interval: Duration,
) -> (tokio::sync::watch::Sender<bool>, tokio::task::JoinHandle<RuntimeResult<()>>) {
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(100)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        return Ok(());
                    }
                }
                _ = ticker.tick() => {
                    match node.retire_unowned_writers().await {
                        Ok(0) => {}
                        Ok(retired) => tracing::info!(
                            retired_scopes = retired,
                            "retired writers after placement ownership changed"
                        ),
                        Err(error) => tracing::warn!(
                            error = %error,
                            "failed to retire writers after placement ownership changed"
                        ),
                    }
                }
            }
        }
    });
    (stop_tx, task)
}

async fn shutdown_signal() -> RuntimeResult<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
