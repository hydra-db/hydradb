//! The Cypher engine kill switch, driven the way on-call drives it: `PUT` on
//! the admin server, then observed by a Bolt client on the same node.
//!
//! The observable is `EXPLAIN`. The experimental engine answers it with one
//! `plan` row rendering its physical plan; the legacy route has no such
//! answer. Both come back over the wire, so the client — not a service
//! accessor — says which engine ran.

use std::collections::HashMap;
use std::sync::Arc;

use boltr::client::BoltSession;
use boltr::types::{BoltDict, BoltValue};
use hydradb::{
    BoltServerConfig, ClientBoltServer, ClientQueryService, ClientQueryServiceConfig, ClientQueryTarget, CypherEngineMode, GraphId,
    GraphMemoryConfig, GraphOpenOptions, GraphScope, MutationAtomicityPolicy, NamespacePath, ObjectStoreNodeDirectory, PlacementConfig,
    PlacementView, QueryCellClient, QueryTransportAction, QueryTransportScopeGrant, ScopedRoutedGraphCluster, StaticClientDatabaseResolver,
    StaticQueryTransportScopeAuthorizer,
};
use slatedb::object_store::memory::InMemory;

use super::{AdminServer, CYPHER_ENGINE_ROUTE, CYPHER_MUTATION_ATOMICITY_ROUTE};
use crate::readiness::NodeReadiness;

const TOKEN: &str = "kill-switch-test-token-with-32-plus-chars";
const EXPLAIN_QUERY: &str = "EXPLAIN MATCH (n:Entity) RETURN n.entity_id AS id";

struct Node {
    bolt: hydradb::BoltServerHandle,
    admin: AdminServer,
    http: reqwest::Client,
}

impl Node {
    async fn start(configured: CypherEngineMode) -> Self {
        let directory = ObjectStoreNodeDirectory::new(["cell-a".to_string()], ["graph-node-0".to_string()]).expect("a one-cell directory");
        let placement =
            PlacementView::new("graph-node-0", ["graph-node-0".to_string()], PlacementConfig::default()).expect("a fleet of one");
        let node = Arc::new(
            ScopedRoutedGraphCluster::new(
                "graph/kill-switch",
                NamespacePath::default(),
                GraphId::default(),
                "graph-node-0",
                directory,
                placement.clone(),
                Arc::new(InMemory::new()),
                GraphOpenOptions::default(),
                GraphMemoryConfig::default(),
                4,
            )
            .expect("a routed cluster"),
        );
        let authorizer = StaticQueryTransportScopeAuthorizer::new()
            .with_bearer_grant(
                TOKEN,
                QueryTransportScopeGrant::graph(
                    GraphScope::default(),
                    [
                        QueryTransportAction::Read,
                        QueryTransportAction::Write,
                        QueryTransportAction::Cancel,
                    ],
                ),
            )
            .expect("a grant");
        let service = ClientQueryService::new(
            Arc::clone(&node) as Arc<dyn QueryCellClient>,
            ClientQueryServiceConfig::default()
                .with_required_bearer_token(TOKEN)
                .with_scope_authorizer(Arc::new(authorizer))
                .with_cypher_engine(configured),
        )
        .expect("a query service");
        let resolver = StaticClientDatabaseResolver::single(
            "default",
            ClientQueryTarget::new(GraphScope::default(), "cell-a").expect("a target"),
        )
        .expect("a resolver");
        let bolt = ClientBoltServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            service.clone(),
            BoltServerConfig::new(Arc::new(resolver)).insecure_allow_plaintext(),
        )
        .await
        .expect("a Bolt listener");
        let ready = NodeReadiness::new(placement);
        ready.mark_ready();
        let admin = AdminServer::bind_scoped("127.0.0.1:0".parse().unwrap(), ready, service, node, TOKEN.to_string(), None)
            .await
            .expect("an admin listener");
        let node = Self {
            bolt,
            admin,
            http: reqwest::Client::new(),
        };
        // Seeded through the legacy mutation path on either configuration:
        // mutations take the shared engine before read-engine dispatch.
        let mut session = node.session().await;
        let _ = session
            .run(
                "CREATE (a:Entity {id: 1, entity_id: 'alpha'})-[:RELATES]->\
                 (b:Entity {id: 2, entity_id: 'beta'})",
            )
            .await
            .expect("seed");
        session.close().await.expect("close");
        node
    }

    async fn session(&self) -> BoltSession {
        BoltSession::connect_basic(self.bolt.local_addr(), "neo4j", TOKEN)
            .await
            .expect("a Bolt session")
    }

    fn url(&self) -> String {
        format!("http://{}{CYPHER_ENGINE_ROUTE}", self.admin.local_addr())
    }

    fn mutation_atomicity_url(&self) -> String {
        format!("http://{}{CYPHER_MUTATION_ATOMICITY_ROUTE}", self.admin.local_addr())
    }

    async fn put(&self, body: serde_json::Value, token: Option<&str>) -> reqwest::Response {
        let mut request = self.http.put(self.url()).json(&body);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        request.send().await.expect("admin PUT")
    }

    async fn set(&self, mode: Option<&str>) -> serde_json::Value {
        let response = self.put(serde_json::json!({ "override": mode }), Some(TOKEN)).await;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        response.json().await.expect("state JSON")
    }

    async fn state(&self) -> serde_json::Value {
        self.http
            .get(self.url())
            .send()
            .await
            .expect("admin GET")
            .json()
            .await
            .expect("state JSON")
    }

    async fn metrics(&self) -> String {
        self.http
            .get(format!("http://{}/metrics", self.admin.local_addr()))
            .send()
            .await
            .expect("scrape")
            .text()
            .await
            .expect("scrape body")
    }

    async fn set_mutation_atomicity(&self, policy: Option<&str>, token: Option<&str>) -> reqwest::Response {
        let mut request = self
            .http
            .put(self.mutation_atomicity_url())
            .json(&serde_json::json!({"override": policy}));
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        request.send().await.expect("mutation atomicity PUT")
    }

    /// The client read histogram's `_count` for one engine, as a scrape sees it.
    async fn reads_recorded_for(&self, engine: &str) -> u64 {
        let needle = format!("graph_client_operation_read_duration_seconds_count{{cypher_engine=\"{engine}\"}} ");
        let metrics = self.metrics().await;
        metrics
            .lines()
            .find_map(|line| line.strip_prefix(&needle))
            .unwrap_or_else(|| panic!("no read series for {engine}:\n{metrics}"))
            .parse()
            .expect("a count")
    }

    /// Which engine a fresh statement runs on, as the Bolt client sees it.
    async fn engine_seen_by_client(&self) -> &'static str {
        let mut session = self.session().await;
        let seen = session_engine(&mut session).await;
        session.close().await.expect("close");
        seen
    }

    async fn stop(self) {
        self.admin.stop().await.expect("admin stop");
        self.bolt.stop().await.expect("bolt stop");
    }
}

/// The experimental engine's `EXPLAIN` row, if `rows` is one.
fn is_experimental_plan(rows: &[Vec<BoltValue>]) -> bool {
    matches!(
        rows,
        [row] if matches!(row.as_slice(), [BoltValue::String(plan)] if plan.contains("access=label-prefix"))
    )
}

async fn session_engine(session: &mut BoltSession) -> &'static str {
    let run = session.connection().run(EXPLAIN_QUERY, HashMap::new(), BoltDict::new()).await;
    if run.is_err() {
        session.connection().reset().await.expect("RESET");
        return "legacy";
    }
    let (rows, _) = session.connection().pull_all().await.expect("PULL");
    if is_experimental_plan(&rows) {
        "experimental"
    } else {
        "legacy"
    }
}

#[tokio::test]
async fn the_override_needs_the_node_token_and_a_well_formed_body() {
    let node = Node::start(CypherEngineMode::Legacy).await;
    let before = node.state().await;
    assert_eq!(before["configured"], "legacy");
    assert_eq!(before["override"], serde_json::Value::Null);
    assert_eq!(before["effective"], "legacy");

    let unauthenticated = node.put(serde_json::json!({"override": "legacy"}), None).await;
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);
    let wrong_token = node
        .put(serde_json::json!({"override": "legacy"}), Some("not-the-node-token"))
        .await;
    assert_eq!(wrong_token.status(), reqwest::StatusCode::UNAUTHORIZED);
    // A missing key must not read as "clear".
    let empty = node.put(serde_json::json!({}), Some(TOKEN)).await;
    assert_eq!(empty.status(), reqwest::StatusCode::BAD_REQUEST);
    let unknown = node.put(serde_json::json!({"override": "graphblas"}), Some(TOKEN)).await;
    assert_eq!(unknown.status(), reqwest::StatusCode::BAD_REQUEST);
    #[cfg(not(feature = "datafusion-cypher-engine"))]
    {
        let unavailable = node.put(serde_json::json!({"override": "datafusion"}), Some(TOKEN)).await;
        assert_eq!(unavailable.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        let body: serde_json::Value = unavailable.json().await.expect("error JSON");
        assert!(body["error"].as_str().unwrap().contains("datafusion-cypher-engine"));
    }
    assert_eq!(node.state().await, before);
    node.stop().await;
}

#[test]
fn datafusion_metric_fields_have_a_bounded_engine_label() {
    assert_eq!(super::cypher_engine_label("read_latency_datafusion"), Some("datafusion"));
    assert_eq!(super::cypher_engine_label("write_latency_datafusion"), Some("datafusion"));
    assert_eq!(
        super::cypher_engine_label("queries_failed_by_reason_datafusion"),
        Some("datafusion")
    );
}

#[cfg(feature = "datafusion-cypher-engine")]
#[tokio::test]
async fn a_compiled_datafusion_engine_can_be_selected_without_a_restart() {
    let node = Node::start(CypherEngineMode::Legacy).await;
    let selected = node.set(Some("datafusion")).await;
    assert_eq!(selected["effective"], "datafusion");
    assert_eq!(selected["datafusion_compiled"], true);
    assert_eq!(node.state().await["effective"], "datafusion");
    assert_eq!(node.set(None).await["effective"], "legacy");
    node.stop().await;
}

#[tokio::test]
async fn mutation_atomicity_override_is_validated_and_reported_with_bounded_labels() {
    let node = Node::start(CypherEngineMode::Legacy).await;
    let url = node.mutation_atomicity_url();
    let before: serde_json::Value = node
        .http
        .get(&url)
        .send()
        .await
        .expect("atomicity GET")
        .json()
        .await
        .expect("atomicity state");
    assert_eq!(before["configured"], MutationAtomicityPolicy::LegacyChunked.as_str());
    assert_eq!(before["override"], serde_json::Value::Null);
    assert_eq!(before["effective"], MutationAtomicityPolicy::LegacyChunked.as_str());

    assert_eq!(
        node.set_mutation_atomicity(Some("statement"), None).await.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        node.set_mutation_atomicity(Some("chunked"), Some(TOKEN)).await.status(),
        reqwest::StatusCode::BAD_REQUEST
    );

    let applied = node.set_mutation_atomicity(Some("statement"), Some(TOKEN)).await;
    assert_eq!(applied.status(), reqwest::StatusCode::OK);
    let applied: serde_json::Value = applied.json().await.expect("atomicity state");
    assert_eq!(applied["configured"], "legacy_chunked");
    assert_eq!(applied["override"], "statement");
    assert_eq!(applied["effective"], "statement");
    let engine_state = node.state().await;
    assert_eq!(engine_state["mutation_atomicity"]["effective"], "statement");
    let metrics = node.metrics().await;
    assert!(metrics.contains("graph_cypher_mutation_atomicity{configured=\"legacy_chunked\",effective=\"statement\"} 1"));

    let cleared = node.set_mutation_atomicity(None, Some(TOKEN)).await;
    assert_eq!(cleared.status(), reqwest::StatusCode::OK);
    let cleared: serde_json::Value = cleared.json().await.expect("atomicity state");
    assert_eq!(cleared["override"], serde_json::Value::Null);
    assert_eq!(cleared["effective"], "legacy_chunked");
    node.stop().await;
}

#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn roll_back_roll_forward_and_clear_are_seen_by_the_client() {
    // A canary configured experimental, rolled back and then cleared.
    let canary = Node::start(CypherEngineMode::Experimental).await;
    assert_eq!(canary.engine_seen_by_client().await, "experimental");
    let rolled_back = canary.set(Some("legacy")).await;
    assert_eq!(rolled_back["configured"], "experimental");
    assert_eq!(rolled_back["override"], "legacy");
    assert_eq!(rolled_back["effective"], "legacy");
    assert_eq!(canary.engine_seen_by_client().await, "legacy");
    let cleared = canary.set(None).await;
    assert_eq!(cleared["override"], serde_json::Value::Null);
    assert_eq!(cleared["effective"], "experimental");
    assert_eq!(canary.engine_seen_by_client().await, "experimental");
    canary.stop().await;

    // A node configured legacy, rolled forward and then cleared.
    let node = Node::start(CypherEngineMode::Legacy).await;
    assert_eq!(node.engine_seen_by_client().await, "legacy");
    let rolled_forward = node.set(Some("experimental")).await;
    assert_eq!(rolled_forward["effective"], "experimental");
    assert_eq!(node.engine_seen_by_client().await, "experimental");
    let metrics = node.metrics().await;
    assert!(
        metrics.contains("graph_cypher_engine{configured=\"legacy\",effective=\"experimental\"} 1"),
        "the override is not visible on /metrics"
    );
    assert!(
        !metrics.contains("graph_buddy_wal_"),
        "a node without buddy WAL exports none of its series"
    );
    let cleared = node.set(None).await;
    assert_eq!(cleared["effective"], "legacy");
    assert_eq!(node.engine_seen_by_client().await, "legacy");
    node.stop().await;
}

/// Bolt RUN prepares and PULL executes, which is the widest window a statement
/// is in flight on a connection. A flip inside it must not move the statement.
#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn an_in_flight_statement_keeps_the_engine_it_started_with() {
    let node = Node::start(CypherEngineMode::Experimental).await;
    let mut session = node.session().await;
    session
        .connection()
        .run(EXPLAIN_QUERY, HashMap::new(), BoltDict::new())
        .await
        .expect("RUN on the experimental engine");

    node.set(Some("legacy")).await;
    // Any statement admitted now is legacy...
    assert_eq!(node.engine_seen_by_client().await, "legacy");
    // ...but the one already running finishes on the experimental engine,
    // which is the only engine that answers EXPLAIN with its plan.
    let (rows, _) = session.connection().pull_all().await.expect("PULL after the flip");
    assert!(is_experimental_plan(&rows), "{rows:?}");
    // And the next statement on the same connection sees the flip.
    assert_eq!(session_engine(&mut session).await, "legacy");
    session.close().await.expect("close");
    node.stop().await;
}

/// A statement recorded before a flip stays where it was recorded. The client
/// latency families are one histogram per engine, so the label a scrape prints
/// comes from the population and not from the node's current engine — without
/// that, the flip below would hand every experimental statement this node ever
/// served to `legacy` at the next scrape, which is the one number the
/// legacy-versus-experimental comparison is read from.
#[cfg(feature = "experimental-cypher-engine")]
#[tokio::test]
async fn a_flip_leaves_the_latencies_already_recorded_on_their_engine() {
    let node = Node::start(CypherEngineMode::Experimental).await;
    assert_eq!(node.engine_seen_by_client().await, "experimental");
    let experimental_reads = node.reads_recorded_for("experimental").await;
    assert!(experimental_reads >= 1, "the statement was recorded");
    assert_eq!(node.reads_recorded_for("legacy").await, 0);

    node.set(Some("legacy")).await;

    // Nothing moved: the counts are exactly what they were before the flip,
    // and the engine that has run nothing is still a zero series rather than
    // an absent one.
    assert_eq!(node.reads_recorded_for("experimental").await, experimental_reads);
    assert_eq!(node.reads_recorded_for("legacy").await, 0);
    node.stop().await;
}

#[cfg(not(feature = "experimental-cypher-engine"))]
#[tokio::test]
async fn a_build_without_the_experimental_engine_refuses_the_roll_forward() {
    let node = Node::start(CypherEngineMode::Legacy).await;
    let refused = node.put(serde_json::json!({"override": "experimental"}), Some(TOKEN)).await;
    assert_eq!(refused.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = refused.json().await.expect("error JSON");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("experimental-cypher-engine")),
        "{body}"
    );
    assert_eq!(body["state"]["effective"], "legacy");
    assert_eq!(body["state"]["experimental_compiled"], false);
    assert_eq!(node.engine_seen_by_client().await, "legacy");

    // Rolling back and clearing still work on this build.
    assert_eq!(node.set(Some("legacy")).await["effective"], "legacy");
    assert_eq!(node.engine_seen_by_client().await, "legacy");
    assert_eq!(node.set(None).await["override"], serde_json::Value::Null);
    node.stop().await;
}
