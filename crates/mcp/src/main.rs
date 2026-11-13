mod local;

use std::{env, error::Error, time::Duration};

use hydradb_scope::{encode_scope_namespace_id, NamespacePath};
use reqwest::{header, Client, Url};
use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerConfig},
    schemars, tool, tool_handler, tool_router, ServerHandler, ServiceExt,
};
use serde::Deserialize;
use serde_json::{json, Value};

type AppResult<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ExecuteQuery {
    /// Raw Cypher query. Reads and mutations are supported.
    query: String,
    /// Tenant name, encoded in the same way as a scoped Bolt database name.
    tenant: String,
    /// Subtenant name within the tenant.
    subtenant: String,
    /// Bound Cypher parameters. Values are passed to the HTTP adapter unchanged.
    #[serde(default)]
    parameters: serde_json::Map<String, Value>,
    /// Maximum rows in this page. Omit to use the node's default.
    page_size: Option<usize>,
    /// query_id from the previous page. Required with cursor to retain cursor ownership.
    query_id: Option<String>,
    /// Opaque next_cursor from the previous page. Reuse the same query and parameters.
    cursor: Option<u64>,
}

#[derive(Clone)]
struct QueryServer {
    client: Client,
    endpoint: Url,
    namespace: NamespacePath,
    cell: String,
}

#[tool_router]
impl QueryServer {
    #[tool(
        description = "Execute Cypher in a Turbolay tenant and subtenant. Supports reads and mutations with bound parameters. Returns one HTTP page including query_id and next_cursor; pass both with the same query, scope, parameters and page_size to continue. The first write creates the scope. Names use the same scope encoding as Bolt.",
        annotations(destructive_hint = true, read_only_hint = false, idempotent_hint = false)
    )]
    async fn execute_query(&self, Parameters(args): Parameters<ExecuteQuery>) -> CallToolResult {
        match self.execute(args).await {
            Ok(value) => CallToolResult::success(vec![ContentBlock::text(value.to_string())]),
            Err(error) => CallToolResult::error(vec![ContentBlock::text(error.to_string())]),
        }
    }
}

impl QueryServer {
    fn from_env() -> AppResult<Self> {
        Self::new(
            &setting("TURBOLAY_HTTP_URL", "http://127.0.0.1:18443"),
            std::fs::read_to_string(env::var("GRAPH_AUTH_TOKEN_FILE")?)?.trim(),
            &setting("GRAPH_NAMESPACE", "default"),
            &setting("GRAPH_ID", "default"),
            setting("GRAPH_CELL_ID", "cell-0"),
        )
    }

    fn new(url: &str, token: &str, namespace: &str, graph: &str, cell: String) -> AppResult<Self> {
        let mut endpoint = Url::parse(url)?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            return Err("TURBOLAY_HTTP_URL must be an HTTP or HTTPS URL".into());
        }
        endpoint.set_query(None);
        endpoint.set_fragment(None);
        endpoint
            .path_segments_mut()
            .map_err(|_| "invalid HTTP URL")?
            .pop_if_empty()
            .extend(["v1", "graphs", graph, "query"]);
        if token.len() < 32 {
            return Err("GRAPH_AUTH_TOKEN_FILE must contain at least 32 characters".into());
        }
        let mut authorization = header::HeaderValue::from_str(&format!("Bearer {token}"))?;
        authorization.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, authorization);
        let client = Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            client,
            endpoint,
            namespace: NamespacePath::parse(namespace)?,
            cell,
        })
    }

    async fn execute(&self, args: ExecuteQuery) -> AppResult<Value> {
        if args.query.trim().is_empty() {
            return Err("query must not be empty".into());
        }
        let namespace = self
            .namespace
            .child(encode_scope_namespace_id("tenant", &args.tenant)?)?
            .child(encode_scope_namespace_id("subtenant", &args.subtenant)?)?;
        let mut response = self
            .client
            .post(self.endpoint.clone())
            .header("X-Graph-Namespace", namespace.to_string())
            .header("X-HydraDB-Caller-Step", "mcp.execute_query")
            .header(header::ACCEPT, "application/json")
            .json(&json!({
                "cell_id": self.cell, "query": args.query, "parameters": args.parameters,
                "page_size": args.page_size, "cursor": args.cursor, "query_id": args.query_id,
            }))
            .send()
            .await
            .map_err(|error| error.without_url())?;
        let body = self.read_response(&mut response).await?;
        Ok(serde_json::from_str(&body)?)
    }

    async fn read_response(&self, response: &mut reqwest::Response) -> AppResult<String> {
        let status = response.status();
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| error.without_url())? {
            if body.len() + chunk.len() > 16 * 1024 * 1024 {
                return Err("query response exceeds 16 MiB; narrow the query or add LIMIT".into());
            }
            body.extend_from_slice(&chunk);
        }
        let body = String::from_utf8(body)?;
        if !status.is_success() {
            return Err(format!("Turbolay HTTP {status}: {body}").into());
        }
        Ok(body)
    }
}

#[tool_handler]
impl ServerHandler for QueryServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("hydradb-mcp", env!("CARGO_PKG_VERSION")))
            .with_instructions("Use execute_query to run Cypher with parameters in a tenant and subtenant. The first write creates the scope. Results contain one page; continue with next_cursor as cursor, the returned query_id, and the same query, parameters, scope and page_size. Mutations change stored data. Failed or disconnected mutations may have committed; check the data before retrying.")
    }
}

fn setting(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn main() -> AppResult<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let result = runtime.block_on(run());
    // Tokio's stdin reader can stay blocked after SIGTERM while a client keeps
    // the pipe open. Children are stopped before ending the runtime here.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

async fn run() -> AppResult<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args == ["--help"] {
        eprintln!("hydradb-mcp [--local DATA_DIRECTORY]\nMCP over stdio. --local starts sibling graph-node and graph-indexer binaries.\nExisting node: set TURBOLAY_HTTP_URL, GRAPH_AUTH_TOKEN_FILE, GRAPH_NAMESPACE, GRAPH_ID, GRAPH_CELL_ID.");
        return Ok(());
    }
    let mut stack = match args.as_slice() {
        [] => None,
        [flag, path] if flag == "--local" => tokio::select! {
            result = local::Stack::start(path) => Some(result?),
            result = shutdown_signal() => { result?; return Ok(()); }
        },
        _ => return Err("usage: hydradb-mcp [--local DATA_DIRECTORY]".into()),
    };
    let query_server = match &stack {
        Some(stack) => stack.server.clone(),
        None => QueryServer::from_env()?,
    };
    let result: AppResult<()> = tokio::select! {
        result = async {
            let server = query_server.serve(rmcp::transport::stdio()).await?;
            server.waiting().await?;
            Ok(())
        } => result,
        result = shutdown_signal() => result,
        result = async {
            match stack.as_mut() {
                Some(stack) => stack.wait().await,
                None => std::future::pending().await,
            }
        } => result,
    };
    if let Some(stack) = stack.as_mut() {
        stack.stop().await;
    }
    result
}

async fn shutdown_signal() -> AppResult<()> {
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
