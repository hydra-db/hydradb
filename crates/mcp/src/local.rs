use std::{collections::BTreeMap, env, fs, io::Write, path::Path, process::Stdio, time::Duration};

use tokio::process::{Child, Command};

use crate::{setting, AppResult, QueryServer};

pub struct Stack {
    pub server: QueryServer,
    node: Child,
    indexer: Child,
    _token: tempfile::NamedTempFile,
}

impl Stack {
    pub async fn start(path: &str) -> AppResult<Self> {
        let root = Path::new(path);
        fs::create_dir_all(root)?;
        let root = fs::canonicalize(root)?;
        for name in ["store", "node-cache"] {
            fs::create_dir_all(root.join(name))?;
        }
        let mut token = tempfile::NamedTempFile::new_in(&root)?;
        let mut random = [0u8; 32];
        getrandom::fill(&mut random).map_err(|error| format!("could not generate local auth token: {error}"))?;
        let auth: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        writeln!(token, "{auth}")?;
        let bolt = setting("GRAPH_BOLT_ADDR", "127.0.0.1:28687");
        let http = setting("GRAPH_HTTP_ADDR", "127.0.0.1:28443");
        let admin = setting("GRAPH_ADMIN_ADDR", "127.0.0.1:29091");
        let indexer_admin = setting("GRAPH_INDEXER_ADMIN_ADDR", "127.0.0.1:29092");
        // Local mode deliberately sets its entire storage and routing configuration.
        let config: BTreeMap<String, String> = [
            ("CLOUD_PROVIDER", "local".to_string()),
            ("LOCAL_PATH", root.join("store").display().to_string()),
            ("GRAPH_DATA_PATH", "data".to_string()),
            ("GRAPH_NAMESPACE", setting("GRAPH_NAMESPACE", "default")),
            ("GRAPH_ID", setting("GRAPH_ID", "default")),
            ("GRAPH_CELL_ID", setting("GRAPH_CELL_ID", "cell-0")),
            ("GRAPH_CELLS", setting("GRAPH_CELL_ID", "cell-0")),
            ("GRAPH_NODE_ID", "node-mcp".to_string()),
            ("GRAPH_AUTH_TOKEN_FILE", token.path().display().to_string()),
            ("GRAPH_ALLOW_PLAINTEXT", "true".to_string()),
            ("GRAPH_DATA_CACHE_BYTES", "67108864".to_string()),
            ("GRAPH_DATA_CACHE_DIR", root.join("node-cache").display().to_string()),
            ("GRAPH_BOLT_ADDR", bolt.clone()),
            ("GRAPH_ADVERTISED_BOLT_ADDR", bolt.clone()),
            ("GRAPH_BOLT_NODE_ADDRESSES", format!("node-mcp={bolt}")),
            ("GRAPH_HTTP_ADDR", http.clone()),
            ("GRAPH_ADMIN_ADDR", admin.clone()),
            ("GRAPH_INDEXER_ADMIN_ADDR", indexer_admin.clone()),
            ("GRAPH_INDEXER_NOTIFY_URL", format!("http://{indexer_admin}/v1/changes:process")),
            ("RUST_MIN_STACK", "33554432".to_string()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect();
        let server = QueryServer::new(
            &format!("http://{http}"),
            &auth,
            &config["GRAPH_NAMESPACE"],
            &config["GRAPH_ID"],
            config["GRAPH_CELL_ID"].clone(),
        )?;
        let bin_dir = env::current_exe()?.parent().ok_or("missing binary directory")?.to_path_buf();
        let spawn = |name: &str| -> AppResult<Child> {
            Ok(Command::new(bin_dir.join(name))
                .envs(&config)
                .stdin(Stdio::null())
                .stdout(fs::File::create(root.join(format!("{name}.log")))?)
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()?)
        };
        let node = spawn("graph-node")?;
        let indexer = spawn("graph-indexer")?;
        let mut stack = Self {
            server,
            node,
            indexer,
            _token: token,
        };
        let client = reqwest::Client::builder().timeout(Duration::from_secs(1)).build()?;
        let ready = async {
            loop {
                if let Some(status) = stack.node.try_wait()? {
                    return Err(format!("graph-node exited {status}; see {}/graph-node.log", root.display()).into());
                }
                if let Some(status) = stack.indexer.try_wait()? {
                    return Err(format!("graph-indexer exited {status}; see {}/graph-indexer.log", root.display()).into());
                }
                let node_ready = client.get(format!("http://{admin}/readyz")).send().await;
                let indexer_ready = client.get(format!("http://{indexer_admin}/readyz")).send().await;
                if node_ready.is_ok_and(|r| r.status().is_success()) && indexer_ready.is_ok_and(|r| r.status().is_success()) {
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };
        match tokio::time::timeout(Duration::from_secs(60), ready).await {
            Ok(Ok(())) => {}
            result => {
                stack.stop().await;
                return match result {
                    Ok(Err(error)) => Err(error),
                    _ => Err(format!("local stack did not become ready; see logs in {}", root.display()).into()),
                };
            }
        }
        eprintln!("graph-node and graph-indexer ready; MCP on stdio; logs in {}", root.display());
        Ok(stack)
    }

    pub async fn wait(&mut self) -> AppResult<()> {
        tokio::select! {
            result = self.node.wait() => Err(format!("graph-node exited: {}", result?).into()),
            result = self.indexer.wait() => Err(format!("graph-indexer exited: {}", result?).into()),
        }
    }

    pub async fn stop(&mut self) {
        for child in [&mut self.indexer, &mut self.node] {
            #[cfg(unix)]
            if let Some(pid) = child.id() {
                // The unreaped child owns this PID until wait completes.
                unsafe {
                    libc::kill(pid as libc::pid_t, libc::SIGTERM);
                }
            }
            #[cfg(not(unix))]
            let _ = child.start_kill();
        }
        for child in [&mut self.indexer, &mut self.node] {
            if tokio::time::timeout(Duration::from_secs(10), child.wait()).await.is_err() {
                let _ = child.kill().await;
            }
        }
    }
}
