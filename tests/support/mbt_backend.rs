//! Shared storage selection for the Quint Connect integration adapters.
//!
//! The default is deliberately process-local `InMemory`. Set
//! `GRAPH_MBT_BACKEND=minio` and `GRAPH_MBT_S3_ENV_FILE=/path/to/minio.env` to
//! load the S3-compatible backend through SlateDB's normal AWS configuration.

use std::{
    collections::BTreeMap,
    env, fs,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context, Result};
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb_graph_kernel::object_store_from_env;

static REPLAY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub struct MbtReplayStore {
    pub object_store: Arc<dyn ObjectStore>,
    pub graph_path: String,
}

pub struct MbtBackend {
    object_store: Arc<dyn ObjectStore>,
    base_prefix: String,
}

impl MbtBackend {
    pub fn from_env() -> Result<Self> {
        let backend = env::var("GRAPH_MBT_BACKEND").unwrap_or_else(|_| "memory".to_string());
        let base_prefix = validated_prefix(
            &env::var("GRAPH_MBT_PREFIX").unwrap_or_else(|_| "formal-mbt".to_string()),
        )?;

        let object_store = match backend.to_ascii_lowercase().as_str() {
            "memory" | "inmemory" => Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
            "minio" | "s3" => {
                let env_file = env::var("GRAPH_MBT_S3_ENV_FILE")
                    .context("GRAPH_MBT_S3_ENV_FILE is required when GRAPH_MBT_BACKEND=minio")?;
                let config = parse_env_file(&env_file)?;
                validate_minio_config(&config)?;
                object_store_from_env(Some(env_file))?
            }
            other => {
                bail!("unsupported GRAPH_MBT_BACKEND={other:?}; expected memory (default) or minio")
            }
        };

        Ok(Self {
            object_store,
            base_prefix,
        })
    }

    pub fn new_replay(&self, adapter: &str) -> Result<MbtReplayStore> {
        validate_component(adapter, "adapter")?;
        let sequence = REPLAY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        Ok(MbtReplayStore {
            object_store: Arc::clone(&self.object_store),
            graph_path: format!(
                "{}/{adapter}/replay-{}-{sequence}",
                self.base_prefix,
                std::process::id()
            ),
        })
    }
}

fn parse_env_file(path: &str) -> Result<BTreeMap<String, String>> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("read GRAPH_MBT_S3_ENV_FILE {path:?}"))?;
    let mut values = BTreeMap::new();
    for (line_number, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else {
            bail!(
                "invalid MinIO config {} at line {}: expected KEY=VALUE",
                path,
                line_number + 1
            );
        };
        let key = key.trim();
        if key.is_empty() {
            bail!(
                "invalid MinIO config {} at line {}: empty key",
                path,
                line_number + 1
            );
        }
        values.insert(
            key.to_string(),
            value
                .trim()
                .trim_matches(|character| matches!(character, '\'' | '"'))
                .to_string(),
        );
    }
    Ok(values)
}

fn validate_minio_config(config: &BTreeMap<String, String>) -> Result<()> {
    let provider = configured_value(config, "CLOUD_PROVIDER");
    if !provider.eq_ignore_ascii_case("aws") {
        bail!(
            "MinIO MBT requires CLOUD_PROVIDER=aws, got {provider:?}; configure AWS_ENDPOINT, AWS_BUCKET, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY"
        );
    }

    let endpoint = required_value(config, "AWS_ENDPOINT")?;
    if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
        bail!("AWS_ENDPOINT must be an http:// or https:// URL, got {endpoint:?}");
    }
    required_value(config, "AWS_BUCKET")?;
    required_value(config, "AWS_ACCESS_KEY_ID")?;
    required_value(config, "AWS_SECRET_ACCESS_KEY")?;
    Ok(())
}

fn configured_value(config: &BTreeMap<String, String>, key: &str) -> String {
    env::var(key)
        .ok()
        .or_else(|| config.get(key).cloned())
        .unwrap_or_default()
}

fn required_value(config: &BTreeMap<String, String>, key: &str) -> Result<String> {
    let value = configured_value(config, key);
    if value.trim().is_empty() {
        bail!(
            "MinIO MBT requires {key}; set it in GRAPH_MBT_S3_ENV_FILE or the process environment"
        );
    }
    Ok(value)
}

fn validated_prefix(prefix: &str) -> Result<String> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        bail!("GRAPH_MBT_PREFIX must contain at least one safe path component");
    }
    for component in prefix.split('/') {
        validate_component(component, "GRAPH_MBT_PREFIX")?;
    }
    Ok(prefix.to_string())
}

fn validate_component(component: &str, label: &str) -> Result<()> {
    if component.is_empty()
        || matches!(component, "." | "..")
        || !component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!(
            "{label} component {component:?} is unsafe; use only ASCII letters, numbers, '.', '-', or '_'"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_env_file, validated_prefix};
    use std::{fs, path::PathBuf};

    #[test]
    fn rejects_unsafe_prefixes() {
        assert!(validated_prefix("../other").is_err());
        assert!(validated_prefix("formal-mbt/../other").is_err());
        assert_eq!(
            validated_prefix("/formal-mbt/run-1/").unwrap(),
            "formal-mbt/run-1"
        );
    }

    #[test]
    fn parses_comments_and_exports() {
        let path = PathBuf::from(format!("/tmp/mbt-backend-config-{}", std::process::id()));
        fs::write(
            &path,
            "# comment\nexport AWS_BUCKET=test\nAWS_ENDPOINT=http://127.0.0.1:9000\n",
        )
        .unwrap();
        let values = parse_env_file(path.to_str().unwrap()).unwrap();
        fs::remove_file(path).unwrap();
        assert_eq!(values["AWS_BUCKET"], "test");
        assert_eq!(values["AWS_ENDPOINT"], "http://127.0.0.1:9000");
    }
}
