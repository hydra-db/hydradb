use std::env;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};

type KubernetesResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SERVICE_ACCOUNT_TOKEN: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const SERVICE_ACCOUNT_CA: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";

#[derive(Clone)]
pub struct KubernetesPodRolePublisher {
    client: Option<reqwest::Client>,
    endpoint: String,
    namespace: String,
    pod_name: String,
    label_key: &'static str,
    token_path: PathBuf,
}

impl KubernetesPodRolePublisher {
    pub fn from_env(label_key: &'static str) -> KubernetesResult<Self> {
        let enabled = parse_bool_env("GRAPH_KUBERNETES_ROLE_LABELS", false)?;
        if !enabled {
            return Ok(Self {
                client: None,
                endpoint: String::new(),
                namespace: String::new(),
                pod_name: String::new(),
                label_key,
                token_path: PathBuf::new(),
            });
        }

        let namespace = required_env("POD_NAMESPACE")?;
        let pod_name = required_env("POD_NAME")?;
        validate_dns_name("POD_NAMESPACE", &namespace)?;
        validate_dns_name("POD_NAME", &pod_name)?;
        let host = required_env("KUBERNETES_SERVICE_HOST")?;
        let port = env::var("KUBERNETES_SERVICE_PORT_HTTPS").unwrap_or_else(|_| "443".to_string());
        let ca_path = PathBuf::from(
            env::var("KUBERNETES_SERVICE_ACCOUNT_CA")
                .unwrap_or_else(|_| SERVICE_ACCOUNT_CA.to_string()),
        );
        let token_path = PathBuf::from(
            env::var("KUBERNETES_SERVICE_ACCOUNT_TOKEN")
                .unwrap_or_else(|_| SERVICE_ACCOUNT_TOKEN.to_string()),
        );
        let ca = std::fs::read(&ca_path)?;
        let ca = reqwest::Certificate::from_pem(&ca)?;
        let client = reqwest::Client::builder()
            .add_root_certificate(ca)
            .https_only(true)
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()?;

        Ok(Self {
            client: Some(client),
            endpoint: format!("https://{host}:{port}"),
            namespace,
            pod_name,
            label_key,
            token_path,
        })
    }

    pub async fn publish(&self, active: bool) -> KubernetesResult<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        let url = format!(
            "{}/api/v1/namespaces/{}/pods/{}",
            self.endpoint, self.namespace, self.pod_name
        );
        let body = role_label_patch(self.label_key, active);

        let mut last_error = None;
        for attempt in 0..5_u32 {
            let token = read_secret(&self.token_path)?;
            let response = client
                .patch(&url)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/merge-patch+json")
                .json(&body)
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    let status = response.status();
                    let detail = response.text().await.unwrap_or_default();
                    last_error = Some(format!(
                        "Kubernetes pod label update returned {status}: {detail}"
                    ));
                    if !status.is_server_error() && status.as_u16() != 429 {
                        break;
                    }
                }
                Err(error) => last_error = Some(error.to_string()),
            }
            tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt + 1))).await;
        }
        Err(Error::other(
            last_error.unwrap_or_else(|| "Kubernetes pod label update failed".to_string()),
        )
        .into())
    }
}

fn required_env(name: &str) -> KubernetesResult<String> {
    let value = env::var(name)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, format!("{name} is required")))?;
    if value.trim().is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, format!("{name} cannot be empty")).into());
    }
    Ok(value)
}

fn parse_bool_env(name: &str, default: bool) -> KubernetesResult<bool> {
    match env::var(name) {
        Ok(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Ok(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Ok(value) => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("{name} must be true or false, got {value}"),
        )
        .into()),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn validate_dns_name(name: &str, value: &str) -> KubernetesResult<()> {
    let valid = value.len() <= 253
        && !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'.'
        })
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric);
    if valid {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!("{name} must be a DNS-safe Kubernetes name"),
        )
        .into())
    }
}

fn read_secret(path: &Path) -> KubernetesResult<String> {
    let value = std::fs::read_to_string(path)?.trim().to_string();
    if value.is_empty() {
        Err(Error::new(
            ErrorKind::InvalidData,
            "Kubernetes service-account token is empty",
        )
        .into())
    } else {
        Ok(value)
    }
}

fn role_label_patch(label_key: &str, active: bool) -> serde_json::Value {
    let mut labels = serde_json::Map::new();
    labels.insert(
        label_key.to_string(),
        serde_json::Value::String(if active { "true" } else { "false" }.to_string()),
    );
    serde_json::json!({"metadata": {"labels": labels}})
}

#[cfg(test)]
mod tests {
    use super::{role_label_patch, validate_dns_name};

    #[test]
    fn validates_kubernetes_pod_and_namespace_names() {
        assert!(validate_dns_name("POD_NAME", "turbolay-node-0").is_ok());
        assert!(validate_dns_name("POD_NAMESPACE", "turbolay-staging").is_ok());
        assert!(validate_dns_name("POD_NAME", "../pod").is_err());
        assert!(validate_dns_name("POD_NAME", "Pod_Name").is_err());
    }

    #[test]
    fn role_patch_uses_the_requested_label_key() {
        assert_eq!(
            role_label_patch("graph.usecortex.io/serving", true),
            serde_json::json!({
                "metadata": {
                    "labels": {"graph.usecortex.io/serving": "true"}
                }
            })
        );
    }
}
