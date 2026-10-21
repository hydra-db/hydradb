use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Error, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hydradb::{
    QueryTransportTlsServerConfigProvider, ReloadableQueryTransportTlsServerConfigProvider,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::ServerConfig;

type TlsResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct FileTlsReloader {
    provider: Arc<ReloadableQueryTransportTlsServerConfigProvider>,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl FileTlsReloader {
    pub fn start(
        certificate_path: &Path,
        private_key_path: &Path,
        interval: Duration,
    ) -> TlsResult<Self> {
        if interval.is_zero() {
            return Err(Error::new(ErrorKind::InvalidInput, "TLS reload interval is zero").into());
        }
        let certificate_path = certificate_path.to_path_buf();
        let private_key_path = private_key_path.to_path_buf();
        let (config, mut fingerprint) = load_server_config(&certificate_path, &private_key_path)?;
        let provider = Arc::new(ReloadableQueryTransportTlsServerConfigProvider::new(
            Arc::new(config),
        ));
        let task_provider = Arc::clone(&provider);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            return;
                        }
                    }
                    _ = ticker.tick() => {
                        match load_server_config(&certificate_path, &private_key_path) {
                            Ok((_, next_fingerprint)) if next_fingerprint == fingerprint => {}
                            Ok((config, next_fingerprint)) => {
                                if let Err(error) = task_provider.rotate(Arc::new(config)) {
                                    tracing::warn!(error = %error, "TLS certificate rotation failed");
                                } else {
                                    fingerprint = next_fingerprint;
                                    tracing::info!("TLS certificate and private key reloaded");
                                }
                            }
                            Err(error) => {
                                tracing::warn!(error = %error, "TLS certificate files are not yet reloadable");
                            }
                        }
                    }
                }
            }
        });
        Ok(Self {
            provider,
            stop_tx,
            task,
        })
    }

    pub fn provider(&self) -> Arc<dyn QueryTransportTlsServerConfigProvider> {
        Arc::clone(&self.provider) as Arc<dyn QueryTransportTlsServerConfigProvider>
    }

    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.task.await;
    }
}

/// Read a TLS secret, naming the attempted path on failure.
///
/// A bare `std::fs::read(path)?` surfaces as `Os { code: 2, kind: NotFound }`
/// with no indication of which file — the certificate or the private key —
/// failed or where it was looked for. On an unmounted secret volume that is
/// the whole diagnosis an operator gets. Wrapping the error keeps `err.kind()`
/// (so callers that branch on `NotFound` still can) while prefixing the label
/// and path, matching how `GRAPH_AUTH_TOKEN_FILE` reads its secret.
fn read_tls_secret(label: &str, path: &Path) -> TlsResult<Vec<u8>> {
    std::fs::read(path).map_err(|err| {
        Error::new(
            err.kind(),
            format!("cannot read {label} at {}: {err}", path.display()),
        )
        .into()
    })
}

fn load_server_config(
    certificate_path: &Path,
    private_key_path: &Path,
) -> TlsResult<(ServerConfig, u64)> {
    let certificate_bytes = read_tls_secret("TLS certificate", certificate_path)?;
    let private_key_bytes = read_tls_secret("TLS private key", private_key_path)?;
    let mut certificate_reader = Cursor::new(certificate_bytes.as_slice());
    let certificates = rustls_pemfile::certs(&mut certificate_reader)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "TLS certificate file at {} is empty",
                certificate_path.display()
            ),
        )
        .into());
    }
    let mut private_key_reader = Cursor::new(private_key_bytes.as_slice());
    let private_key = rustls_pemfile::private_key(&mut private_key_reader)?.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            format!(
                "TLS private key file at {} contains no key",
                private_key_path.display()
            ),
        )
    })?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)?;
    let mut hasher = DefaultHasher::new();
    certificate_bytes.hash(&mut hasher);
    private_key_bytes.hash(&mut hasher);
    Ok((config, hasher.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair};

    fn write_certificate_pair(certificate_path: &Path, private_key_path: &Path) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let certificate = params.self_signed(&key).unwrap();
        std::fs::write(certificate_path, certificate.pem()).unwrap();
        std::fs::write(private_key_path, key.serialize_pem()).unwrap();
    }

    #[tokio::test]
    async fn file_tls_reloader_rotates_after_atomic_secret_update() {
        let root = tempfile::tempdir().unwrap();
        let certificate_path = root.path().join("tls.crt");
        let private_key_path = root.path().join("tls.key");
        write_certificate_pair(&certificate_path, &private_key_path);
        let reloader = FileTlsReloader::start(
            &certificate_path,
            &private_key_path,
            Duration::from_millis(10),
        )
        .unwrap();
        let provider = reloader.provider();
        let initial_generation = provider.generation();
        write_certificate_pair(&certificate_path, &private_key_path);
        tokio::time::timeout(Duration::from_secs(2), async {
            while provider.generation() == initial_generation {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        provider.current_server_config().unwrap();
        reloader.stop().await;
    }

    #[test]
    fn missing_certificate_names_the_path_and_keeps_kind() {
        let root = tempfile::tempdir().unwrap();
        let certificate_path = root.path().join("absent.crt");
        let private_key_path = root.path().join("tls.key");
        let error = load_server_config(&certificate_path, &private_key_path).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains(&certificate_path.display().to_string()),
            "error should name the certificate path, got: {message}"
        );
        assert!(
            message.contains("TLS certificate"),
            "error should say which secret failed, got: {message}"
        );
        let io_error = error.downcast_ref::<Error>().expect("io::Error preserved");
        assert_eq!(io_error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn empty_certificate_names_the_path() {
        let root = tempfile::tempdir().unwrap();
        let certificate_path = root.path().join("tls.crt");
        let private_key_path = root.path().join("tls.key");
        std::fs::write(&certificate_path, b"").unwrap();
        let key = KeyPair::generate().unwrap();
        std::fs::write(&private_key_path, key.serialize_pem()).unwrap();
        let error = load_server_config(&certificate_path, &private_key_path).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains(&certificate_path.display().to_string()),
            "error should name the certificate path, got: {message}"
        );
        assert!(
            message.contains("is empty"),
            "error should report the empty file, got: {message}"
        );
    }
}
