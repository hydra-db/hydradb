use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use slatedb_graph_kernel::{
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

fn load_certificate_material(
    certificate_path: &Path,
    private_key_path: &Path,
) -> TlsResult<(
    Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
    tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>,
    DefaultHasher,
)> {
    let certificate_bytes = std::fs::read(certificate_path)?;
    let private_key_bytes = std::fs::read(private_key_path)?;
    let mut certificate_reader = Cursor::new(certificate_bytes.as_slice());
    let certificates = rustls_pemfile::certs(&mut certificate_reader)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(Error::new(ErrorKind::InvalidData, "TLS certificate file is empty").into());
    }
    let mut private_key_reader = Cursor::new(private_key_bytes.as_slice());
    let private_key = rustls_pemfile::private_key(&mut private_key_reader)?.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "TLS private key file contains no key",
        )
    })?;
    let mut fingerprint = DefaultHasher::new();
    certificate_bytes.hash(&mut fingerprint);
    private_key_bytes.hash(&mut fingerprint);
    Ok((certificates, private_key, fingerprint))
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

fn load_server_config(
    certificate_path: &PathBuf,
    private_key_path: &PathBuf,
) -> TlsResult<(ServerConfig, u64)> {
    let certificate_bytes = std::fs::read(certificate_path)?;
    let private_key_bytes = std::fs::read(private_key_path)?;
    let mut certificate_reader = Cursor::new(certificate_bytes.as_slice());
    let certificates = rustls_pemfile::certs(&mut certificate_reader)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(Error::new(ErrorKind::InvalidData, "TLS certificate file is empty").into());
    }
    let mut private_key_reader = Cursor::new(private_key_bytes.as_slice());
    let private_key = rustls_pemfile::private_key(&mut private_key_reader)?.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "TLS private key file contains no key",
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
}
