use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use slatedb_graph_kernel::{
    QueryTransportTlsClientConfigProvider, QueryTransportTlsServerConfigProvider,
    ReloadableQueryTransportTlsClientConfigProvider,
    ReloadableQueryTransportTlsServerConfigProvider,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::{
    server::WebPkiClientVerifier, ClientConfig, RootCertStore, ServerConfig,
};

type TlsResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct FileTlsReloader {
    provider: Arc<ReloadableQueryTransportTlsServerConfigProvider>,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

pub struct FileMutualTlsServerReloader {
    provider: Arc<ReloadableQueryTransportTlsServerConfigProvider>,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl FileMutualTlsServerReloader {
    pub fn start(
        certificate_path: &Path,
        private_key_path: &Path,
        client_ca_path: &Path,
        interval: Duration,
    ) -> TlsResult<Self> {
        let certificate_path = certificate_path.to_path_buf();
        let private_key_path = private_key_path.to_path_buf();
        let client_ca_path = client_ca_path.to_path_buf();
        let (config, mut fingerprint) =
            load_mutual_server_config(&certificate_path, &private_key_path, &client_ca_path)?;
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
                        if changed.is_err() || *stop_rx.borrow() { return; }
                    }
                    _ = ticker.tick() => {
                        match load_mutual_server_config(&certificate_path, &private_key_path, &client_ca_path) {
                            Ok((_, next)) if next == fingerprint => {}
                            Ok((config, next)) => {
                                if task_provider.rotate(Arc::new(config)).is_ok() {
                                    fingerprint = next;
                                }
                            }
                            Err(error) => tracing::warn!(error = %error, "internal mTLS server certificate reload failed"),
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

pub struct FileMutualTlsClientReloader {
    provider: Arc<ReloadableQueryTransportTlsClientConfigProvider>,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl FileMutualTlsClientReloader {
    pub fn start(
        certificate_path: &Path,
        private_key_path: &Path,
        server_ca_path: &Path,
        interval: Duration,
    ) -> TlsResult<Self> {
        let certificate_path = certificate_path.to_path_buf();
        let private_key_path = private_key_path.to_path_buf();
        let server_ca_path = server_ca_path.to_path_buf();
        let (config, mut fingerprint) =
            load_mutual_client_config(&certificate_path, &private_key_path, &server_ca_path)?;
        let provider = Arc::new(ReloadableQueryTransportTlsClientConfigProvider::new(
            Arc::new(config),
        ));
        let task_provider = Arc::clone(&provider);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() { return; }
                    }
                    _ = ticker.tick() => {
                        match load_mutual_client_config(&certificate_path, &private_key_path, &server_ca_path) {
                            Ok((_, next)) if next == fingerprint => {}
                            Ok((config, next)) => {
                                if task_provider.rotate(Arc::new(config)).is_ok() {
                                    fingerprint = next;
                                }
                            }
                            Err(error) => tracing::warn!(error = %error, "internal mTLS client certificate reload failed"),
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

    pub fn provider(&self) -> Arc<dyn QueryTransportTlsClientConfigProvider> {
        Arc::clone(&self.provider) as Arc<dyn QueryTransportTlsClientConfigProvider>
    }

    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.task.await;
    }
}

fn load_mutual_server_config(
    certificate_path: &Path,
    private_key_path: &Path,
    client_ca_path: &Path,
) -> TlsResult<(ServerConfig, u64)> {
    let (certificates, private_key, mut fingerprint) =
        load_certificate_material(certificate_path, private_key_path)?;
    let ca_bytes = std::fs::read(client_ca_path)?;
    ca_bytes.hash(&mut fingerprint);
    let roots = load_root_store(&ca_bytes)?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificates, private_key)?;
    Ok((config, fingerprint.finish()))
}

fn load_mutual_client_config(
    certificate_path: &Path,
    private_key_path: &Path,
    server_ca_path: &Path,
) -> TlsResult<(ClientConfig, u64)> {
    let (certificates, private_key, mut fingerprint) =
        load_certificate_material(certificate_path, private_key_path)?;
    let ca_bytes = std::fs::read(server_ca_path)?;
    ca_bytes.hash(&mut fingerprint);
    let roots = load_root_store(&ca_bytes)?;
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certificates, private_key)?;
    Ok((config, fingerprint.finish()))
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

fn load_root_store(bytes: &[u8]) -> TlsResult<RootCertStore> {
    let mut reader = Cursor::new(bytes);
    let certificates =
        rustls_pemfile::certs(&mut reader).collect::<std::result::Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        return Err(Error::new(ErrorKind::InvalidData, "TLS CA file is empty").into());
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate)?;
    }
    Ok(roots)
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
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose,
    };

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

    #[tokio::test]
    async fn internal_mutual_tls_requires_and_accepts_a_trusted_client_certificate() {
        let root = tempfile::tempdir().unwrap();
        let certificate_path = root.path().join("tls.crt");
        let private_key_path = root.path().join("tls.key");
        let ca_path = root.path().join("ca.crt");

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
        let ca = ca_params.self_signed(&ca_key).unwrap();
        let leaf_key = KeyPair::generate().unwrap();
        let mut leaf_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        leaf_params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
        std::fs::write(&certificate_path, leaf.pem()).unwrap();
        std::fs::write(&private_key_path, leaf_key.serialize_pem()).unwrap();
        std::fs::write(&ca_path, ca.pem()).unwrap();

        let server = FileMutualTlsServerReloader::start(
            &certificate_path,
            &private_key_path,
            &ca_path,
            Duration::from_secs(60),
        )
        .unwrap();
        let client = FileMutualTlsClientReloader::start(
            &certificate_path,
            &private_key_path,
            &ca_path,
            Duration::from_secs(60),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_config = server.provider().current_server_config().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio_rustls::TlsAcceptor::from(server_config)
                .accept(stream)
                .await
                .unwrap();
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let server_name = tokio_rustls::rustls::pki_types::ServerName::try_from("localhost")
            .unwrap()
            .to_owned();
        tokio_rustls::TlsConnector::from(client.provider().current_client_config().unwrap())
            .connect(server_name, stream)
            .await
            .unwrap();
        accept.await.unwrap();
        client.stop().await;
        server.stop().await;
    }
}
