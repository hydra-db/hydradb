//! The Bolt adapter as a pingora `ServerApp`, with TLS in front of it when
//! configured.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use pingora_core::apps::ServerApp;
use pingora_core::protocols::Stream;
use pingora_core::server::ShutdownWatch;
use tokio_rustls::TlsAcceptor;
use turbolay_proxy_bolt::BoltProxy;

pub struct BoltApp {
    pub proxy: Arc<BoltProxy>,
    pub tls: Option<TlsAcceptor>,
}

#[async_trait]
impl ServerApp for BoltApp {
    async fn process_new(self: &Arc<Self>, session: Stream, _shutdown: &ShutdownWatch) -> Option<Stream> {
        let result = match &self.tls {
            Some(acceptor) => match acceptor.accept(session).await {
                Ok(stream) => self.proxy.serve(stream).await,
                Err(error) => {
                    tracing::debug!(%error, "bolt TLS handshake failed");
                    return None;
                }
            },
            None => self.proxy.serve(session).await,
        };
        if let Err(error) = result {
            tracing::debug!(%error, "bolt session ended with an error");
        }
        None
    }
}

/// A TLS acceptor from PEM files.
pub fn tls_acceptor(certificate: &Path, private_key: &Path) -> Result<TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    let certificates =
        rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(certificate)?)).collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(private_key)?))?
        .ok_or("no private key in GRAPH_TLS_PRIVATE_KEY")?;
    let config = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}
