#[cfg(feature = "bolt-server")]
pub(crate) mod bolt;
#[cfg(feature = "http-api")]
pub(crate) mod http;
pub(crate) mod service;

#[cfg(test)]
pub(crate) struct ClientTestTlsBundle {
    pub server: std::sync::Arc<tokio_rustls::rustls::ServerConfig>,
    pub client: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    #[cfg(feature = "http-api")]
    pub certificate_der: Vec<u8>,
}

#[cfg(test)]
pub(crate) fn client_test_tls_bundle() -> ClientTestTlsBundle {
    use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair};
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
    use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};

    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let certificate = params.self_signed(&key).unwrap();
    let private_key: PrivateKeyDer<'static> = PrivatePkcs8KeyDer::from(key.serialize_der()).into();
    let server = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.der().clone()], private_key)
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(certificate.der().clone()).unwrap();
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    ClientTestTlsBundle {
        server: std::sync::Arc::new(server),
        client: std::sync::Arc::new(client),
        #[cfg(feature = "http-api")]
        certificate_der: certificate.der().to_vec(),
    }
}
