//! The node's client listeners: the public Bolt and HTTP pair, and an
//! optional internal pair that is always plaintext.
//!
//! The internal pair is for a proxy in front of the node that terminates
//! client TLS itself. With neither internal address set the node binds exactly
//! the public pair.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use futures::future::BoxFuture;
use tokio::sync::watch;

use hydradb::{
    BoltServerConfig, BoltServerHandle, ClientBoltServer, ClientHttpServer, ClientQueryService, HttpQueryServerConfig,
    HttpQueryServerHandle, QueryTransportTlsServerConfigProvider,
};

use crate::config::RuntimeConfig;

pub struct ClientListenerAddrs {
    pub bolt: SocketAddr,
    pub http: SocketAddr,
    pub internal_bolt: Option<SocketAddr>,
    pub internal_http: Option<SocketAddr>,
}

impl From<&RuntimeConfig> for ClientListenerAddrs {
    fn from(config: &RuntimeConfig) -> Self {
        Self {
            bolt: config.bolt_addr,
            http: config.http_addr,
            internal_bolt: config.bolt_internal_addr,
            internal_http: config.http_internal_addr,
        }
    }
}

/// Resolves once the client listeners begin to stop.
///
/// Draining waits for in-flight responses, up to the HTTP graceful-shutdown
/// timeout, so a route that streams (the buddy WAL feed) ends its responses
/// on this instead of holding every stop to that timeout.
pub type ListenersStopping = BoxFuture<'static, ()>;

pub struct ClientListeners {
    stop: watch::Sender<bool>,
    bolt: BoltServerHandle,
    http: HttpQueryServerHandle,
    internal_bolt: Option<BoltServerHandle>,
    internal_http: Option<HttpQueryServerHandle>,
}

impl ClientListeners {
    /// `bolt` and `http` carry everything but transport security. The public
    /// listeners serve `tls`, or plaintext when it is `None`; the internal ones
    /// always serve plaintext, each with the same limits as its public twin.
    ///
    /// `public_http_routes` builds extra routes for the public HTTP listener
    /// only, given [`ListenersStopping`]. Only there: the internal listener is
    /// plaintext even when the node has TLS.
    pub async fn bind(
        addrs: ClientListenerAddrs,
        service: &ClientQueryService,
        bolt: BoltServerConfig,
        http: HttpQueryServerConfig,
        tls: Option<Arc<dyn QueryTransportTlsServerConfigProvider>>,
        public_http_routes: impl FnOnce(ListenersStopping) -> Router,
    ) -> hydradb::Result<Self> {
        let (stop, stopped) = watch::channel(false);
        let stopping: ListenersStopping = Box::pin(async move {
            let mut stopped = stopped;
            // A dropped sender also means stop: the listeners are gone.
            let _ = stopped.wait_for(|stop| *stop).await;
        });
        let (public_bolt, public_http) = match tls {
            Some(provider) => (
                bolt.clone().with_tls_provider(Arc::clone(&provider)),
                http.clone().with_tls_provider(provider),
            ),
            None => (bolt.clone().insecure_allow_plaintext(), http.clone().insecure_allow_plaintext()),
        };
        let public_bolt = ClientBoltServer::bind(addrs.bolt, service.clone(), public_bolt).await?;
        let public_http =
            ClientHttpServer::bind_with_routes(addrs.http, service.clone(), public_http, public_http_routes(stopping)).await?;
        let internal_bolt = match addrs.internal_bolt {
            Some(addr) => Some(ClientBoltServer::bind(addr, service.clone(), bolt.insecure_allow_plaintext()).await?),
            None => None,
        };
        let internal_http = match addrs.internal_http {
            Some(addr) => Some(ClientHttpServer::bind(addr, service.clone(), http.insecure_allow_plaintext()).await?),
            None => None,
        };
        Ok(Self {
            stop,
            bolt: public_bolt,
            http: public_http,
            internal_bolt,
            internal_http,
        })
    }

    pub fn bolt_addr(&self) -> SocketAddr {
        self.bolt.local_addr()
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http.local_addr()
    }

    pub fn internal_bolt_addr(&self) -> Option<SocketAddr> {
        self.internal_bolt.as_ref().map(BoltServerHandle::local_addr)
    }

    pub fn internal_http_addr(&self) -> Option<SocketAddr> {
        self.internal_http.as_ref().map(HttpQueryServerHandle::local_addr)
    }

    /// Drain every listener. Each holds a clone of the query service, so this
    /// runs before the node is unwrapped.
    ///
    /// The public pair stops in the order the node has always stopped it,
    /// HTTP then Bolt. The internal listeners drain at the same time rather
    /// than after it, so turning them on adds nothing to how long shutdown
    /// takes, and it still fits the pod's termination grace period.
    pub async fn stop(self) -> hydradb::Result<()> {
        let Self {
            stop,
            bolt,
            http,
            internal_bolt,
            internal_http,
        } = self;
        stop.send_replace(true);
        let public = async {
            http.stop().await?;
            bolt.stop().await
        };
        let internal_http = async {
            match internal_http {
                Some(http) => http.stop().await,
                None => Ok(()),
            }
        };
        let internal_bolt = async {
            match internal_bolt {
                Some(bolt) => bolt.stop().await,
                None => Ok(()),
            }
        };
        let (public, internal_http, internal_bolt) = tokio::join!(public, internal_http, internal_bolt);
        public.and(internal_http).and(internal_bolt)
    }
}
