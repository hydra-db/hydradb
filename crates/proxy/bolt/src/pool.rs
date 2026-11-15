//! One pool of logged-in Bolt connections per backend node.
//!
//! A connection is created with a handshake, HELLO and LOGON with the cluster
//! token, once, and then serves many units. It goes back to the pool only when
//! the unit left it READY; anything else is dropped. Checkout re-checks it:
//! READY, under the age limit, under the idle limit, and the socket still open
//! with nothing unread on it.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use deadpool::managed::{self, Metrics, Object, Pool, RecycleError, RecycleResult};
use hydradb_protocol::bolt::{client_handshake, decode_summary, hello, logon_bearer, MessageReader, MessageWriter, ServerMessage, Summary};
use hydradb_protocol::boltr::error::BoltError;
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use turbolay_proxy_core::types::NodeId;

/// Pool settings, the same for every node.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Connections per node, idle and in use. Admission bounds units per node
    /// below this, so checkout never waits on the pool itself.
    pub max_per_node: usize,
    pub max_age: Duration,
    pub max_idle: Duration,
    /// Time to open a backend connection and log it in.
    pub connect_timeout: Duration,
    /// Time a node gets to answer a RESET before its connection is dropped.
    pub reset_timeout: Duration,
    pub user_agent: String,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_per_node: 256,
            max_age: Duration::from_secs(50 * 60),
            max_idle: Duration::from_secs(10 * 60),
            connect_timeout: Duration::from_secs(3),
            reset_timeout: Duration::from_secs(3),
            user_agent: format!("turbolay-proxy/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

/// A logged-in backend connection.
pub struct BackendConn {
    pub(crate) reader: MessageReader<OwnedReadHalf>,
    pub(crate) writer: MessageWriter<OwnedWriteHalf>,
    /// Set only when the last unit ended with the connection in READY.
    pub(crate) ready: bool,
    pub(crate) idle_since: Instant,
    pub(crate) version: (u8, u8),
    closed: IntCounter,
}

impl BackendConn {
    pub fn version(&self) -> (u8, u8) {
        self.version
    }
}

impl Drop for BackendConn {
    fn drop(&mut self) {
        self.closed.inc();
    }
}

pub struct Manager {
    addr: String,
    token: Arc<str>,
    config: PoolConfig,
    max_message: usize,
    opened: IntCounter,
    closed: IntCounter,
}

impl managed::Manager for Manager {
    type Type = BackendConn;
    type Error = BoltError;

    async fn create(&self) -> Result<BackendConn, BoltError> {
        tokio::time::timeout(self.config.connect_timeout, self.connect())
            .await
            .map_err(|_| BoltError::Io(std::io::ErrorKind::TimedOut.into()))?
    }

    async fn recycle(&self, conn: &mut BackendConn, metrics: &Metrics) -> RecycleResult<BoltError> {
        self.check(conn, metrics)
    }
}

impl Manager {
    /// Open a connection and log it in.
    async fn connect(&self) -> Result<BackendConn, BoltError> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        stream.set_nodelay(true)?;
        let version = client_handshake(&mut stream).await?;
        let (reader, writer) = stream.into_split();
        let mut reader = MessageReader::new(reader, self.max_message);
        let mut writer = MessageWriter::new(writer);
        writer.send(&hello(&self.config.user_agent, version)).await?;
        writer.send(&logon_bearer(&self.token)).await?;
        writer.flush().await?;
        for step in ["HELLO", "LOGON"] {
            let summary = decode_summary(reader.next().await?.body())?;
            if !matches!(summary, ServerMessage::Success { .. }) {
                return Err(BoltError::Authentication(format!(
                    "backend {} refused {step}: {} {}",
                    self.addr,
                    summary.code().unwrap_or("?"),
                    summary.message().unwrap_or("")
                )));
            }
        }
        self.opened.inc();
        Ok(BackendConn {
            reader,
            writer,
            ready: true,
            idle_since: Instant::now(),
            version,
            closed: self.closed.clone(),
        })
    }

    /// Whether a pooled connection can be handed out again.
    fn check(&self, conn: &mut BackendConn, metrics: &Metrics) -> RecycleResult<BoltError> {
        if !conn.ready {
            return Err(RecycleError::message("connection was not left READY"));
        }
        if metrics.age() > self.config.max_age {
            return Err(RecycleError::message("connection past its maximum age"));
        }
        if conn.idle_since.elapsed() > self.config.max_idle {
            return Err(RecycleError::message("connection idle past the limit"));
        }
        // An idle READY connection has nothing to say. Readable means the
        // node closed it or sent something unexpected; either way it is gone.
        // `try_read` answers from the reactor's readiness without a syscall
        // while the socket has not turned readable. The reader owns the read
        // half and holds nothing between messages, so the socket is reached
        // through the write half, whose buffer is empty because every unit
        // ends with a flush.
        let socket: &TcpStream = conn.writer.get_ref().as_ref();
        let mut probe = [0_u8; 1];
        match socket.try_read(&mut probe) {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            Ok(0) => Err(RecycleError::message("node closed the connection")),
            Ok(_) => Err(RecycleError::message("unexpected data on an idle connection")),
            Err(error) => Err(RecycleError::Backend(BoltError::Io(error))),
        }
    }
}

/// A checked-out connection.
pub type PooledConn = Object<Manager>;

/// One node's pool.
pub struct NodePool {
    pool: Pool<Manager>,
    addr: String,
    opened: IntCounter,
    closed: IntCounter,
}

impl NodePool {
    /// Check out a READY connection.
    pub async fn checkout(&self) -> Result<PooledConn, BoltError> {
        self.pool.get().await.map_err(|error| match error {
            managed::PoolError::Backend(error) => error,
            other => BoltError::Backend(format!("backend pool: {other:?}")),
        })
    }
}

type PoolMap = HashMap<NodeId, Arc<NodePool>>;

/// Every node's pool, created on first use and replaced when the node's
/// address changes.
pub struct BackendPools {
    pools: Arc<RwLock<PoolMap>>,
    config: PoolConfig,
    token: Arc<str>,
    max_message: usize,
    opened: IntCounterVec,
    closed: IntCounterVec,
}

/// Reads each pool's size when `/metrics` is scraped.
struct PoolGauges {
    pools: Arc<RwLock<PoolMap>>,
    connections: IntGaugeVec,
    idle: IntGaugeVec,
}

impl Collector for PoolGauges {
    fn desc(&self) -> Vec<&Desc> {
        self.connections.desc().into_iter().chain(self.idle.desc()).collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        for (node, pool) in self.pools.read().expect("pool map").iter() {
            let status = pool.pool.status();
            self.connections.with_label_values(&[node.as_str()]).set(status.size as i64);
            self.idle.with_label_values(&[node.as_str()]).set(status.available as i64);
        }
        self.connections.collect().into_iter().chain(self.idle.collect()).collect()
    }
}

/// Checkout statistics for one node, for `/metrics` and the benchmarks.
#[derive(Clone, Debug, Default)]
pub struct PoolStats {
    pub size: usize,
    pub available: usize,
    pub opened: u64,
    pub closed: u64,
}

impl BackendPools {
    /// Pool metrics register into `registry`, so `/metrics` serves them with
    /// the router's.
    pub fn new(config: PoolConfig, token: Arc<str>, max_message: usize, registry: &Registry) -> Self {
        let counter = |name: &str, help: &str| {
            let counter = IntCounterVec::new(Opts::new(name, help), &["node"]).expect("valid metric");
            registry.register(Box::new(counter.clone())).expect("metric registered once");
            counter
        };
        let gauge = |name: &str, help: &str| IntGaugeVec::new(Opts::new(name, help), &["node"]).expect("valid metric");
        let pools = Arc::new(RwLock::new(PoolMap::new()));
        registry
            .register(Box::new(PoolGauges {
                pools: Arc::clone(&pools),
                connections: gauge("turbolay_proxy_pool_connections", "Backend connections, idle and in use."),
                idle: gauge("turbolay_proxy_pool_idle_connections", "Idle backend connections."),
            }))
            .expect("metric registered once");
        Self {
            pools,
            config,
            token,
            max_message,
            opened: counter("turbolay_proxy_backend_connections_opened_total", "Backend connections opened."),
            closed: counter("turbolay_proxy_backend_connections_closed_total", "Backend connections closed."),
        }
    }

    /// The pool for `node` at `addr`, created on first use.
    pub fn pool(&self, node: &NodeId, addr: &str) -> Arc<NodePool> {
        if let Some(pool) = self.pools.read().expect("pool map").get(node) {
            if pool.addr == addr {
                return Arc::clone(pool);
            }
        }
        let mut pools = self.pools.write().expect("pool map");
        if let Some(pool) = pools.get(node) {
            if pool.addr == addr {
                return Arc::clone(pool);
            }
        }
        let (opened, closed) = (
            self.opened.with_label_values(&[node.as_str()]),
            self.closed.with_label_values(&[node.as_str()]),
        );
        let manager = Manager {
            addr: addr.to_string(),
            token: Arc::clone(&self.token),
            config: self.config.clone(),
            max_message: self.max_message,
            opened: opened.clone(),
            closed: closed.clone(),
        };
        let pool = Arc::new(NodePool {
            pool: Pool::builder(manager)
                .max_size(self.config.max_per_node)
                .build()
                .expect("a pool without timeouts needs no runtime"),
            addr: addr.to_string(),
            opened,
            closed,
        });
        pools.insert(node.clone(), Arc::clone(&pool));
        pool
    }

    /// Check out a READY connection to `node` at `addr`.
    pub async fn get(&self, node: &NodeId, addr: &str) -> Result<PooledConn, BoltError> {
        self.pool(node, addr).checkout().await
    }

    /// Per node pool statistics, keyed by node id.
    pub fn stats(&self) -> Vec<(NodeId, PoolStats)> {
        let mut stats: Vec<_> = self
            .pools
            .read()
            .expect("pool map")
            .iter()
            .map(|(node, pool)| {
                let status = pool.pool.status();
                (
                    node.clone(),
                    PoolStats {
                        size: status.size,
                        available: status.available,
                        opened: pool.opened.get(),
                        closed: pool.closed.get(),
                    },
                )
            })
            .collect();
        stats.sort_by(|a, b| a.0.cmp(&b.0));
        stats
    }
}

/// Return a connection to its pool if it is READY, otherwise drop it.
pub(crate) fn release(mut conn: PooledConn, ready: bool) {
    conn.ready = ready;
    conn.idle_since = Instant::now();
    if !ready {
        drop(Object::take(conn));
    }
}
