//! The ready proxy replicas, from a watch on the proxy service's
//! EndpointSlices.
//!
//! Pushed as pods turn ready or unready, from the same list kube-proxy
//! balances over. Needs a Role with get, list and watch on `endpointslices` in
//! the proxy's namespace.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use futures::StreamExt;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::watcher::{self, Event};
use kube::{Api, Client};
use tokio::sync::watch;
use turbolay_proxy_topology::ProxySetSource;

/// Watches EndpointSlices labelled `kubernetes.io/service-name=<service>`.
pub struct EndpointSliceProxySet {
    pub namespace: String,
    pub service: String,
    /// The Bolt port advertised for each ready pod address.
    pub port: u16,
}

/// The `host:port` of every ready endpoint in `slice`. A missing ready
/// condition counts as ready, as the EndpointSlice API defines it.
pub fn ready_addresses(slice: &EndpointSlice, port: u16) -> Vec<String> {
    slice
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.conditions.as_ref().and_then(|conditions| conditions.ready) != Some(false))
        .flat_map(|endpoint| endpoint.addresses.iter())
        .map(|address| {
            if address.contains(':') {
                format!("[{address}]:{port}")
            } else {
                format!("{address}:{port}")
            }
        })
        .collect()
}

#[async_trait::async_trait]
impl ProxySetSource for EndpointSliceProxySet {
    async fn run(self: Box<Self>, proxies: watch::Sender<Vec<String>>) {
        let client = match Client::try_default().await {
            Ok(client) => client,
            Err(error) => {
                tracing::error!(%error, "no kubernetes client; the replica set stays at its initial value");
                proxies.closed().await;
                return;
            }
        };
        let api: Api<EndpointSlice> = Api::namespaced(client, &self.namespace);
        let config = watcher::Config::default().labels(&format!("kubernetes.io/service-name={}", self.service));
        let mut slices: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut initial: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut stream = watcher::watcher(api, config).boxed();
        let publish = |slices: &BTreeMap<String, Vec<String>>| {
            let mut next: Vec<String> = slices.values().flatten().cloned().collect();
            next.sort();
            next.dedup();
            proxies.send_if_modified(|current| {
                let changed = *current != next;
                if changed {
                    *current = next;
                }
                changed
            });
        };
        loop {
            tokio::select! {
                _ = proxies.closed() => return,
                event = stream.next() => match event {
                    None => return,
                    Some(Err(error)) => tracing::warn!(%error, "endpointslice watch error, retrying"),
                    Some(Ok(Event::Init)) => initial.clear(),
                    Some(Ok(Event::InitApply(slice))) => {
                        let name = slice.metadata.name.clone().unwrap_or_default();
                        initial.insert(name, ready_addresses(&slice, self.port));
                    }
                    // Sent even when unchanged: the first full list is how the
                    // proxy learns the set is known, and the first replica's
                    // list is empty until it is ready itself.
                    Some(Ok(Event::InitDone)) => {
                        slices = std::mem::take(&mut initial);
                        let mut next: Vec<String> = slices.values().flatten().cloned().collect();
                        next.sort();
                        next.dedup();
                        proxies.send_replace(next);
                    }
                    Some(Ok(Event::Apply(slice))) => {
                        let name = slice.metadata.name.clone().unwrap_or_default();
                        slices.insert(name, ready_addresses(&slice, self.port));
                        publish(&slices);
                    }
                    Some(Ok(Event::Delete(slice))) => {
                        slices.remove(&slice.metadata.name.clone().unwrap_or_default());
                        publish(&slices);
                    }
                },
            }
        }
    }
}
