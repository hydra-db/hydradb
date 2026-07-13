# Turbolay Helm Chart

This chart deploys Turbolay graph nodes and controller candidates. SlateDB stores durable state in object storage; node and controller cache volumes are disposable by default.

## Requirements

- Kubernetes 1.27 or newer.
- An object-store bucket and workload credentials.
- TLS Secrets, or cert-manager with configured issuers.
- External Secrets Operator only when `auth.externalSecret.enabled=true`.
- Prometheus Operator only when `serviceMonitor.enabled=true`.

## Install

Create a production values file from `examples/values-eks.yaml`, replace every account, DNS, issuer, bucket, and image value, then run:

```bash
helm upgrade --install turbolay charts/turbolay \
  --namespace turbolay \
  --create-namespace \
  --values values-production.yaml \
  --atomic \
  --timeout 15m
```

Verify the deployment:

```bash
kubectl -n turbolay get pods,services
helm test turbolay -n turbolay
```

## Security

Public TLS and internal mTLS are enabled by default. With cert-manager disabled, provide the release-scoped Secrets shown by `helm template`, or set explicit names through `tls.public.secretName` and `tls.internal.*SecretName`. The chart can generate a client token, use an existing Secret, or materialize one through External Secrets. Production deployments should use an existing or external secret rather than a token in Helm values.

Client ingress is denied by default. Set `networkPolicy.clientIngressFrom` to
the HydraDB and ingestion namespaces, Pods, or CIDRs that may reach Bolt and
HTTPS. Load balancers should be internal unless public access is explicitly
required.

## Cache Storage

`emptyDir` is the default because S3 is ground truth. Use `persistentVolume` to retain warm SSD cache across pod replacement. Cache loss affects cold-start latency, not durable graph data.

The development example references an existing MinIO Service only to exercise
the S3-compatible path without AWS. The chart does not install MinIO, and the
EKS example leaves the custom endpoint unset so production uses AWS S3 directly.
When a custom HTTP or HTTPS endpoint is configured, the chart derives its port
and creates the matching egress rule. A non-empty
`objectStore.aws.endpointEgressTo` is required while NetworkPolicy is enabled
to restrict that rule to the endpoint namespace, Pod selector, or a
non-universal CIDR. Empty lists, empty label selectors, universal CIDRs, and
peers without a destination selector are rejected before installation.

## Upgrades

Graph nodes default to StatefulSet `OnDelete` updates so operators can drain and replace one node at a time after `helm upgrade`. The controller runtime lease ensures only one controller candidate owns the writable control plane.
