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

## Image Publication

`.github/workflows/container.yml` builds the production Dockerfile on pull
requests and publishes `linux/amd64` images to the regional
`staging/turbolay` Amazon ECR repository after a push to `main`. It follows the
HydraDB deployment convention by publishing the full commit SHA and `latest`
tags through the shared AWS role. Every published image also has an OCI digest,
an SBOM, and build-provenance attestation. Argo CD is pinned to the digest and
never deploys by `latest`.

The publisher uses GitHub OIDC to assume the organization-provided
`AWS_ROLE_ARN` secret, matching `hydradb-application`. Do not store AWS access
keys in GitHub. The ECR repository and role access are provisioned through the
team's existing AWS infrastructure process.

After publishing, the workflow checks out `usecortex/hydradb-argocd` with the
existing `INFRA_REPO_TOKEN`, updates the staging tag and digest, validates the
Helm release, and pushes the deployment commit to `main`. This is the same
promotion path used by HydraDB application and ingestion services.

Set the `TURBOLAY_STAGING_DEPLOY_ENABLED` repository variable to `true` only
after the staging ECR repository, S3 roles, certificates, and client-auth Secret
exist. The workflow then uses the shared `ARGOCD_AUTH_TOKEN` to refresh, sync,
and wait for the `turbolay-staging` application to become healthy.

EKS nodes pull the private image through their IAM node role. Attach
`AmazonEC2ContainerRegistryPullOnly` or equivalent repository-scoped pull
permissions; no registry password or Kubernetes image-pull Secret is required.

## Security

Public TLS and internal mTLS are enabled by default. With cert-manager disabled, provide the release-scoped Secrets shown by `helm template`, or set explicit names through `tls.public.secretName` and `tls.internal.*SecretName`. The chart can generate a client token, use an existing Secret, or materialize one through External Secrets. Production deployments should use an existing or external secret rather than a token in Helm values.

When `tls.public.trustBundle.enabled=true`, install trust-manager first and
configure its trust source namespace to this release namespace. The chart then
publishes only the public CA certificate into explicitly selected client
namespaces; private keys never leave the Turbolay namespace.

Each release currently serves exactly one graph scope and one cell. Deploy a
separate release, object-store prefix, and controller database for every
independently writable namespace or subtenant.

Client ingress is denied by default. Set `networkPolicy.clientIngressFrom` to
the HydraDB and ingestion namespaces, Pods, or CIDRs that may reach Bolt and
HTTPS. Load balancers should be internal unless public access is explicitly
required.

Outbound HTTPS is also denied by default. Set `networkPolicy.httpsEgressTo` to
private peers that cover the Kubernetes API used for active-role publication
and, on AWS with IRSA, the private S3 and STS endpoint addresses. Prefer
interface VPC endpoints with private DNS so this traffic stays inside the VPC.
Kubernetes NetworkPolicy cannot restrict traffic by DNS name, so the chart
rejects empty selectors and universal CIDRs instead of opening TCP/443 to the
Internet.

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

Graph nodes use ordered StatefulSet rolling updates with a disruption budget.
Healthy standbys remain ready, while runtime-owned Pod labels expose only the
current data writer and active controller through client and control Services.
SlateDB fencing and the controller runtime lease remain the final correctness
boundary during failover.
