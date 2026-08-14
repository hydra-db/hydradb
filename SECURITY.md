# Security Policy

## Reporting a vulnerability

Report it privately to the HydraDB maintainers. **Do not open a public issue**,
and do not describe the vulnerability in a pull request, a discussion, or a
commit message before a fix has shipped.

Include as much of the following as you have:

- The version — a release tag, image tag, or commit SHA.
- The component: `graph-node`, `graph-indexer`, the Bolt adapter, the HTTP API,
  the Helm chart, or the object-store layer.
- The impact, and what an attacker needs to reach it — network access to the
  Bolt or HTTP port, a valid auth token, object-store credentials, or access to
  the cluster.
- A reproduction, ideally minimal.

We will acknowledge within 3 working days and give an initial assessment,
including our severity view, within 10 working days. If you have not heard back
in that time, resend — assume the mail did not arrive rather than that it was
ignored.

We will credit you when the fix ships unless you would rather stay anonymous.
Please give us a reasonable window to release a fix before disclosing publicly.

## Supported versions

HydraDB is pre-1.0. Fixes land on `main` and in the next release. There are no
long-term support branches and older tags are not patched in place.

| Version | Supported |
|---|---|
| Latest release | Yes |
| `main` | Yes |
| Anything older | No — upgrade |

## Scope

In scope: the `graph-node` and `graph-indexer` binaries, the Bolt and HTTP
adapters, authentication and authorization, the query engine, the object-store
and writer-coordination layer, the published container images, and the Helm
chart's security defaults.

Out of scope:

- Vulnerabilities in dependencies, unless HydraDB's use of them is what makes
  the issue exploitable. [SlateDB](https://github.com/usecortex/slatedb) owns
  writer fencing, WAL durability, compaction, and object-store coordination;
  report issues in that layer upstream.
- Findings that require configuration the documentation warns against.
  `GRAPH_ALLOW_PLAINTEXT=true` disables the TLS requirement on both public
  adapters and is documented as local development only, so a report that traffic
  is readable with it set is expected behaviour.
- Missing hardening with no demonstrated impact, and scanner output submitted
  without a working reproduction.

The chart refuses to render unscoped network egress rather than defaulting to
`0.0.0.0/0`, and CI asserts this on every pull request. A way to make it render
an open default is in scope.
