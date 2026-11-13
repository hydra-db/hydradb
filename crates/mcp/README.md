# Run HydraDB through MCP

`hydradb-mcp` exposes the `execute_query` stdio MCP tool using `rmcp` 3.5.1.
Execute Cypher in a tenant and subtenant. The first write creates and registers the scope:

```json
{"name":"execute_query","arguments":{"query":"MATCH (n {id: $id}) RETURN n.id","parameters":{"id":1},"tenant":"acme","subtenant":"sales","page_size":100}}
```

`execute_query` runs raw Cypher, including writes, through HydraDB's authenticated HTTP query API.
It returns one HTTP JSON page with `columns`, typed `rows`, `query_id`, `read_epoch`, `bookmark`, and `next_cursor`.
To continue, pass `next_cursor` as `cursor` and the returned `query_id` with the same query, parameters, tenant, subtenant, and page size.
The HTTP adapter uses `query_id` to check cursor ownership.
A null `next_cursor` means the result is complete. Omit `page_size` to use the node's default.
Each page is limited to 16 MiB. Reduce `page_size` for larger rows.
Errors set MCP's `isError` flag. A disconnected or failed write can have committed; inspect the data before retrying.

GraphNode can automatically report these query failures to an HTTP receiver
that buffers reports for ClickHouse. Reporting is off by default; use
`GRAPH_QUERY_TELEMETRY_ENABLED=true` and `GRAPH_QUERY_TELEMETRY_URL` on the node.
Set `GRAPH_QUERY_TELEMETRY_ENABLED=false` to turn it off after a restart.
See the [query telemetry guide](../query-telemetry/README.md).

## Start the node, indexer, and MCP server

From the repository root:

```sh
just mcp-local /tmp/hydradb-mcp
```

The launcher starts the sibling `graph-node` and `graph-indexer` executables, waits for both readiness endpoints, and serves MCP on stdin and stdout.
The three executables must stay in the same directory.
`just build-mcp` builds them without starting them. Cargo's configured target directory contains the binaries under `debug/`.
Connect an MCP client to the launcher command. The process waits for the client's initialization request.

Local mode stores data beneath the supplied directory and preserves it across restarts.
Logs are `graph-node.log` and `graph-indexer.log` in that directory.
The launcher creates a temporary private auth-token file and removes it when it exits.
MCP stdout contains only protocol messages. Startup messages go to stderr.
Closing MCP stdin, sending SIGINT, or sending SIGTERM shuts down both children.
If a child exits, the launcher exits and stops the other child.

Local endpoints default to:

| Service | Address | Override |
| --- | --- | --- |
| Bolt | `127.0.0.1:28687` | `GRAPH_BOLT_ADDR` |
| Query HTTP | `127.0.0.1:28443` | `GRAPH_HTTP_ADDR` |
| Node admin | `127.0.0.1:29091` | `GRAPH_ADMIN_ADDR` |
| Indexer admin | `127.0.0.1:29092` | `GRAPH_INDEXER_ADMIN_ADDR` |

Use loopback addresses for local mode. It enables plaintext for local development.
Set all four overrides to distinct unused addresses when running another local stack.

Example MCP client configuration, with absolute paths substituted:

For Claude Code, register the launcher from the repository root:

```sh
just build-mcp
claude mcp add hydradb -- "$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')/debug/hydradb-mcp" --local /absolute/path/to/local-data
```

Other clients can use this configuration:

```json
{
  "mcpServers": {
    "hydradb": {
      "command": "/absolute/path/to/target/debug/hydradb-mcp",
      "args": ["--local", "/absolute/path/to/local-data"]
    }
  }
}
```

## Connect to an existing node

Omit `--local` and configure the existing HTTP endpoint:

```sh
export TURBOLAY_HTTP_URL=https://your-node:8443
export GRAPH_AUTH_TOKEN_FILE=/path/to/auth-token
export GRAPH_NAMESPACE=production
export GRAPH_ID=default
export GRAPH_CELL_ID=cell-0
target/debug/hydradb-mcp
```

`TURBOLAY_HTTP_URL` defaults to `http://127.0.0.1:18443`.
The namespace, graph, and cell default to `default`, `default`, and `cell-0`.
The token file is required. HTTP redirects are disabled.
Access follows the configured node credential's scope grants.

Tenant names use the existing scoped Bolt encoding. For tenant `acme` and subtenant `sales`,
the HTTP namespace is `<GRAPH_NAMESPACE>/YWNtZQ/c2FsZXM`.
This is the same scope as the Bolt database `<GRAPH_ID>.scope1.YWNtZQ.c2FsZXM`.
Both names are required and cannot be empty or contain control characters.
Names containing slashes are encoded as one segment.

## Verify the running stack

```sh
just smoke-mcp
```

This builds the executables and drives MCP over stdio with Python's standard library.
It checks tool discovery, parameterized writes and reads in three scopes, eight rows across three HTTP pages,
authorization, invalid arguments, query errors, published index generations,
persistence after restart, and child shutdown.
It uses a temporary data directory and prints `mcp-smoke-ok` on success.
It does not invoke `cargo test`.

The MCP client starts the launcher as a child so it owns the protocol pipes.
A `just mcp-local` process started by hand waits for JSON-RPC messages on its terminal stdin.
It does not expose an MCP HTTP port for a second client to attach to.
