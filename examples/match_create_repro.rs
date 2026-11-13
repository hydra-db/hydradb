//! Reproduces the "MATCH + CREATE is not supported by the mutation engine"
//! limitation reported against the graph-node write engine.
//!
//! The HTTP query API and the Bolt path both classify a write query and then
//! hand it to `parse_opencypher_mutation_query_with_parameters` to decide
//! whether the mutation engine can execute it (see
//! `src/client/service.rs` -> "write query is not executable by the mutation
//! engine"). That parser is the exact decision point, so we exercise it
//! directly here — no object store, no server, no I/O — and print the verdict
//! for each query shape called out in the report.
//!
//!   just check-examples        # or, to actually run it:
//!   cargo run --example match_create_repro --features opencypher
//!
//! A query is executable by the mutation engine iff the parser returns
//! `Ok(Some(_))`. `Ok(None)` is what the client turns into
//! "write query is not executable by the mutation engine"; `Err(_)` is a
//! specific unsupported-feature rejection surfaced verbatim to the caller.

use std::collections::BTreeMap;

use hydradb::{parse_opencypher_mutation_query_with_parameters, parse_opencypher_row_query};

fn main() {
    let params = BTreeMap::new();

    println!("== READS (row engine) ==\n");
    for query in [
        "CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 2})", // create is a write, shown below
        "MATCH (a {id: 1})-[:FOLLOWS]->(b) RETURN b.id AS id",
    ] {
        // Only the second is a read; parse_opencypher_row_query rejects the
        // CREATE, which is why we route writes through the mutation parser.
        match parse_opencypher_row_query(query) {
            Ok(_) => println!("  ROW OK        {query}"),
            Err(e) => println!("  not a read    {query}\n                -> {e}"),
        }
    }

    println!("\n== WRITES (mutation engine) ==\n");
    let writes = [
        // Works: a whole subgraph created fresh in one shot.
        ("create-fresh-subgraph", "CREATE (a {id: 1})-[:FOLLOWS]->(b {id: 2})"),
        // THE BUG: attach a new edge/node to already-existing data.
        ("match-then-create", "MATCH (x {id: 1}) CREATE (x)-[:REL2]->(z {id: 3})"),
        // Reported: string id property instead of integer.
        ("create-string-id", "CREATE (a {key: \"abc\"})-[:REL]->(b {key: \"xyz\"})"),
        // Reported: two CREATE clauses chained in one query.
        ("create-then-create", "CREATE (a {id: 1}) CREATE (b {id: 2})"),
        // For contrast: MATCH ... SET / DELETE *is* accepted post-MATCH.
        ("match-then-set", "MATCH (x {id: 1}) SET x.seen = 1"),
        // WORKAROUND for match-then-create: vertices are addressed by their
        // integer `id`, so referencing the existing node's id in a plain
        // single-clause CREATE attaches the new edge/node to it — no MATCH.
        ("attach-by-id", "CREATE (x {id: 1})-[:REL2]->(z {id: 3})"),
        // MERGE probes: which shapes does the mutation engine accept?
        ("merge-edge", "MERGE (a {id: 1})-[:REL2]->(b {id: 3})"),
        ("merge-single-node", "MERGE (a {id: 1})"),
        ("merge-on-create", "MERGE (a {id: 1})-[:REL2]->(b {id: 3}) ON CREATE SET b.new = 1"),
        ("merge-string-id", "MERGE (a {key: \"abc\"})-[:REL2]->(b {key: \"xyz\"})"),
    ];

    for (name, query) in writes {
        match parse_opencypher_mutation_query_with_parameters(query, &params) {
            Ok(Some(_)) => {
                println!("  [{name}] ACCEPTED by mutation engine");
            }
            Ok(None) => {
                println!("  [{name}] REJECTED (Ok(None))");
                println!(
                    "        client/Bolt surfaces: invalid_request \
                     \"write query is not executable by the mutation engine\""
                );
            }
            Err(e) => {
                println!("  [{name}] REJECTED: {e}");
            }
        }
        println!("        query: {query}\n");
    }
}
