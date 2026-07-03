---
title: "RFC 0003: Keyspace Layout & Order-Preserving Encoding"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0004-graph-data-model-and-write-path.md
  - 0005-posting-list-substrate.md
  - 0006-index-framework.md
---

# RFC 0003: Keyspace Layout & Order-Preserving Encoding

## Summary

SlateDB is a plain ordered-byte-key KV store with **no custom comparators** — key order is unsigned lexicographic byte order, full stop (RFC 0002, accepted constraint). Every logical ordering turbolay needs (UID order for posting-list intersection, numeric order for range predicates, sequence order for the changelog tail, prefix clustering per predicate) must therefore be baked into the *bytes* of the key.

This RFC decides the complete turbolay keyspace: the subsystem byte, the record-type tags, the exact byte layout of every key, and the order-preserving encoding of every component. It builds directly on `opendata`'s `common::serde` toolkit (D8) — we add `SUBSYSTEM GRAPH = 0x05` and the graph record types; we do not invent new encoding primitives.

The design is Dgraph's key layout (`x/keys.go`) adapted to `common::serde`: a predicate-clustered, big-endian, prefix-scannable keyspace where a single range scan walks one predicate's adjacency or index in UID/term order.

## Decision

One SlateDB database per namespace. All keys share the 2-byte `common::serde::KeyPrefix` (`subsystem = GRAPH = 0x05`, `version = 1`), followed by a 1-byte record-type tag, followed by a record-type-specific, order-preserving tail.

Names (labels, edge predicates, property keys) are **interned to `u32` ids** via a schema keyspace — schema-preserving encoding, per the fundamentals-of-graph guidance that schema-free encoding "can easily double storage size because property key strings are repeated for every vertex." Ids are compact, fixed-width, and give clean prefix clustering.

## Key anatomy

```
| KeyPrefix (2B) | RecordTag (1B) | tail... |
  ^ subsystem=0x05                  ^ order-preserving, record-type-specific
    version=0x01
```

- `KeyPrefix` (`common/src/serde/key_prefix.rs`): `[subsystem: u8][version: u8]`, big-endian. `GRAPH = 0x05` is registered in `common::serde::subsystem` (0x01 timeseries, 0x02 vector, 0x03 log, 0x04 keyvalue, **0x05 graph**).
- `RecordTag` (`common/src/serde/record_tag.rs`): record type packed in the high nibble. Reserving a whole tag byte (rather than folding into the version) keeps record types cheap to add and keeps SlateDB prefix-bloom effective per record type.

### Record types (`RecordType`, high nibble of the tag)

| Tag | Record type | Key tail | Value | Owner RFC |
|-----|-------------|----------|-------|-----------|
| `0x1` | `SchemaName` | `[kind:1][name: terminated_bytes]` | `id: u32` (BE) | 0003 |
| `0x2` | `SchemaId` | `[kind:1][id: u32 BE]` | `SchemaEntry` (name + directives) | 0003 |
| `0x3` | `Node` | `[uid: u64 BE]` | `NodeRecord` (label-id set + property blob) | 0004 |
| `0x4` | `EdgeOut` | `[src_uid: u64 BE][pred_id: u32 BE]` | `PostingList` of dst uids | 0004/0005 |
| `0x5` | `EdgeIn` | `[dst_uid: u64 BE][pred_id: u32 BE]` | `PostingList` of src uids | 0004/0005 |
| `0x6` | `EdgePart` | `[EdgeOut\|In][anchor_uid:8][pred_id:4][start_uid: u64 BE]` | posting-list part | 0005 |
| `0x7` | `Index` | `[pred_id: u32 BE][token: order-preserving bytes]` | `PostingList` of node uids | 0006 |
| `0x8` | `Count` | `[pred_id: u32 BE][dir:1][degree: u32 BE]` | `PostingList` of node uids | 0006 |
| `0x9` | `Xid` | `[xid: terminated_bytes]` | `uid: u64` (BE) | 0004 |
| `0xA` | `Log` | `[seq: u64 BE]` | `ChangeRecord` | 0004 |
| `0xB` | `Meta` | `[meta_key: bytes]` | scalar / bitmap / counter | 0003 |

`kind` for schema records: `0=Label, 1=Predicate, 2=PropertyKey`. All three name-spaces are interned independently so a label and a predicate may share a name without colliding.

## Order-preserving component encodings

All from `common::serde`; each is round-trip and ordering property-tested (see Acceptance). **Keys are big-endian; values are little-endian** (the opendata house rule).

### UIDs — `u64` big-endian, fixed 8 bytes
Node/edge ids are internal dense `u64`s (RFC 0004, D5). Big-endian fixed-width so byte order == numeric order, which is the **contract the posting-list set algebra relies on** (RFC 0005): a sorted scan of an adjacency key walks destinations in ascending UID order, and roaring's AND/OR/NOT operate over that ascending-uid ordering. Dense u64s (not UUIDs) are also what make roaring compression and cheap set math work — namidb documented the UUID mistake that forced it into binary-searched `Vec<NodeId>` instead of offset math.

### Interned ids — `u32` big-endian
`pred_id`, `label_id`, `prop_id`. Fixed-width, compact, prefix-clustering. Allocated from the `Predicate`/`Label`/`PropertyKey` id-spaces by `common::SequenceAllocator` (RFC 0004), stamped into the schema keyspace on first use.

### Variable-length strings — `terminated_bytes`
For `xid`, schema names, and `exact`-token index values. `common/src/serde/terminated_bytes.rs` escapes `0x00 → 0x01 0x01`, `0x01 → 0x01 0x02`, and terminates with `0x00`. This guarantees the two invariants a composite key needs: encoded `"a"` is **not a prefix of** encoded `"ab"` (so `"a" < "ab"` holds after a following component is appended), and embedded `0x00` bytes are safe. Both are proptested upstream (`should_preserve_ordering`, `should_prefix_range_contain_all_prefixed_keys`).

### Sequence numbers — `u64` big-endian
Changelog `seq` is fixed 8-byte BE (order-preserving, fixed width) so the tail scan `(W, latest]` is a bounded key-range scan. (We use fixed BE rather than opendata's segmented `var_u64` because turbolay's changelog is a single per-namespace stream, not segmented.)

### Numeric index tokens — `sortable`
Range predicates (`>`, `<`, `>=`, `<=`) become key-range scans **only if numeric tokens sort correctly as bytes**. `common/src/serde/sortable.rs`: `encode_i64_sortable(v) = (v as u64) ^ 0x8000_0000_0000_0000` (flip sign bit); `encode_f64_sortable` flips all bits for negatives, the sign bit for positives — then write big-endian. This is exactly the fundamentals rule: "negative integers must be mapped such that their binary representations sort correctly alongside positive integers, and floating-point values are adjusted to avoid sign-bit reversals." `f64` comparison uses `.to_bits()` so `-0.0`/`+0.0`/NaN are handled by policy (NaN sorts last, decided in Acceptance).

### Range construction — `BytesRange` / `lex_increment`
Prefix scans use `common::bytes::{BytesRange, lex_increment}`. `BytesRange::prefix(p)` computes the exclusive upper bound via `lex_increment` (increment last non-`0xFF`, drop trailing `0xFF`s), and `from_prefix_and_subrange` mirrors SlateDB's `scan_prefix(prefix, subrange)` semantics byte-for-byte. A predicate's whole adjacency is `scan_prefix([tag=EdgeOut][src_uid][pred_id], ..)`; a numeric range is `scan_prefix([tag=Index][pred_id], lo_token..=hi_token)`.

## What is clustered, and why

Because the tag and interned ids are fixed-width and big-endian, the physical key order groups data usefully:

- All of a node's out-edges on one predicate share the key `[EdgeOut][src_uid][pred_id]` → one posting list, one `get`. (Dgraph's core locality property.)
- All index tokens for a predicate are contiguous under `[Index][pred_id]` → a range predicate is one bounded scan.
- All changelog entries are contiguous and seq-ordered under `[Log]` → the tail scan is a single bounded scan.
- `Node`, `EdgeOut`, `EdgeIn` for the same UID are **not** adjacent (different tags). This is deliberate: co-locating them (the "one block read gets vertex + edges" optimization from the fundamentals read-heavy chapter) is a *later* keyspace-interleaving optimization, not v0. v0 keeps record types cleanly separated; the block cache + bloom filters absorb the extra `get`.

## Edge sortkey / ordered adjacency — deferred

Two graph-on-KV models exist (fundamentals ch04/ch13): the **posting-list model** (Dgraph — key = `(pred, src)`, value = sorted set of dst uids, ordered by uid) and the **composite-edge-key model** (JanusGraph/namidb — key = `(src, pred, sortkey, dst)`, one key per edge, natively ordered by an embedded edge property).

turbolay v0 is the **pure posting-list model** (D3): the adjacency key carries **no sortkey**; the posting list is ordered by destination UID; edge properties live in the posting's side-array (RFC 0005), keyed by dst uid. Cypher `ORDER BY` on an edge property is therefore a materialize-then-sort in the executor for v0. Embedding a sortkey in the key for natively-ordered adjacency (and the resulting one-key-per-edge tradeoff) is a documented optimization for the CSR/ordered-adjacency RFC (0009), not v0. *(This supersedes the `[src_uid][pred][sortkey]` sketch in `plan.md` §4.2.)*

## Merge-operator dispatch table (D11)

`MergeOperator` is registered once on the `StorageBuilder` **and every `DbReader`/compactor** (opendata requirement). It routes on the record tag; a merge operand on any other record type is a bug and returns `MergeOperatorError` (fail-closed). Operands must be associative.

| Record type / meta-key | Operand kind | Merge semantics |
|---|---|---|
| `Meta["deleted_nodes"]` | roaring `Treemap` (u64) | set union — tombstoned node uids, filtered at read (RFC 0004) |
| `Meta["deleted_edges"/pred_id/anchor_uid]` | roaring `Treemap` | set union — per-(anchor,pred) deleted dst/src uids (RFC 0005) |
| `EdgeOut`/`EdgeIn` (fast-add path) | roaring set union (u64) | associative merge into the set; resolved at read/compaction (RFC 0005 decides add-via-merge vs RMW) |
| `Meta["count"/pred_id]`, corpus counters | `i64` LE | sum (degree/edge-count statistics) |
| `Index`, `Count`, `Node`, `Schema*`, `Xid`, `Log` | — | **no merge**; last-write-wins via `put` only. Split/rollup and index maintenance are single-writer RMW. |

The non-associative work (posting-list bin-split at 512 KiB, delta→complete rollup, moving a uid between count buckets) is done by the single writer as read-modify-write — safe precisely because there is one writer per namespace (D2). This is the RFC 0002 §constraints commitment made concrete.

## Schema keyspace (name interning)

The schema is tiny (one entry per distinct label/predicate/property name) and fully cached in memory by the writer and each reader; the KV records are the durable source of truth.

```
SchemaName [kind][name]  -> u32 id          (lookup: name -> id, on write)
SchemaId   [kind][id]    -> SchemaEntry      (lookup: id -> name + directives, on read/return)

SchemaEntry {
  name: String,
  value_type: ValType,        // for predicates carrying scalar values
  directives: {               // index build flags (RFC 0006)
    index: Vec<Tokenizer>,    // exact/term/hash/int/float
    reverse: bool,            // materialize EdgeIn (v0: always true — D10)
    count: bool,              // maintain Count index
    list: bool,               // multi-value predicate
  },
}
```

Interning happens on the write path: an unknown name is allocated an id (`SequenceAllocator`) and its `SchemaName`/`SchemaId` records are written **in the same `WriteBatch`** as the data that first used it, so schema and data commit atomically (no dangling id). Recovery re-reads the schema on open.

## Acceptance

Exhaustive + property tests (proptest, the opendata convention) proving `encoded byte order == logical order` for every component and every boundary:

1. **UID ordering**: `a.cmp(&b) == encode_u64_be(a).cmp(&encode_u64_be(b))` across the full u64 range and boundaries (0, 1, 2^32−1, 2^32, u64::MAX).
2. **`terminated_bytes` sort-safety**: `enc("a") < enc("ab")` after appending a following component; embedded `0x00`/`0x01` round-trip; the prefix-range containment invariant (reuse the upstream proptests).
3. **`sortable` numerics**: `.windows(2)` ordering sweep over i64 and f64 including negatives, ±0.0, subnormals, ±∞; NaN policy asserted (sorts last, never matches a range).
4. **Composite key ordering**: for `[Index][pred_id][token]`, prove a fixed `pred_id` clusters all its tokens and that within it the token order equals logical value order — for each tokenizer.
5. **Range totality**: `BytesRange::prefix` / `from_prefix_and_subrange` contain exactly the intended keys and nothing from an adjacent tag or predicate (boundary: `pred_id` and `pred_id+1`; tag and `tag+1`).
6. **Round-trip**: every key/value type `decode(encode(x)) == x`, and every `ParsedKey`-equivalent parser is the exact inverse of its builder.

## Alternatives considered

- **Inline predicate strings in keys (Dgraph's literal layout).** Dgraph stores the predicate string (length-prefixed) in every key and leans on prefix bloom filters. Amortized over a posting list it's tolerable, but it bloats index keys and the changelog, and complicates fixed-width parsing. Rejected in favor of `u32` interning (schema-preserving), which the fundamentals guidance explicitly recommends at scale. The schema cache makes the name↔id lookup free in the hot path.
- **`var_u64` sequence keys (opendata `log` layout).** opendata's log segments its keyspace and uses relative `var_u64` seqs. turbolay's changelog is a single per-namespace stream, so a fixed 8-byte BE seq is simpler and equally order-preserving. Revisit only if the changelog is ever segmented.
- **Composite-edge-key model (sortkey in key, one key per edge).** Gives natively-ordered adjacency but is the opposite storage model (D3) and multiplies key count by edge count — reintroducing the tiny-object pressure we avoid. Deferred to RFC 0009 as an ordered-adjacency optimization.
- **Co-locating Node + EdgeOut + EdgeIn under one UID prefix.** The "one block read gets vertex and its edges" locality optimization. Deferred; v0 keeps record types separate and relies on the block cache. Revisit if RFC 0017 shows first-hop `get` latency dominating.

## Final contract

- One SlateDB DB per namespace; every key is `[GRAPH=0x05][ver=1][RecordTag][order-preserving tail]`.
- Names (label/predicate/property) are interned to `u32` ids in a schema keyspace, written atomically with the data that first uses them.
- UIDs are dense `u64` big-endian; numeric index tokens use `sortable`; variable-length components use `terminated_bytes`; changelog seq is `u64` BE. All encodings come from `common::serde` and are ordering-proptested.
- The pure posting-list model has no sortkey in the adjacency key (deferred to 0009).
- `MergeOperator` is routed by record tag and accepts operands only for the associative record types in the dispatch table; all non-associative maintenance is single-writer RMW (D2, D11).
