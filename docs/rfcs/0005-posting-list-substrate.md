---
title: "RFC 0005: Posting-List Substrate (Roaring Adjacency & Index Sets)"
status: draft
date: 2026-07-03T00:00:00Z
related:
  - 0000-rfc-index.md
  - 0003-keyspace-and-encoding.md
  - 0004-graph-data-model-and-write-path.md
  - 0006-index-framework.md
  - 0007-opencypher-read-path.md
---

# RFC 0005: Posting-List Substrate

## Summary

The posting list is turbolay's unit of storage, mutation, and traversal: the value at an adjacency or index key is a **compressed sorted set of `u64` UIDs**, and a graph edge (or an index membership) is a member of one of those sets. This RFC decides the concrete representation, how sets are added to / deleted from / split, and the set-algebra the read path (RFC 0007) runs on top.

**Decision (Q1, Q2):** posting lists are **roaring bitmaps** (`RoaringTreemap`, 64-bit), stored on SlateDB, with the value carrying a small header so a future CSR / UidPack representation is an additive format bump ("CSR-ready", not CSR). This deliberately **replaces Dgraph's UidPack codec and its `algo/uidlist` intersection engine** with roaring: intersection/union/difference come from the library, the deleted-uid bitmaps use the same type, and there is no bespoke delta-codec to port. Dgraph's *structure* (predicate-clustered posting lists, 512 KiB split, delta-then-rollup) is kept; its *encoding* is not.

## Why roaring, not UidPack

Dgraph packs UIDs as 256-uid blocks of group-varint deltas (`codec.go`) and ships a matching block-skipping intersection engine (`algo/uidlist.go`). That buys ~13% compression and block-max metadata for WAND — both of which only matter at the scale and query-latency profile we have explicitly deferred (RFC 0000 correct-first ledger). For a correctness-first v0:

- **Roaring is purpose-built** for compressed sorted-integer sets with fast boolean ops — exactly graph adjacency plus traversal intersection.
- **The set algebra is free and correct**: `AND = a & b`, `OR = a | b`, `NOT/difference = a - b`, cardinality, rank/select, min/max — no hand-written merge-joins to get subtly wrong.
- **One representation everywhere**: adjacency sets, index posting lists, deleted-node and deleted-edge bitmaps are all roaring, so add = union, delete = difference, uniformly.
- **Less code to M1**: we skip porting `codec.go` and `uidlist.go` entirely.

If RFC 0017 later shows posting decode or intersection dominating on real S3, swapping a hot posting kind to UidPack/CSR is a format-tag change behind the same `PostingList` interface (RFC 0009), not a rewrite. That is what "CSR-ready" buys.

## Posting value format

Every adjacency (`EdgeOut`/`EdgeIn`) and index (`Index`/`Count`) key's value is a `PostingValue`:

```
PostingValue {
  format: u8,     // 1 = roaring-v1. Reserved: 2 = UidPack, 3 = CSR (RFC 0009). Written from day one.
  kind:   u8,     // Single | Split
  // Single:
  set: RoaringTreemap,           // serialized (roaring's portable format)
  // Split (supernode): the main key holds only a parts manifest:
  parts: Vec<PartRef { start_uid: u64, min_uid: u64, max_uid: u64, card: u32 }>,
}
```

- The **`format` tag** is the CSR-ready seam: a 1-byte write-only cost today that lets a later RFC introduce UidPack/CSR values without a dual-read migration on the whole keyspace.
- The **`PartRef` min/max/card** is the CSR-ready skip metadata (roaring gives min/max/cardinality in O(1)): a future ranged/leapfrog reader can skip whole parts by uid range without decoding them. Written from day one, exploited only in RFC 0009.
- **Edge facets** (properties on an edge) are *not* in the set — the set is pure membership. A faceted edge stores its properties in a companion record `EdgeProp[src_uid][pred_id][dst_uid] → facets` (RFC 0004). This mirrors Dgraph's pack-vs-postings split (plain edges cost membership only; faceted edges pay for a companion record).

  **Amendment (Workstream A, M1 Wave 4):** this RFC originally specified a per-projection, two-copy key, `EdgeProp[dir][anchor_uid][pred_id][neighbor_uid]` (one copy keyed from each of `EdgeOut`/`EdgeIn`). The implementation instead keys the companion **once**, by full edge identity, `EdgeProp[src_uid][pred_id][dst_uid]` — a single copy, not two. This is a deliberate divergence: a single copy avoids the double-write/update anomaly a two-copy scheme invites (the two copies drifting out of sync if a future write path ever updates one without the other), at the cost of directionality — facets are still O(1) point lookups from either projection (an in-edge read flips to the canonical `(src, pred, dst)` key), but an **in-direction prefix scan** over a destination's incoming facets (e.g. "every faceted edge pointing at `dst`, regardless of source") is not supported by this key shape; only the out-direction `(src, pred)` prefix scan is (see [`edge_prop_range`](../../src/serde/keys.rs)).

## Splitting supernodes (512 KiB)

Kept from Dgraph, simpler on S3 (RFC 0002 / fundamentals ch18: new parts are new keys, no in-place rewrite):

- A posting whose serialized `set` exceeds **512 KiB** is **bin-split** at a pivot uid into part-keys `EdgePart[dir][anchor][pred][start_uid]`, each a `Single` `PostingValue`. Roaring's `rank`/`select` picks the median-cardinality pivot; recurse until no part exceeds the threshold.
- The main key's value becomes `kind = Split` carrying the `parts` manifest (start/min/max/card per part). A read that needs the whole set reads the parts named in the manifest; a read filtered to a uid range reads only the parts whose `[min,max]` overlaps.
- Split is **single-writer RMW** (Q10) — non-associative, safe because there is one writer (D2, D11). It runs when an add pushes a `Single` over the threshold, or at rollup.

## Add / delete mechanics

### Add (Q10 — size-adaptive)
- **Small / `Single` list**: `batch.merge(key, roaring_singleton(neighbor))` — the merge operator (RFC 0003 dispatch) unions the operand into the set. Associative, no read on the hot write path.
- **At the split boundary**: when the writer observes (via the merge-resolved size, or a periodic check) that a `Single` set has crossed 512 KiB, it does one RMW to bin-split into parts. Subsequent adds `merge` into the appropriate part key (chosen by the manifest's uid ranges).

### Delete (Q9 — deleted-edge bitmap)
- `DeleteEdge` records `batch.merge(Meta["deleted_edges"/pred_id/anchor_uid], roaring_singleton(neighbor))` for both directions (RFC 0004). Nothing in the adjacency posting is touched — O(1) regardless of degree.
- Reads compute `live = set − deleted_edges − deleted_nodes` (roaring difference; RFC 0004's deleted-node bitmap subtracts tombstoned endpoints in either direction).
- **Rollup** (below) folds the deleted bitmap into the set and clears it.

### Rollup (replaces Dgraph's timed `IncrRollup`)
There are no MVCC version stacks to collapse (D2/D4). "Rollup" here is a maintenance RMW the single writer runs when a posting's deleted bitmap grows past a ratio (default: deleted-cardinality > 25% of live, tunable in RFC 0017): read set + deleted, write `set − deleted`, clear the deleted bitmap, re-split/merge parts if the size crossed a threshold. It is naturally a candidate to piggyback on SlateDB compaction cadence, but is applied by the writer, not the compactor, because it is non-associative.

## Set algebra (the traversal primitive)

The read path (RFC 0007) is entirely: read posting lists, then combine them.

| Operation | Roaring | Used for |
|---|---|---|
| membership / neighbors | deserialize + `− deleted` | one hop |
| AND (intersection) | `a & b` | `WHERE` predicate ∧, multi-index anchor |
| OR (union) | `a \| b` | `WHERE` predicate ∨, `IN`, multi-part read |
| NOT (difference) | `a − b` | `WHERE` predicate ¬, deleted subtraction |
| cardinality | `len()` | degree, count index, planner selectivity |
| min/max/rank/select | O(1)/O(log) | part skipping, pagination |

Multi-way AND is applied **smallest-first** (roaring intersections are cheapest when the smaller set drives) — the planner (RFC 0007) uses `len()` to order them. This is the whole of Dgraph's `IntersectSorted`/`IntersectCompressedWith` replaced by library calls; the block-skipping those functions did by hand is roaring's internal container arithmetic.

## Read path for one hop

```
fn neighbors(anchor_uid, pred_id, dir, deleted_nodes: &RoaringTreemap) -> RoaringTreemap:
    let v = get(Edge{dir}[anchor_uid][pred_id])?          // one get (or manifest + parts)
    let mut set = match v.kind {
        Single => v.set,
        Split  => union(v.parts.map(|p| get(EdgePart[..][p.start_uid]).set)),
    };
    if let Some(del) = get(Meta["deleted_edges"/pred_id/anchor_uid]) { set -= del }
    set -= deleted_nodes;                                  // tombstoned endpoints
    set
```

`deleted_nodes` is read once per query and cached (it is a per-namespace bitmap). A whole-set read of a split supernode is a manifest `get` + one `get` per part; a uid-range-filtered read (e.g. an intersection where the other side is small) reads only overlapping parts.

## Shared abstraction

RFC 0006 builds value/reverse/count indexes on the **same** `PostingList` type and the same split/add/delete/rollup machinery — an index posting list is a set of node UIDs keyed by `Index[pred_id][token]` instead of by `Edge[anchor][pred]`. Nothing index-specific lives here; this RFC owns the set representation and its lifecycle, RFC 0006 owns what the sets *mean*.

## Deferred: ordered adjacency (Q4)

v0 orders a posting list by UID only. Natively-ordered adjacency — retrieving edges pre-sorted by an edge property (weight, timestamp) without a materialize-then-sort — requires the **composite-edge-key model** (sortkey embedded in the key, one key per edge; fundamentals ch04/ch13, JanusGraph/namidb). That is the opposite storage model and multiplies key count by edge count, so it is **out of v0 scope**. Cypher `ORDER BY` on an edge property is handled by the executor materializing and sorting the (bounded) result (RFC 0007).

When ordered adjacency is needed at scale, RFC 0009 adds it as a per-predicate opt-in secondary layout (a `format`/keyspace variant), leaving the default posting-list model untouched. Recorded here so the seam is explicit: the `format` tag and the `EdgeProp` companion records already give us a place to hang an ordered variant without disturbing existing data.

## Deferred: UidPack / CSR / block-max (RFC 0009, 0010)

The `format` tag reserves `2 = UidPack`, `3 = CSR`; `PartRef.min/max/card` reserves the skip metadata; block-max/WAND scoring is a fulltext concern that arrives with the fulltext index extension (RFC 0015), not adjacency. None of these block v0; all are additive behind the `PostingList` interface.

## Acceptance

1. **Round-trip & ordering**: `PostingValue` serialize/deserialize is identity; roaring set iteration is ascending-uid (the RFC 0003 contract).
2. **Set-algebra equivalence**: AND/OR/NOT/difference vs a naïve `BTreeSet<u64>` oracle over randomized inputs (proptest).
3. **Split lifecycle exercised from day one**: force the threshold low in tests; assert a growing list splits into parts, the manifest min/max/card are correct, whole-set and range-filtered reads return the same members as an unsplit oracle, and re-merge on shrink works.
4. **Delete correctness**: interleave adds and deletes; assert `live = set − deleted` matches the oracle in both directions; assert rollup folds the bitmap and clears it without changing the visible set.
5. **Supernode add is O(1)-ish**: adding to a split list touches one part key + the (possibly) manifest, not the whole set; adding via merge does not read the set.
6. **Facet companion**: a faceted edge stores/reads its properties from `EdgeProp` while membership stays in the set; a plain edge writes no `EdgeProp`.

## Final contract

- Posting values are roaring `RoaringTreemap`s with a `format` tag (CSR-ready) and, when split, a parts manifest with per-part min/max/card skip metadata.
- Adds are size-adaptive (roaring-union merge small, single-writer RMW at the 512 KiB bin-split). Deletes are per-`(anchor,pred)` deleted-edge bitmaps subtracted at read and folded at rollup; node deletes subtract the deleted-node bitmap.
- Set algebra (AND/OR/NOT/card/min-max) is roaring's, applied smallest-first — this is the entire traversal/intersection primitive, replacing Dgraph's UidPack + `uidlist` engine.
- Ordered adjacency (sortkey) and UidPack/CSR/block-max are deferred behind the `format` tag and companion-record seams (RFC 0009/0010), non-blocking for v0.
- The same `PostingList` type and lifecycle back the indexes in RFC 0006.
