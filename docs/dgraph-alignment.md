---
title: "turbolay — Dgraph Alignment Ledger"
status: living
date: 2026-07-05
related:
  - goals.md
  - plan.md
  - impl/2026-07-05-graphblas-experiment-sketch.md
---

# How closely does turbolay align with Dgraph?

Verified against the Dgraph checkout (`../dgraph`) on 2026-07-05. Short
answer: **very closely at the storage layer, deliberately not at all at the
distribution layer, and less closely at the data-model layer than "Dgraph's
storage model reimplemented" suggests.** The divergences are all deliberate,
but two of them have runtime consequences worth re-checking after
implementation (§4).

## 1. Ported nearly verbatim (the storage geometry)

| Dgraph | turbolay | Fidelity |
|---|---|---|
| `x/keys.go` — `DataKey`/`IndexKey`/`ReverseKey`/`CountKey` | `keys.rs` — Node/EdgeOut/EdgeIn/Index/Count | Same key taxonomy, same "key design is query design" role |
| `posting/list.go:44` — `maxListSize = mb / 2` | 512 KiB split threshold (RFC 0005) | **Exact same constant**; `binSplit` ↔ EdgePart + manifest |
| Indexes-as-posting-lists, tokenizers (exact/term/hash/int/float), count index, lossy-token re-fetch | RFC 0006 | Straight port of the index model |
| Dense internal u64 uids + external id mapping | Uid + `Xid` record type | Same idea; turbolay's xid→uid is more first-class than Dgraph's app-level xidmap |
| Tombstone-and-filter deletes, no eager cascade | deleted-bitmap + vacuum (RFC 0012) | Aligned in spirit |

## 2. Deliberately deleted (the distributed half — the point of the project)

Raft (`worker/draft.go`), the Zero timestamp oracle, `start_ts`/`commit_ts`,
conflict OCC, MVCC-in-key (`posting/mvcc.go`, `posting/oracle.go`), and the
Badger value log. All exist in Dgraph to serialize *concurrent distributed
writers*. Single-writer-per-namespace + SlateDB manifest-epoch fencing (D2)
gets serialization by construction. See `goals.md` §derivation chain.

## 3. Replaced mechanisms (same role, different machinery)

- **UidPack (`codec/codec.go`) + `algo/uidlist.go` → roaring.** Same job —
  compressed uid sets + set algebra — off-the-shelf instead of hand-rolled.
- **Badger → SlateDB.** Delta-then-rollup-on-timestamp → associative merge
  operators + single-writer RMW; "rollup" maps onto SlateDB compaction.

## 4. Looser alignment — re-check these after implementation

**4a. turbolay is a property graph; Dgraph is triples.** Dgraph has *no node
record* — a "node" is the union of its predicates, and node properties are
**value postings** (`Posting.value`, `pb.proto:347`) flowing through the same
posting-list machinery. turbolay materializes `Node[uid]` = labels + property
blob. That is Neo4j's data model on Dgraph's storage geometry. Consequence
for M2: Dgraph indexes value postings directly; turbolay's index extraction
must decode the node blob (a codec-cost seam RFC 0017's `encode_node` /
build-tick timers exist to watch).

**4b. Facets diverge structurally — including Wave 4's EdgeProp.** Dgraph
stores facets **inline in the posting-list value**: `PostingList{pack,
postings, splits}` (`pb.proto:402`) is a hybrid container — plain uids in the
UidPack, faceted/valued edges as full `Posting` messages (facets field) *in
the same KV value*. turbolay externalizes facets to a companion `EdgeProp`
key (Wave 4). RFC 0005's "mirrors Dgraph's pack-vs-postings split" is true in
spirit (plain edges cost membership only), not in mechanism.

- **The trade:** Dgraph reads facets *for free* with the posting list;
  turbolay pays **one extra point-get per faceted edge read**. For the RAG-KG
  workload (`RELATES` edges carry properties) this is real read amplification
  on the traversal path. Mitigations: batch gets, block cache, and the
  `edge_prop_range(src, pred)` prefix scan. **Measure it on real S3** (RFC
  0017) before concluding either way.
- **The win:** turbolay's posting values stay pure roaring sets (cheap set
  algebra, clean merge semantics), and a faceted supernode doesn't bloat the
  adjacency value. Single-copy `[src][pred][dst]` is also better normalized
  than Dgraph, which *duplicates* facets into the `@reverse` posting.

**4c. Smaller divergences.**
- Predicates are interned `u32`s in keys; Dgraph embeds the predicate
  *string* in every key. turbolay strictly better on key size.
- Reverse edges unconditional; Dgraph's `@reverse` is opt-in.
- Query surface: openCypher subset, not DQL/GraphQL±.

## Net

Storage geometry — keyspace roles, posting-list model, split threshold, index
taxonomy — is Dgraph to the letter. The data semantics on top (node records,
companion facets, Cypher) are property-graph, not Dgraph. The distribution
half is absent by design. The one divergence with a measurable runtime cost
is **4b**, and it lands in Wave 4 — revisit this note when the first real-S3
traversal numbers exist.
