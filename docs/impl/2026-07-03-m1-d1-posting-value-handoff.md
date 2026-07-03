---
title: "turbolay — M1 Deliverable 1 Handoff: PostingValue + the north-star proptest"
date: 2026-07-03
kind: handoff
status: ready to build (pre-flight cleared)
---

# M1 · Deliverable 1 — `PostingValue` + set-algebra north-star proptest

On-ramp for whoever builds **`src/posting.rs`**: the posting-list value type that
every adjacency and index key stores, plus its RFC 0005 acceptance proptests.
This is the *first* thing built in M1 and the *first* thing tested — everything
downstream (merge fill, add/delete/neighbors, the write path, indexes) reads and
writes this type.

**Binding spec:** RFC 0005 §"Posting value format", §"Splitting supernodes",
§"Set algebra", and Acceptance #1–#3. Read it first; this doc turns it into a
concrete build unit and pins the on-wire format the RFC leaves semi-open.

**Pre-flight is done — build on it, don't redo it** (see
`2026-07-03-m1-spike-and-seqnum-decision.md`):
- `roaring = "0.10"` (workspace dep, resolves 0.10.12) is in. `RoaringTreemap`
  portable serialize/deserialize is confirmed stable, ascending, and cheap.
- `common` is a **vendored fork** at `vendor/common`; turbolay is a workspace.
- `WriteOptions.seqnum` injection landed (matters for D5, not D1).

## Scope — what D1 is and is NOT

**D1 delivers:** the `PostingValue` *type* — its in-memory shape, its on-wire
serialize/deserialize, its read-side set-algebra surface, and the acceptance
proptests (#1 round-trip, #2 set-algebra-vs-oracle, #3 split-lifecycle *reads*).

**D1 does NOT deliver** (later deliverables, keep the seam clean):
- The **merge operator** roaring-union fill — that's D3 (`merge.rs`).
- **add / delete / neighbors / rollup / the split *operation*** — that's D4
  (`posting_ops.rs`). D1 defines how a `Split` value *looks* and how a split
  value *reads*; it does not implement the RMW that *produces* one.
- Any **write-path / batch** wiring — D5 (`write.rs`).
- Reading part values from storage — D1's split reads take the parts as inputs
  (see `read_split` below); the storage `get` per part is D4's job.

Keeping D1 pure-data + pure-function (no `Storage`, no `async`) is deliberate: it
makes the north-star proptest a fast, deterministic, I/O-free oracle test.

## The type

RFC 0005's format, made concrete:

```rust
/// format tag byte. 1 = roaring-v1 (the only value written in v0).
/// Reserved: 2 = UidPack, 3 = CSR (RFC 0009) — the CSR-ready seam.
const FORMAT_ROARING_V1: u8 = 1;

/// A skip-metadata entry for one part of a split (supernode) posting list.
/// min/max/card come from roaring in O(1); a ranged reader skips parts whose
/// [min,max] doesn't overlap its query range without decoding them.
pub struct PartRef {
    pub start_uid: u64,  // reconstructs the part key: edge_part_key(dir,anchor,pred, Uid(start_uid))
    pub min_uid: u64,
    pub max_uid: u64,
    pub card: u32,
}

pub enum PostingKind {
    /// The whole set lives inline in this value.
    Single(RoaringTreemap),
    /// This value is a manifest; the members live in EdgePart[..][start_uid] keys.
    Split(Vec<PartRef>),
}

pub struct PostingValue {
    pub format: u8,       // always FORMAT_ROARING_V1 in v0; reject unknown on decode
    pub kind: PostingKind,
}
```

Consider newtyping or documenting that an **empty `Single` set** is the canonical
"no members" value (vs. an absent key). D4/D5 decide whether an empty posting is
stored or deleted; D1 just must round-trip it.

## On-wire format (pin this — RFC leaves it open)

One value = header + body. **Little-endian** for the manifest integers (the
opendata values-LE house rule; keys are BE for order, values are LE). Roaring's
portable format is its own self-describing layout — write it verbatim.

```
byte 0      : format  (u8)               = 1
byte 1      : kind    (u8)               0 = Single, 1 = Split
Single body : bytes[2..]                 = RoaringTreemap portable bytes
                                           (roaring::RoaringTreemap::serialize_into;
                                            length is implied by the value length)
Split body  : bytes[2..6]                = part_count (u32 LE)
              then part_count × 28 bytes, each PartRef:
                start_uid (u64 LE) | min_uid (u64 LE) | max_uid (u64 LE) | card (u32 LE)
```

- **Decode is total and fail-closed:** unknown `format` → `Error::encoding`; a
  `kind` byte other than 0/1 → error; a truncated body → error. No panics on
  adversarial bytes (mirror the M0 keyspace parser posture and its proptests).
- `serialize` → `Bytes`; `deserialize(&[u8]) -> Result<PostingValue>`. Round-trip
  must be identity for both kinds (acceptance #1).
- Reserve nothing else in the header now — the `format` byte is the only version
  seam v0 needs.

## Read-side API (what D4/D5/D7 will call)

Pure functions on the decoded value; no storage, no async:

```rust
impl PostingValue {
    // constructors
    fn single(set: RoaringTreemap) -> Self;
    fn empty() -> Self;                       // Single(∅)
    fn split(parts: Vec<PartRef>) -> Self;

    // codec
    fn serialize(&self) -> Bytes;
    fn deserialize(bytes: &[u8]) -> Result<Self>;

    // shape
    fn is_split(&self) -> bool;
    fn parts(&self) -> Option<&[PartRef]>;    // Some iff Split
    fn serialized_len(&self) -> usize;        // for the 512 KiB split check (D4)

    // whole-set materialization
    //   Single -> clones/returns the set.
    //   Split  -> unions the provided part sets. D4 supplies them (it did the
    //             per-part storage gets); D1 just folds. Order-independent.
    fn materialize_single(&self) -> Option<&RoaringTreemap>;   // Some iff Single
    fn union_parts(parts: &[RoaringTreemap]) -> RoaringTreemap; // Split helper
}
```

The **set algebra itself is roaring's** — do NOT wrap `&`/`|`/`-`/`len`/`min`/`max`
behind bespoke methods; callers operate on `RoaringTreemap` directly (RFC 0005:
"the set algebra is free and correct"). D1's job is to get the *right*
`RoaringTreemap` out of a value (Single inline, or Split via `union_parts`), not
to re-expose boolean ops. The one-hop `neighbors()` read (RFC 0005 §"Read path
for one hop") lives in D4 because it needs `Storage` gets and the deleted-bitmap
subtraction; D1 gives it `materialize_single` / `union_parts`.

## Where it plugs into the keyspace (context, not D1 work)

The value at these keys is a `PostingValue` (all already built in M0
`serde::keys`): `edge_key(dir, anchor, pred)` (EdgeOut/EdgeIn),
`index_key(pred, token)` (Index), `count_key(...)` (Count). A `Split` value's
parts live at `edge_part_key(dir, anchor, pred, Uid(start_uid))` (EdgePart), each
itself a `Single` `PostingValue`. D1 does not read/write storage — it just must
make `start_uid` round-trip so D4 can rebuild those keys.

## The north-star proptest (write this FIRST, RFC 0005 acceptance #1–#3)

`tests/posting_props.rs` — this is M1's north star, the way
`Index[pred][token]` ordering was M0's. It fails until `PostingValue` exists and
is correct, and it's I/O-free (no `Storage`).

**#1 — round-trip identity.** For random `Vec<u64>` (incl. empty, singletons,
32-bit-boundary values `u32::MAX`, `u32::MAX+1`, `u64::MAX`, duplicates):
`deserialize(serialize(v)) == v` for a `Single`; iteration is strictly ascending.
Repeat for a hand-built `Split` manifest.

**#2 — set-algebra vs `BTreeSet<u64>` oracle.** Generate two random `Vec<u64>`
inputs `a`, `b`. Build `PostingValue::single` for each and `BTreeSet<u64>` oracles.
Assert, over roaring's ops, agreement with the oracle for:
`AND (a&b)`, `OR (a|b)`, `NOT/difference (a−b and b−a)`, `cardinality (len)`,
`min`, `max`. This is the whole traversal/intersection primitive — if it's right,
RFC 0007's read path is built on solid ground.

**#3 — split lifecycle, read side.** Force a **tiny split threshold** in the test
(e.g. split a `Single` whenever `serialized_len` > a few hundred bytes) and build
a `Split` manifest + its part sets by hand (the split *operation* is D4, but you
can construct the post-split shape directly for D1's read test). Assert:
- manifest `min_uid`/`max_uid`/`card` per part match the part's actual roaring
  min/max/len;
- `union_parts(parts)` equals the unsplit oracle set (whole-set read);
- a range-filtered read (union only parts whose `[min,max]` overlaps `[lo,hi]`)
  equals the oracle filtered to `[lo,hi]` (this is the skip-metadata contract the
  `PartRef` exists for).

Use `proptest` (already a dev-dep). Keep the generators small and boundary-heavy;
follow the M0 `tests/keyspace_props.rs` style (given/when/then comments, named
properties). Also add plain `#[test]` unit cases for the decode fail-closed paths
(unknown format, bad kind byte, truncated manifest).

## Constraints & gotchas

- **No `Storage` in D1.** Pure data + pure functions. All tests run without a
  namespace. (Correctness harness is in-memory anyway — RFC 0017 D12: no S3/
  LocalStack.)
- **Fail-closed decode, no panics on bad bytes** — same bar as the M0 parsers.
- **Don't implement split/rollup/add/delete here** — only the value *shape* and
  its reads. Leaving a `// D4: split operation` seam is correct, not incomplete.
- **`serialized_len` is the 512 KiB trigger input** (D4), so it must reflect the
  real on-wire size, not an estimate — compute it from the actual serialization.
- **Values LE, keys BE.** The manifest ints are LE. Don't reintroduce BE here.
- Register nothing, wire nothing — D1 adds `mod posting;` to `lib.rs` and the
  test file, nothing else.

## Definition of done

- `src/posting.rs` with `PostingValue`, `PartRef`, `PostingKind`, the codec, and
  the read-side API above; `pub` where D3/D4/D5 need it.
- `tests/posting_props.rs` green: acceptance #1 (round-trip), #2 (oracle), #3
  (split-lifecycle reads) + fail-closed decode unit tests.
- `cargo test`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` all clean.
- One commit, message noting which RFC 0005 acceptance items it closes.

## After D1
D2 `value.rs` (NodeRecord/TypedValue) can proceed in parallel — it's independent.
D3 `merge.rs` fills the operator using `PostingValue` (roaring-union of operands).
D4 `posting_ops.rs` builds add/delete/neighbors/split/rollup on this type. D5
`write.rs` composes it into the atomic batch on the one-clock seqnum contract.
See `2026-07-03-m1-handoff.md` for the full 1→6 sequence.
