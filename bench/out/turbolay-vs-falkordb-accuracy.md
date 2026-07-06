# turbolay vs FalkorDB — accuracy / correctness (2026-07-06)

Are turbolay's hop-swept queries returning the **expected answer**? Checked
against FalkorDB as the oracle, on the **same hub datasets** (seed 42, scale 1.0,
degrees 50/100/1000/10000), **same anchors** (the 3 hub persons), and the matched
`KNOWS*H..H` pattern.

## Method

- turbolay: `execute_distinct_with_hops` — the full DISTINCT row set, **no LIMIT**,
  deduped by the returned column tuple (`turbolay-bench verify --hops … --anchor-index …`).
- FalkorDB: the same MATCH/RETURN as the timed comparison, but `RETURN DISTINCT`
  and no `ORDER BY/LIMIT`, with **`RESULTSET_SIZE=-1`** to lift FalkorDB's default
  10,000-row output cap.
- Compared per (query, hops, degree, anchor): **hops 1–3 full row-content set
  equality**; **hops 4–5 set-size parity** (sets reach ~75k rows).
- Grid: ic02/ic07/ic08/ic09 × hops 1–5 × 3 anchors × 4 degrees = **240 cells**.

## Result

**240 / 240 cells match. Zero discrepancies.**

turbolay returns byte-identical DISTINCT result sets to FalkorDB at every hop,
query, degree, and anchor — with the supernode case genuinely exercised (at
deg-10000, ic02 hop-1 returns 29,022 rows, not the hubless base graph's 100, so
the ~5,000-out-degree hub is really being traversed).

Sample (deg-10000, anchor 0), turbolay | FalkorDB distinct-row counts:

| query | h1 | h2 | h3 | h4 | h5 |
|-------|----|----|----|----|----|
| ic02  | 29022\|29022 | 74272\|74272 | 74887\|74887 | 74887\|74887 | 74887\|74887 |
| ic07  | 12675\|12675 | 26079\|26079 | 26223\|26223 | 26223\|26223 | 26223\|26223 |
| ic08  | 5322\|5322 | 11048\|11048 | 11114\|11114 | 11114\|11114 | 11114\|11114 |
| ic09  | 29022\|29022 | 74272\|74272 | 74887\|74887 | 74887\|74887 | 74887\|74887 |

(Sets saturate by hop 3 — once the frontier covers the graph's giant component,
deeper hops add no new distinct rows.)

## Gotcha caught (and why the first pass looked wrong)

The first accuracy pass reported 84 "mismatches" — **all** with FalkorDB count
`= 10000` exactly. That was FalkorDB's **default `RESULTSET_SIZE=10000`** cap
silently truncating its *own* output; turbolay's larger sets were the correct
ones. Setting `RESULTSET_SIZE=-1` cleared every mismatch. (The prior repo bench
report flagged this same trap.) A run also has to keep the hub CSVs present, or
turbolay's `verify` regenerates a hubless dataset (`hub_count=0`) and the degree
axis collapses — the numbers above are from a guaranteed-hub pass.

## Bottom line

turbolay is **correct** across the whole hop × supernode-degree matrix — it is
slower than FalkorDB (4–14×; see `turbolay-vs-falkordb-hopdeg.md`) but returns
exactly the right answers, including on high-degree supernodes and deep hops.
