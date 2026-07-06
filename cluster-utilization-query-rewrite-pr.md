# Cluster-utilization: push the cluster filter down (console query rewrite)

**Standalone, console-only change — no DB/index change.** The cluster-detail
page recomputes a whole-fleet utilization rollup on every load and filters to the
one cluster being viewed *last*. Moving that filter to the front makes the heavy
work scale with the **one cluster** instead of the **whole fleet**: **~8–15× lower
latency**, widening as the fleet grows, measured against the real query shape.

## The change

The query (`buildReplicaUtilizationHistoryQuery`) is parameterized by a single
cluster, but `WHERE cluster_id IN (…)` only enters at the final join, so the
optimizer can't push it into the shared `replica_utilization_history_binned` CTE
that the five Top-1 argmax CTEs all read. Three edits fix it:

1. Filter `replica_history` to the target cluster up front (both UNION branches).
2. Restrict the offline-events scan to that replica set.
3. Drop the redundant `replica_history` re-join in the binned CTE.

Same result rows; only execution changes. No builtin view or index involved.

## EXPLAIN proof (the filter moves from top to leaf)

- **Old** — `Filter: (#14{cluster_id} = "c0")` sits at the **top** of the plan,
  after five `Consolidating Monotonic Top1` operators that each read the
  whole-fleet binned data. The fleet-wide rollup runs, then we discard all but
  one cluster.
- **New** — `pushdown=(… #1{cluster_id} = "c0")` on the **source read of
  `replica_history`**. The metrics join, the group-aggregate, and all five Top-1s
  then only ever process one cluster's replicas.

## Sweep: old vs new (both unindexed ad-hoc peeks)

p50 latency (ms). "Concurrency N" = N cluster pages opened at the same time
(closed-loop; p50 ≈ what each user waits when N load together). Bench cluster
`scale=1,workers=1` (matches `mz_catalog_server`), 14-day window, 1-hour buckets,
5-min scrape, 2 replicas/cluster.

**OLD — filter last (recompute whole fleet):**

| clusters (replicas) | 1 | 4 | 16 | 32 |
|---|---|---|---|---|
| 25 (50)   | 3,684 | 14,400 | 57,809 | 117,630 |
| 100 (200) | 14,830 | 58,548 | 235,391 | 483,997 |
| 200 (400) | 29,278 | 131,883 | _not run¹_ | _not run¹_ |

**NEW — pushdown (recompute one cluster):**

| clusters (replicas) | 1 | 4 | 16 | 32 |
|---|---|---|---|---|
| 25 (50)   | 477 | 1,561 | 5,989 | 12,219 |
| 100 (200) | 1,199 | 4,560 | 17,890 | 38,499 |
| 200 (400) | 2,245 | 8,499 | 34,520 | 101,955 |

**Speedup (old p50 / new p50):**

| clusters | 1 | 4 | 16 | 32 |
|---|---|---|---|---|
| 25  | 7.7× | 9.2× | 9.7× | 9.6× |
| 100 | 12.4× | 12.8× | 13.2× | 12.6× |
| 200 | 13.0× | 15.5× | — | — |

¹ old @200/conc16–32 skipped: at ~30 s per single query they'd take 15–30+ min
per cell and risk OOMing the test stack. The trend is linear in concurrency
(old @200 ≈ 29 s × N), i.e. ~8 min / ~16 min — the point is already made.

## Analysis

- **The win is structural: old scales with the whole fleet, new with one
  cluster.** Single-user latency: old grows ~linearly with fleet
  (3.7 s → 14.8 s → 29 s across 25 → 100 → 200 clusters, ≈8× over 8× clusters);
  new grows far slower (0.48 s → 1.2 s → 2.2 s, ≈4.7×). So the speedup **widens
  with scale** (7.7× → 13× → 15×). In a real deployment with hundreds of clusters
  viewing 1–2 replicas, the ratio is larger still.

- **Both still degrade under concurrency — but new from a ~10× lower base.** These
  are unindexed peeks on a single-worker catalog server, so N simultaneous loads
  serialize either way. The rewrite doesn't make it concurrency-flat; it makes
  every load an order of magnitude cheaper, so the same hardware absorbs ~10×
  more before falling over. (Concurrency-flatness is what the *separate* indexed
  builtin-view work buys — see below.)

- **In production the rewrite is even better than this bench shows.** These bench
  tables are unindexed, so new still full-scans `replica_metrics_history` — that
  residual scan is why new's latency still creeps up with fleet size. The real
  `mz_cluster_replica_metrics_history` is indexed on `replica_id`, so the pushed-
  down replica set turns that scan into a delta-join lookup, and the offline
  status full-scan into a lookup too. Expect new's fleet-size growth to largely
  flatten and the absolute numbers to drop.

- **Scope of impact:** the console uses this recompute query for every timeframe
  except "Last 14 days" (which already reads the indexed view). So this rewrite
  improves all the short-window cluster-detail loads, today, with no migration.

## Recommendation

**Ship this as the standalone first PR.** It's a console-only query change, no
catalog/DB migration, low risk (identical output, verified row-for-row on
synthetic fleets), and an 8–15× latency win that grows with fleet size. It stands
on its own.

The follow-up — three maintained, indexed builtin views read via a `cluster_id`
point lookup — is what additionally makes the path **concurrency-flat and
sub-second** (it removes the per-load recompute entirely). The rewrite and the
indexed views compose: the rewrite helps every non-14d timeframe immediately;
the views make the whole thing scale under many concurrent viewers.

## Methodology notes

- Both variants are ad-hoc peeks with **no index** — this isolates the *query
  rewrite*, not indexing.
- Latencies are closed-loop: because one query exceeds the 10 s measurement
  window, completed-count ≈ concurrency (each worker finishes ~1 query), so the
  numbers read as "time-to-result when N pages load simultaneously," not
  sustained QPS.
- Real scrape is 1/min; the bench used 5-min samples, so old's real recompute
  cost (and the win) is understated by ~5×.
- Raw results: `results_rewrite.jsonl`, `results_rewrite_200fill.jsonl`; full
  plans: `explain_{old,new}_{25,100,200}.txt` (in the harness dir).
