# largestClusterReplica / largestMaintainedObjects

> **CORRECTION:** the "byte-identical files / copy-paste bug" below was a `.sql`
> snapshot-export artifact. The real builders are different: `largestClusterReplica.ts`
> (replicas) and `largestMaintainedQueries.ts` (reads `mz_dataflow_arrangement_sizes`,
> scoped by running on the target replica via session vars). No console bug.
> The `largestClusterReplica` perf note (small, current-metrics) is still valid.

## MEASURED VERDICT (Phase 2 sweep) — not worth a rewrite

EXPLAIN flagged full scans of `mz_hydration_statuses` and
`mz_cluster_replica_metrics`, so a cluster-scoped pushdown *looked* like a win.
A faithful sweep (shadows indexed like prod: hydration on `(object_id,
replica_id)`, metrics on `(replica_id)`; 2 replicas/cluster, 100 objects/replica)
says otherwise. p50 ms, old vs new:

| clusters (hydration rows) | conc | old | new |
|---|---|---|---|
| 25 (5k)   | 1 | 24.0 | 32.6 |
| 100 (20k) | 1 | 31.6 | 36.3 |
| 200 (40k) | 1 | 39.9 | 38.8 |
| 100 (20k) | 32 | 604.5 | 429.8 |
| 200 (40k) | 32 | 880.9 | 643.7 |

- **Single-user is a wash-to-slight-regression** (the pushdown's subquery
  overhead ≈ the scan it removes). The cluster page loads this query *once*, so
  single-user is the case that matters — and it gets slightly worse.
- NEW only wins under **high concurrency + large fleet** (~1.4x at 200c/conc32),
  and even there it's sub-second.
- **OLD barely grows with fleet** (24 → 40 ms single-user, 25 → 200 clusters)
  because these are small *current-state* relations, not time series — the "full
  scan" is cheap. That's the opposite of lagHistory / replicaUtilization, which
  scan time-series history (millions of rows) where scoping is a huge win.

**No PR.** The rewrite was reverted. Lesson: EXPLAIN's "full scan" only matters if
the relation is big; confirm with a sweep before claiming a win. Raw:
`results_largest.jsonl`, `explain_largest_*.txt`.

## (original write-up, from the snapshot)

`largestClusterReplicaQuery.sql` and `largestMaintainedObjects.sql`. Per-cluster
(`WHERE cr.cluster_id = 's2'`), `ORDER BY isHydrated DESC, heapLimit DESC LIMIT 1`
— pick the single largest/most-loaded replica of a cluster.

## Correctness flag: the two files are byte-identical

```
diff largestClusterReplicaQuery.filled.sql largestMaintainedObjects.filled.sql  -> IDENTICAL
```

Both contain the *replica* query. `largestMaintainedObjects.sql` almost certainly
should be a different query (largest maintained dataflow objects, e.g. via
`mz_object_arrangement_sizes`/`mz_dataflow_arrangement_sizes`), not a duplicate of
the replica query. Flagging — this is likely a copy-paste/naming error in the
Console, not something to "optimize."

Also note the `LIMIT '1}'` template artifact in the source (Handlebars leak);
I normalized it to `LIMIT 1` to get a plan.

## Plan finding (Phase 1)

From `raw/largestClusterReplicaQuery.explain.txt`:

```
cte l1 = →Consolidating Monotonic GroupAggregate
           Aggregations: max          -- max(heap_limit), max(heap_bytes) GROUP BY replica_id
           → mz_internal.mz_cluster_replica_metrics  Key: (#0{replica_id})
...
→Delta Join ...
  →Filter: (#2{cluster_id} = "s2")    -- cluster filter applied AFTER l1
    →Read l0                          -- replicas ⋈ sizes
```

Same structural anti-pattern as the others: the `max … GROUP BY replica_id`
aggregation (`l1`) runs over **all replicas'** metrics, and the cluster filter is
applied afterward in the final join.

**But the magnitude is small.** `l1` reads `mz_cluster_replica_metrics` — the
*current* metrics (one row per replica per process), not the history time series.
So the aggregation is O(replicas), not O(replicas × samples). Pushing the cluster
filter down would scope it to one cluster's replicas, but the absolute saving is
tiny compared to the history-scanning queries (lagHistory, replicaUtilization).

The `mz_hydration_statuses` LEFT JOIN (`WHERE object_id NOT LIKE 's%'`) similarly
scans all non-system hydration statuses; again O(objects), modest.

## Recommendation

Low performance priority — the costs scale with replica/object *count*, not with
history depth, so they're small. If a rewrite is done for consistency, the fix is
the same as elsewhere: restrict the metrics/hydration subqueries to the cluster's
replicas before aggregating (e.g. `WHERE replica_id IN (SELECT id FROM
mz_cluster_replicas WHERE cluster_id = 's2')`). Not worth a sweep on its own.

The **identical-files bug is the real finding here** and should be raised with the
Console team.
