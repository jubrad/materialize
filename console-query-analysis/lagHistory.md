# lagHistory — push the cluster filter below the Top-1

`lagHistoryQuery.sql`. Per-cluster page query (`WHERE clusters.id = 's2'`). Charts
per-object wallclock lag over the last hour, max lag per 1-minute bucket.

## Plan finding (Phase 1, `EXPLAIN` on mz_catalog_server)

The structure mirrors the replicaUtilization anti-pattern. From `raw/lagHistoryQuery.explain.txt`:

```
→Consolidating Monotonic Top1
  Group By #2, #0                 -- (object_id, bucket_start)
  Order By #1 desc nulls_first    -- lag DESC  => max lag per (object, bucket)
  →Filter: (timestamp_tz_to_mz_timestamp((#2{occurred_at} + 01:00:00)) >= <mz_now>)  -- 1h temporal filter
    →Arranged mz_internal.mz_wallclock_global_lag_recent_history  Key: (#0{object_id})
...
→Arrange (#0{id})
  →Filter: (#6{cluster_id} = "s2")            -- cluster filter, SEPARATE branch
    →Arranged mz_catalog.mz_objects
```

The `Consolidating Monotonic Top1` (the `lag_history_binned_by_max_lag`
`DISTINCT ON`) runs over **every object's** lag in the 1-hour window. The cluster
filter `cluster_id = 's2'` is applied to `mz_objects` in a *separate* branch and
joined to the top-1 output afterward. So for a deployment with many objects, the
top-1 processes the whole fleet and the join throws away all but one cluster's
objects.

Good news already present: the 1-hour temporal filter *is* pushed into the lag
read, so memory is bounded to an hour. The miss is the cluster (object-set) scope.

## Rewrite hypothesis

`mz_wallclock_global_lag_recent_history` has no `cluster_id`; the object→cluster
mapping is `mz_objects`. So restrict `object_id` to the cluster's objects *before*
the bin + top-1:

```sql
lag_history_with_temporal_filter AS (
    SELECT occurred_at, lag, object_id
    FROM mz_wallclock_global_lag_recent_history
    WHERE occurred_at + INTERVAL '3600000 MILLISECONDS' >= mz_now()
      AND object_id IN (SELECT id FROM mz_objects WHERE cluster_id = 's2')   -- push down
)
```

`mz_wallclock_global_lag_recent_history` is indexed on `object_id`, so the
restricted set becomes a lookup, and the bin + top-1 only process that cluster's
objects. The trailing `WHERE clusters.id = 's2'` then becomes redundant (the
object set is already scoped) but is harmless to keep. Output is unchanged.

This is the same shape and the same fix as the replicaUtilization pushdown, so the
expectation is the same: per-cluster page-load cost stops scaling with total fleet
object count and scales with the one cluster's object count instead.

## Phase-2 sweep (faithful: lag shadow indexed on `object_id` like prod)

### Structural confirmation (EXPLAIN on the indexed shadow)

- **OLD**: `Consolidating Monotonic Top1` reads `wallclock_lag` directly;
  `wallclock_lag_idx (*** full scan ***)` — the top-1 runs over every object.
- **NEW**: the top-1 now sits above a `Differential Join … » wallclock_lag[object_id]`
  whose other input is `objects` filtered `cluster_id = "c0"`;
  `wallclock_lag_idx (differential join)` — a **lookup** by the cluster's
  object_ids, top-1 scoped to those. The full scan is gone.

So the rewrite converts the lag read from a full index scan to a key lookup, and
because the shadow is indexed exactly as `mz_catalog_server` is, this reflects
production behavior.

### Latency (p50 ms; ad-hoc peeks, lag shadow indexed on object_id, single-worker bench)

"Concurrency N" = N cluster pages loaded at once (closed-loop; p50 ≈ time each
waits). 10 objects/cluster, 120 lag samples/object over the 1h window.

**OLD — filter after Top-1 (full scan of lag):**

| clusters (objects) | 1 | 4 | 16 | 32 |
|---|---|---|---|---|
| 25 (250)   | 70.2 | 253.0 | 1024.1 | 2056.0 |
| 100 (1000) | 249.7 | 979.1 | 3884.9 | 7709.5 |
| 200 (2000) | 481.6 | 1889.4 | 7613.5 | 15244.1 |

**NEW — object_id pushdown (lookup):**

| clusters (objects) | 1 | 4 | 16 | 32 |
|---|---|---|---|---|
| 25 (250)   | 25.4 | 37.3 | 123.8 | 262.5 |
| 100 (1000) | 25.6 | 42.0 | 138.6 | 286.6 |
| 200 (2000) | 26.8 | 49.3 | 160.6 | 338.8 |

**Speedup (old/new p50):**

| clusters | 1 | 4 | 16 | 32 |
|---|---|---|---|---|
| 25  | 2.8× | 6.8× | 8.3× | 7.8× |
| 100 | 9.8× | 23.3× | 28.0× | 26.9× |
| 200 | 18.0× | 38.3× | 47.4× | 45.0× |

### Read of the result

- **NEW is flat across fleet size** — 25.4 / 25.6 / 26.8 ms single-user at 25 /
  100 / 200 clusters. The lookup touches only the one cluster's objects, so total
  fleet size doesn't matter. This is the key property.
- **OLD scales linearly with the fleet** — 70 → 250 → 482 ms single-user as the
  object count grows, because the Top-1 full-scans all objects' lag every time.
- **So the speedup widens with scale**: 2.8× at 25 clusters → 18× at 200, single
  user; up to **45× at 200 clusters / 32 concurrent**. OLD hits 15 s p50 there;
  NEW stays under 340 ms.
- This is cleaner than the replicaUtilization sweep precisely because the shadow
  is **indexed like production** (`wallclock_lag_idx` on `object_id`): we can see
  NEW become a true lookup (flat) rather than an unindexed full scan.

### Recommendation

Strong console-only win, same shape as the merged replicaUtilization rewrite.
Apply the `object_id IN (SELECT id FROM mz_objects WHERE cluster_id = ?)` pushdown
to the `lag_history_with_temporal_filter` CTE in the Console's lag-history query
builder. Output is unchanged (verified row-for-row). No DB change. Raw:
`results_lag.jsonl`, `explain_lag_{old,new}_{25,100,200}.txt`.

