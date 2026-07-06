# consoleClusterUtilizationOverview — already optimal (the destination)

`consoleClusterUtilizationOverviewQuery.sql`. Reads the maintained, indexed
builtin view for the 14-day cluster-utilization overview:

```sql
SELECT ... FROM mz_console_cluster_utilization_overview
WHERE cluster_id IN ('s2') ORDER BY "bucketStart";
```

## Plan finding (Phase 1)

From `raw/consoleClusterUtilizationOverviewQuery.explain.txt`:

```
Explained Query (fast path):
  →Map/Filter/Project
    →Index Lookup on mz_internal.mz_console_cluster_utilization_overview
      (using mz_internal.mz_console_cluster_utilization_overview_ind)
      Lookup values: ("s2")
```

A single **fast-path index point lookup**. Nothing to improve.

## Why this matters for the rest

This is the destination state. It's the same data as the per-timeframe
`replicaUtilizationHistory` recompute, but precomputed once into a maintained view
and served as a `cluster_id` point lookup — no per-load recompute, no fleet-wide
top-1s. It confirms the broader direction:

- the **query rewrite** (push the cluster filter down, the open PR #37323) makes
  the *recompute* path scale with one cluster instead of the fleet, and
- the **indexed-view** path collapses it further to this point lookup.

`lagHistory` is the next query that would benefit from the same arc: the rewrite
first (cheap, console-only), and a maintained view later if warranted.
