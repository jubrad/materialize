# clustersList — full-history scan for `latestStatusUpdate`

> **CORRECTION:** the "correctness bug" below (`replica_id = c.id`) was a `.sql`
> snapshot artifact. The real `clusterList.ts` uses
> `.whereRef("crs_inner.replica_id", "=", "cr.id")` — it's correct. The
> performance finding (full status-history scan for `latestStatusUpdate`) is real
> and confirmed in the builder. Read the perf section; ignore the bug section.

## (original write-up, from the snapshot)

`clustersList.sql`. The cluster-list page: every cluster with its replicas,
per-replica statuses, and a `latestStatusUpdate` timestamp. **Not** parameterized
(shows all clusters), so it inherently touches the whole fleet.

## Plan finding (Phase 1)

From `raw/clustersList.explain.txt`, `Used Indexes`:

```
- mz_catalog.mz_clusters_ind (*** full scan ***, differential join)
- mz_catalog.mz_roles_ind (*** full scan ***)
- mz_catalog.mz_cluster_replicas_ind (*** full scan ***)
- mz_internal.mz_cluster_replica_status_history_ind (*** full scan ***)
- mz_internal.mz_cluster_replica_history_ind (*** full scan ***)
```

The expensive one is the `latestStatusUpdate` subquery, which the plan renders as:

```
→Consolidating Monotonic GroupAggregate
  Aggregations: max                      -- max(crsh.occurred_at) per cluster
  → ... mz_cluster_replica_history ⋈ mz_cluster_replica_status_history
        (mz_cluster_replica_status_history_ind: *** full scan ***)
```

i.e. a **full scan of the entire `mz_cluster_replica_status_history`** plus a
fleet-wide `max(occurred_at) GROUP BY cluster`, **with no temporal filter**, on
every page load. Status history has 30-day retention and a row per status change
per replica per process, so this scan grows with fleet activity and is the
dominant cost of the page.

The other full scans (clusters, replicas, roles) are expected for a "list every
cluster" page — you genuinely need all of them, and they're small relative to
status history.

## Why a temporal filter does NOT fix it

`latestStatusUpdate` wants the *true* maximum `occurred_at`. A cluster that hasn't
changed status in weeks still has a real latest-update time, so a
`occurred_at >= mz_now() - INTERVAL '...'` filter would drop it and report NULL.
So the usual history-bounding trick is wrong here.

## Candidate rewrite (needs semantic confirmation)

`mz_cluster_replica_statuses` (the **current** statuses, one row per
replica/process, not the time series) carries an `updated_at` column — the time
the current status was set. Deriving `latestStatusUpdate` from
`max(updated_at)` over the current statuses replaces an O(all history events)
full scan with an O(replicas) scan:

```sql
SELECT crh.cluster_id, max(crs.updated_at) AS latest_status_update
FROM mz_cluster_replica_statuses crs
JOIN mz_cluster_replicas crh ON crh.id = crs.replica_id
GROUP BY crh.cluster_id
```

**Caveat (must verify before adopting):** the current query LEFT JOINs
`mz_cluster_replica_history`, so its max includes the last events of **dropped**
replicas; the current-statuses source only covers **live** replicas. If the page
must reflect a dropped replica's final status time, the two differ. Worth checking
what `latestStatusUpdate` is actually used for in the UI before changing.

No sweep run yet — this one needs the semantic question answered first; the plan
evidence (full scan + fleet-wide max over unbounded history) is clear regardless.

## Correctness bug (independent of performance)

The inner per-replica statuses subquery:

```sql
FROM mz_cluster_replica_statuses AS crs_inner
WHERE crs_inner.replica_id = c.id      -- c is the CLUSTER (FROM mz_clusters AS c)
```

`c.id` is the cluster id; this should be the replica id (`cr.id` from the
enclosing `mz_cluster_replicas AS cr`). As written, `replica_id = <cluster id>`
never matches, so every replica's `statuses` array comes back empty. Likely a real
bug — worth confirming against the rendered UI.
