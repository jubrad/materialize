# Console query performance analysis

Phase-1 (plan) diagnosis of the six Console queries in `~/Downloads/consoleQueries/`,
done with the `mz-query-perf` skill: each query filled with literals and
`EXPLAIN`ed on `mz_catalog_server` (real relations, real indexes, real view
definitions). Raw plans in `raw/*.explain.txt`; filled queries in
`raw/*.filled.sql`.

## Summary

> **Correction (verified against the real Kysely builders, not the `.sql`
> snapshots).** Two findings I first drew from the `~/Downloads/*.sql` snapshots
> were snapshot-export artifacts, not real console code. See "Snapshot artifacts"
> below. The table is the corrected picture.

| query | builder param | finding | win | PR |
|---|---|---|---|---|
| **lagHistory** | `clusterId`/`objectIds` **optional** (+ `groupByCluster`, `getLatest`) | Top-1 over **all objects**, filter applied *after* via join. When a cluster/object filter is set, push it before the Top-1. | **measured 18× single-user / 45× @32-conc (200 clusters); NEW flat with fleet** | **[#37327](https://github.com/MaterializeInc/materialize/pull/37327)** (draft) |
| **clustersList** | not by cluster (all) | `latestStatusUpdate` does a **full scan + fleet-wide `max(occurred_at)`** over all status history, no temporal filter. (No correctness bug — that was a snapshot artifact.) | medium, needs a semantics decision | none yet |
| **largestClusterReplica** | `clusterId` **required** | Full scans of hydration/metrics, filter after — but **measured: no win** (small current-state relations; pushdown regresses single-user, ~1.4× only at high conc). | none (measured) | none (reverted) |
| **largestMaintainedQueries** | scoped by session (runs on the replica) | Different, healthy query (`mz_dataflow_arrangement_sizes`). **Not** a duplicate — the identical `.sql` was a snapshot artifact. | none | none |
| **clusterDeploymentLineage** | `clusterIds` optional | `mz_cluster_deployment_lineage_ind (*** full scan ***)`: index keyed on `cluster_id`, query filters `current_deployment_cluster_id`. Small relation; only a DB index would help. | tiny | none (DB-side) |
| **consoleClusterUtilizationOverview** | `clusterIds` optional | **Already optimal** — fast-path `Index Lookup … ("s2")`. | none (target) | none |

### Answer to "are these all parameterized by cluster?" — no, it's a mix

- `largestClusterReplica`: **required** `clusterId`.
- `lagHistory`, `clusterDeploymentLineage`, `consoleClusterUtilizationOverview`:
  **optional** cluster filter (also run environment-wide / other modes).
- `largestMaintainedQueries`: **not** by a `cluster_id` predicate — it runs *on*
  the target replica (session vars) and reads that replica's local dataflows.
- `clustersList`: **not parameterized** — lists the whole environment.

The pushdown win only applies in the *filtered* modes; the environment-wide paths
legitimately need the fleet-wide computation, so the lagHistory rewrite is
correctly conditional on `clusterId`/`objectIds`.

### Snapshot artifacts (corrected)

1. `largestMaintainedObjects.sql` == `largestClusterReplicaQuery.sql` in the
   `~/Downloads` export, but the real builders (`largestMaintainedQueries.ts` vs
   `largestClusterReplica.ts`) are **different queries**. No copy-paste bug.
2. `clustersList.sql` showed `crs_inner.replica_id = c.id` (cluster id); the real
   `clusterList.ts` correctly uses `.whereRef("crs_inner.replica_id", "=", "cr.id")`.
   No correctness bug.

Lesson reinforced: diagnose plans on real relations *and* verify edits against the
real builders, not exported snapshots.

## Headline

- **lagHistory is the clear win** and mirrors the replicaUtilization rewrite
  exactly: a `DISTINCT ON (bucket, object) ORDER BY lag DESC` top-1 runs over
  *every* object's lag in the window, and `WHERE clusters.id = 's2'` is applied
  to `mz_objects` in a separate branch and joined *after* the top-1. Restricting
  `object_id` to the cluster's objects before the top-1 scopes the heavy operator
  to one cluster. **Phase-2 sweep (faithful — lag shadow indexed on `object_id`
  like prod) confirms it: NEW is flat with fleet size (≈26 ms single-user at 25 /
  100 / 200 clusters) while OLD scales linearly (70 → 250 → 482 ms); 18× single-
  user and 45× at 32 concurrent at 200 clusters.** EXPLAIN shows the lag read go
  from `*** full scan ***` to a `differential join` lookup. See `lagHistory.md`.

- **consoleClusterUtilizationOverview confirms the destination**: reading the
  maintained, indexed view is a single fast-path index lookup. This is what the
  replicaUtilization recompute should ultimately become for every timeframe.

- **The two "correctness bugs" I first reported were snapshot artifacts** (the
  identical `largest*` files and the `replica_id = c.id` correlation). Both are
  fine in the real builders. See "Snapshot artifacts" above.

See the per-query files for plan excerpts, rewrite hypotheses, and (for
lagHistory) sweep results. **Note:** `clustersList.md` and `largestClusterReplica.md`
were written from the snapshots and still describe those artifacts as bugs;
trust this README's corrected table over those two files.

## Recommendations

1. **lagHistory — DONE: draft PR
   [#37327](https://github.com/MaterializeInc/materialize/pull/37327).** Conditional
   pushdown (only when `clusterId`/`objectIds` set). typecheck + lint clean;
   compiled query EXPLAINed on `mz_catalog_server` shows the lag read go from full
   scan to differential-join lookup. Measured 18–45× on a faithful indexed sweep.
2. **clustersList latestStatusUpdate** — real perf issue (full status-history scan
   + fleet-wide `max(occurred_at)`, no temporal filter). Candidate: `max(updated_at)`
   over current `mz_cluster_replica_statuses`. **Needs a product decision** about
   dropped-replica semantics before a PR. Not done.
3. **largestClusterReplica** — **measured: not worth it (no PR).** EXPLAIN flagged
   full scans, but a faithful sweep shows single-user is a wash/slight regression
   (small current-state relations, cheap scan; pushdown overhead cancels it);
   only ~1.4x at high concurrency + large fleet. The page loads it once, so
   single-user is what matters. See `largestClusterReplica.md`.
4. **largestMaintainedQueries / clusterDeploymentLineage /
   consoleClusterUtilizationOverview** — no console-side change warranted (healthy
   query / DB-index-only / already optimal, respectively).

## Method note

Plan diagnosis uses the real relations on `mz_catalog_server` — strictly more
faithful than a synthetic shadow, and free. The latency sweep (Phase 2, only for
candidates) uses shadow user tables that **replicate the production indexes**
(the lesson from the replicaUtilization work: an unindexed shadow measures a
regime that doesn't exist in prod). Skill: `.agents/skills/mz-query-perf/`.
