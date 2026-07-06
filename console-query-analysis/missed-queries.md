# Console queries NOT in the snapshot set — triage

Swept `console/src/api/materialize/**` (83 query-builder files) for builders that
read **time-series / history** relations (the expensive ones — small current-state
relations don't matter, per the `largestClusterReplica` lesson). Found these that
weren't in `~/Downloads/consoleQueries/`. Triaged for the anti-patterns from the
`mz-query-perf` skill (top-1 then late filter; unfiltered history scan).

## HIGH — `cluster/materializationLag.ts` (clear win, twin of lagHistory)

Parameterized by `objectIds`. The `ml` subquery takes the latest lag per object
(`distinctOn(object_id) ORDER BY object_id, occurred_at DESC`, 5-min temporal
filter) over `mz_wallclock_global_lag_recent_history` for **every object in the
environment**, then LEFT JOINs to the `objectIds`-filtered objects:

```ts
.leftJoin(
  qb.selectFrom("mz_wallclock_global_lag_recent_history")
    .select(["object_id", "lag"])
    .distinctOn(["object_id"])
    .orderBy("object_id").orderBy("occurred_at", "desc")
    .where(... occurred_at + INTERVAL '5 MINUTES' >= mz_now())   // no objectIds filter!
    .as("ml"),
  "ml.object_id", "objects.id")
```

Same relation and same anti-pattern as the lagHistory pushdown (PR #37327). Fixed
by pushing `object_id IN (objectIds)` into both the `ml` (lag) and `hs`
(hydration) subqueries.

**MEASURED → draft PR [#37346](https://github.com/MaterializeInc/materialize/pull/37346).**
EXPLAIN on `mz_catalog_server`: the lag read goes from `*** full scan ***` →
`(lookup)` (the hydration read stays a cheap full scan — small current-state
relation). Faithful sweep (lag shadow indexed on `object_id`), p50 ms old → new:

| clusters (objects) | 1 | 16 | 32 |
|---|---|---|---|
| 25 (500)   | 33 → 49 | 177 → 149 | 345 → 296 |
| 100 (2000) | 50 → 45 | 483 → 244 | 985 → 510 |
| 200 (4000) | 74 → 53 | 885 → 366 | 1767 → 797 |

A **moderate** win — between lagHistory (huge) and largestClusterReplica (none):
~1.4× single-user and ~2.2–2.4× under concurrency at 200 clusters, scaling with
environment size. Trade-off: a slight single-user regression at small fleets
(33 → 49 ms, both sub-100 ms; crossover ~100 clusters), so the gain is
concentrated where the query is actually slow. Documented in the PR.

## MEDIUM — `maintained-objects/criticalPath.ts`

Recursive critical-path traversal over `mz_wallclock_global_lag_recent_history`
(5 `DISTINCT ON`s, 2 temporal filters). The probe bucket filters
`WHERE object_id = <objectId>`, but some `per_object_lag` / `per_object_peak` CTEs
compute peak lag **per object across the whole environment**, consumed by the
recursion. Whether that's wasteful depends on whether the dependency closure can
be derived up front — the recursion discovers it, so a pushdown is non-trivial.
Worth a closer look, but not a simple filter move. Lower confidence than
materializationLag.

## MEDIUM — `query-history/queryHistoryList.ts`

447 lines over `mz_recent_activity_log` (one of the largest relations). Filters
(`cluster_id`, `session_id`, `finished_status`, `execution_id`) are all optional;
no `mz_now` temporal bound. It's a paginated log listing, so some scanning is
inherent, but worth an `EXPLAIN` to confirm the filters push down and the
`ORDER BY` + `LIMIT` is served efficiently rather than sorting the whole log.
Different category from the per-entity dashboard queries.

## LOW / already-scoped (no action)

- `source/sourceErrors.ts`, `sink/sinkErrors.ts`: filter `WHERE source_id/sink_id = ?`
  early; no top-1. Scoped to the object.
- `source/sourceStatistics.ts`, `sink/sinkStatistics.ts`: `distinctOn` (latest
  stats) but filtered `WHERE s.id = ?` first, so the top-1 is already scoped to
  the one source/sink.
- `maintained-objects/lagAggregate.ts`: small aggregate with a temporal filter.
- `useCreditConsumption.ts`: reads `mz_cluster_replica_history` (small).

## Bottom line

One genuine missed win: **`materializationLag`** (same fix and confidence as
lagHistory). `criticalPath` and `queryHistoryList` are worth an EXPLAIN but are
more involved / inherently scan-heavy. The source/sink error+statistics queries
are already correctly scoped to their entity.
