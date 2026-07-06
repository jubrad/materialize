# Cluster-utilization overview — performance findings

Backing the Console's cluster-detail replica-utilization graphs. The Console used
to run a whole-fleet utilization rollup on **every** cluster page load against
`mz_catalog_server`. This summarizes what we measured and weighs the options.

**TL;DR — ship the three maintained indexed views + `cluster_id` point lookup.**
It turns a single-worker, throughput-capped fleet-wide recompute into a shared,
sub-second lookup: **~0.33 → ~125 qps, p50 102 s → 236 ms at 32 concurrent
clients**, holding up at 800 replicas. The two alternatives we tested lose: a
single 1-min base is a nicer storage shape but **collapses under concurrent
SUBSCRIBE**; a base + 1h chain is a memory wash today.

> Numbers from a local `environmentd`, dedicated bench cluster `scale=1,workers=1`
> (1 worker, like `mz_catalog_server`), synthetic fleet, 14 d of metrics at 1/min.
> Memory = settled maintained-arrangement bytes (`mz_object_arrangement_sizes`).
> **2 replicas per cluster** (so "200 replicas" = "100 clusters").

## The problem

Each page load issued the full rollup (five per-bucket arg-max top-1s, a 6-way
join, a name lateral, the offline `jsonb_agg`) and only *then* filtered to the one
cluster. The filter sat *after* the top-1s, so it couldn't push into a lookup —
every query recomputed the whole fleet, and on the single-worker catalog server N
concurrent loads serialize.

## The fix, and what it buys (before/after)

Three maintained **indexed** views — `_3h` (1-min/3 h), `_24h` (5-min/24 h),
`_overview` (1-h/14 d) — with **`cluster_id` as the leading key** so
`WHERE cluster_id = ?` is a point lookup. The Console reads the finest view
covering the timeframe.

`SELECT … WHERE cluster_id = ?` from N concurrent clients, 25 clusters:

| N clients | before (no index) | after (indexed) |
|---|---|---|
| 1  | 0.33 qps / 3.50 s | 3.5 qps / 354 ms |
| 4  | 0.35 qps / 12.9 s | 58.8 qps / 58 ms |
| 16 | 0.33 qps / 51.8 s | 125.1 qps / 124 ms |
| 32 | 0.32 qps / 102.2 s | 118.1 qps / 236 ms |

Before is pinned at ~0.33 qps regardless of concurrency (clients just queue; p50
grows linearly). After sustains ~125 qps, sub-300 ms p50 at N=32. **Scale:** at
400 clusters (800 replicas) the indexed path still does 4.3 → 131 qps (N=1→32);
the unindexed path at 100 clusters degrades to 0.08 qps and **drops server
connections** at N≥16.

## Weighing the options

| | **1. Three maintained views** (shipped) | **2. Single 1-min base** | **3. Base + 1h chain** |
|---|---|---|---|
| Maintained objects | 3 | 1 | 2 |
| Memory @200 replicas | 7230 MB | **4760 MB** | 7080 MB |
| Hydration | baseline | **~40% faster** | ~40% faster (base) |
| Single-user query p50 | **88–192 ms** | 320–330 ms (re-bin) | minute data + client re-bin |
| Concurrent SUBSCRIBE (N=32) | **230 ms, flat mem** | 18.9 s, +628 MB | 230 ms, flat mem |
| <24 h resolution | 5-min | 1-min | 1-min |
| **Verdict** | ✅ **ship** | ❌ SUBSCRIBE-hostile | ⚠️ wash today |

**Option 2 — single 1-min base.** Tempting: at 1/min scrape each 1-min bucket
holds one sample, so the five top-1s collapse to identity and vanish; one index
subsumes all three views, ~40% faster hydration, less memory at scale. But the
intended use is SUBSCRIBE, and **a SUBSCRIBE can't share an arrangement unless a
catalog object maintains it.** Re-binning coarse ranges at query time means each
subscriber builds its *own* re-bin dataflow:

| N SUBSCRIBEs | Opt 1 (maintained) | Opt 2 (re-bin) |
|---|---|---|
| 1  | 27 ms / flat | 611 ms / flat |
| 16 | 168 ms / flat | 9.8 s / +258 MB |
| 32 | 230 ms / flat | **18.9 s / +628 MB** |

Opt 2 serializes on the worker and blows up — 82× worse snapshots at N=32. Rules
it out for the SUBSCRIBE use case. <details><summary>queries</summary>

See **Q4** (base) and **Q5** (unmaterialized re-bin) in the appendix.</details>

**Option 3 — base + a maintained 1h view on top.** Both arrangements are
maintained (SUBSCRIBE-safe), 1-min resolution for <24 h, two objects not three.
Measured, it's a **memory wash**: 3460 MB @100 replicas (+0.9%) / 7080 MB @200
(−2%) vs the shipped 7230 MB, and apples-to-apples *both* grow ~2.05×/doubling —
no divergence in the measured range. The genuine scaling signal lives in the
**base shape**, not the chain total: the 14d top-1 rollup grows super-linearly
(~2.45×/doubling) while the 1-min base grows ~linearly, so the *base alone* is
29% cheaper than the lean 14d view at 100 clusters — but adding the 1h object to
make it usable eats that back. A material chain win only appears once the base's
near-linear growth dominates, **extrapolated past ~200 clusters (~400 replicas);
not measured here** (data stops at 100 clusters). The 1h view does reuse the base
(1170 MB increment vs ~2690 MB standalone). Worth revisiting if fleets approach
that scale or if 1-min <24 h resolution is wanted. <details><summary>query</summary>

See **Q7** in the appendix.</details>

## Key supporting findings

- **Pushdown is everything.** With `cluster_id` *after* the top-1s, a coarse
  re-bin full-scans the fleet: **7.1 s / 14.3 s** at 50 / 100 replicas. Making
  `cluster_id` the **leading `DISTINCT ON` key** turns it into a lookup —
  **330 ms, flat across fleet size**. Same trap the original Console query fell
  into.
- **The `DISTINCT ON GROUP SIZE` hint is mandatory.** Without it the 14d view
  hydrates in **545.8 s**; with it, **~67 s** at 100 replicas (~8×).
- **Bin width is the overview's main cost knob.** At 50 replicas the 14d view is
  **930 MB at 8-h bins vs 1700 MB at 1-h bins** — the finer overview resolution
  roughly doubles its memory. (We moved 14 d from 8 h → 1 h deliberately.)
- **The offline `jsonb_agg` is a non-issue in the indexed design.** Measured with
  the offline part on vs off, coexisting in one cluster state, settled:
  **+0.3% memory at realistic volume, +3.3% at a 20× stress load**; the jsonb
  arrangement in isolation is ~0 MB (90 MB at stress). `EXPLAIN` confirms the
  variants differ (13 vs 0 jsonb refs). It aggregates the tiny status table and
  is maintained once, so it adds nothing per query — no need to split it out.
  <details><summary>why the earlier A/B looked wrong</summary>

  An initial A/B measured the two variants in *separate* cluster states (build →
  measure → drop, twice) and read the first non-zero arrangement size, producing
  an impossible "without offline uses *more* memory." Measuring them coexisting
  and settled gives the correct, necessarily ≥ 0 deltas above.</details>

## Conclusion

Ship the three maintained indexed views with the `cluster_id` point lookup. The
1-min base is a better single-user storage shape but is SUBSCRIBE-hostile; the
base + 1h chain is a memory wash today (scaling upside only past ~200 clusters);
the offline `jsonb_agg` is negligible. This exploration strengthens the shipped
design rather than replacing it.

<details>
<summary><b>Methodology</b></summary>

All runs on a local `environmentd`, dedicated bench cluster `scale=1,workers=1`
(one worker, matching `mz_catalog_server`), synthetic fleet shadowing the real
system relations (`replica_sizes`, `replica_history`, `replica_name_history`,
`replica_status_history`, `replica_metrics_history`) with 14 days of metrics at a
1/min scrape (20,160 samples/replica). Memory = sum of maintained-arrangement
bytes from `mz_internal.mz_object_arrangement_sizes`, read after the arrangement
settles. Throughput from N concurrent clients issuing point lookups for a fixed
duration. Raw per-run stdout for every number here is preserved alongside this
doc.
</details>

---

<details>
<summary><b>Appendix: queries</b></summary>

The harness shadows the real system relations with user tables of identical
shape. Two CTEs are shared by several designs and shown once.

**`rmh` — per-sample cross-process rollup:**

```sql
SELECT m.occurred_at, m.replica_id, r.size, r.cluster_id,
  (SUM(m.cpu_nano_cores::float8)/(NULLIF(s.cpu_nano_cores,0)*s.processes))   AS cpu_percent,
  (SUM(m.memory_bytes::float8)/(NULLIF(s.memory_bytes,0)*s.processes))       AS memory_percent,
  (SUM(m.disk_bytes::float8)/(NULLIF(s.disk_bytes,0)*s.processes))           AS disk_percent,
  SUM(m.memory_bytes::float8) AS memory_bytes, SUM(m.disk_bytes::float8) AS disk_bytes,
  s.disk_bytes*s.processes AS total_disk_bytes, s.memory_bytes*s.processes AS total_memory_bytes,
  MAX(m.heap_bytes::float8) AS heap_bytes, MAX(m.heap_limit) AS heap_limit,
  COALESCE(MAX(m.heap_bytes::float8/NULLIF(m.heap_limit,0)),
           SUM(m.memory_bytes::float8)/(NULLIF(s.memory_bytes,0)*s.processes)) AS heap_percent
FROM replica_history AS r
JOIN replica_sizes AS s ON r.size = s.size
JOIN replica_metrics_history AS m ON m.replica_id = r.replica_id
GROUP BY m.occurred_at, m.replica_id, r.size, r.cluster_id,
         s.cpu_nano_cores, s.memory_bytes, s.disk_bytes, s.processes
```

**`five_max(src, GS)` — five per-bucket arg-max top-1s + their join**, over a
relation `src` with the `rmh` columns plus a bucket column `b`. `cluster_id` is
carried through every branch, so `WHERE cluster_id = ?` pushes into all five reads
(a point lookup when `src` is indexed on `cluster_id`):

```sql
WITH binned AS (SELECT * FROM <src>),
max_memory AS (SELECT DISTINCT ON (cluster_id, b, replica_id) cluster_id, b, replica_id, memory_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = <GS>) ORDER BY cluster_id, b, replica_id, COALESCE(memory_bytes,0) DESC),
max_disk AS (SELECT DISTINCT ON (cluster_id, b, replica_id) cluster_id, b, replica_id, disk_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = <GS>) ORDER BY cluster_id, b, replica_id, COALESCE(disk_bytes,0) DESC),
max_cpu AS (SELECT DISTINCT ON (cluster_id, b, replica_id) cluster_id, b, replica_id, cpu_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = <GS>) ORDER BY cluster_id, b, replica_id, COALESCE(cpu_percent,0) DESC),
max_heap AS (SELECT DISTINCT ON (cluster_id, b, replica_id) cluster_id, b, replica_id, heap_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = <GS>) ORDER BY cluster_id, b, replica_id, COALESCE(heap_bytes,0) DESC),
max_mad AS (SELECT DISTINCT ON (cluster_id, b, replica_id) cluster_id, b, replica_id, memory_and_disk_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = <GS>) ORDER BY cluster_id, b, replica_id, COALESCE(memory_and_disk_percent,0) DESC)
SELECT max_memory.b, max_memory.replica_id, max_memory.cluster_id,
  max_memory.memory_percent, max_memory.occurred_at AS mem_at,
  max_disk.disk_percent, max_cpu.cpu_percent, max_heap.heap_percent, max_mad.memory_and_disk_percent
FROM max_memory
JOIN max_disk USING (b, replica_id, cluster_id) JOIN max_cpu  USING (b, replica_id, cluster_id)
JOIN max_heap USING (b, replica_id, cluster_id) JOIN max_mad USING (b, replica_id, cluster_id)
```

### Q1. Shipped rollup (maintained indexed view)

Byte-for-byte the builtin body (real definition in
`src/catalog/src/builtin/mz_internal.rs`). The three shipped views are this body
at `(bin, retention, GROUP SIZE)` = `('1 minute',3h,1)`, `('5 minutes',24h,5)`,
`('1 hour',14d,60)`. Shown at 1-minute / 14 days:

```sql
CREATE VIEW util AS
WITH replica_metrics_history AS (
  SELECT m.occurred_at, m.replica_id, r.size,
    (SUM(m.cpu_nano_cores::float8)/(NULLIF(s.cpu_nano_cores,0)*s.processes))   AS cpu_percent,
    (SUM(m.memory_bytes::float8)/(NULLIF(s.memory_bytes,0)*s.processes))       AS memory_percent,
    (SUM(m.disk_bytes::float8)/(NULLIF(s.disk_bytes,0)*s.processes))           AS disk_percent,
    SUM(m.disk_bytes::float8) AS disk_bytes, SUM(m.memory_bytes::float8) AS memory_bytes,
    s.disk_bytes*s.processes AS total_disk_bytes, s.memory_bytes*s.processes AS total_memory_bytes,
    MAX(m.heap_bytes::float8) AS heap_bytes, MAX(m.heap_limit) AS heap_limit,
    COALESCE(MAX(m.heap_bytes::float8/NULLIF(m.heap_limit,0)),
             SUM(m.memory_bytes::float8)/(NULLIF(s.memory_bytes,0)*s.processes)) AS heap_percent
  FROM replica_history AS r
  JOIN replica_sizes AS s ON r.size = s.size
  JOIN replica_metrics_history AS m ON m.replica_id = r.replica_id
  GROUP BY m.occurred_at, m.replica_id, r.size, s.cpu_nano_cores, s.memory_bytes, s.disk_bytes, s.processes
),
binned AS (
  SELECT *, date_bin('1 minute', occurred_at, '1970-01-01'::timestamp) AS bucket_start
  FROM replica_metrics_history
  WHERE mz_now() <= date_bin('1 minute', occurred_at, '1970-01-01'::timestamp) + INTERVAL '14 days'
),
max_memory AS (SELECT DISTINCT ON (bucket_start, replica_id) bucket_start, replica_id, memory_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = 60) ORDER BY bucket_start, replica_id, COALESCE(memory_bytes,0) DESC),
max_disk AS (SELECT DISTINCT ON (bucket_start, replica_id) bucket_start, replica_id, disk_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = 60) ORDER BY bucket_start, replica_id, COALESCE(disk_bytes,0) DESC),
max_cpu AS (SELECT DISTINCT ON (bucket_start, replica_id) bucket_start, replica_id, cpu_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = 60) ORDER BY bucket_start, replica_id, COALESCE(cpu_percent,0) DESC),
max_memory_and_disk AS (SELECT DISTINCT ON (bucket_start, replica_id) bucket_start, replica_id, memory_percent, disk_percent, memory_and_disk_percent, occurred_at
  FROM (SELECT *, CASE WHEN disk_bytes IS NULL AND memory_bytes IS NULL THEN NULL
        ELSE (COALESCE(memory_bytes,0)+COALESCE(disk_bytes,0))/NULLIF((total_memory_bytes+total_disk_bytes),0) END AS memory_and_disk_percent
        FROM binned) AS inner_mad
  OPTIONS (DISTINCT ON INPUT GROUP SIZE = 60) ORDER BY bucket_start, replica_id, COALESCE(memory_and_disk_percent,0) DESC),
max_heap AS (SELECT DISTINCT ON (bucket_start, replica_id) bucket_start, replica_id, heap_percent, occurred_at
  FROM binned OPTIONS (DISTINCT ON INPUT GROUP SIZE = 60) ORDER BY bucket_start, replica_id, COALESCE(heap_bytes,0) DESC),
offline AS (
  SELECT date_bin('1 minute', occurred_at, '1970-01-01'::timestamp) AS bucket_start, replica_id,
    jsonb_agg(jsonb_build_object('replicaId', rsh.replica_id, 'occurredAt', rsh.occurred_at,
      'status', rsh.status, 'reason', rsh.reason)) AS offline_events
  FROM replica_status_history AS rsh
  WHERE process_id = 0 AND status = 'offline'
    AND mz_now() <= date_bin('1 minute', occurred_at, '1970-01-01'::timestamp) + INTERVAL '14 days'
  GROUP BY bucket_start, replica_id
)
SELECT bucket_start, replica_id, max_memory.memory_percent, max_memory.occurred_at AS max_memory_at,
  max_disk.disk_percent, max_disk.occurred_at AS max_disk_at,
  max_memory_and_disk.memory_and_disk_percent, max_memory_and_disk.memory_percent AS mad_mem,
  max_memory_and_disk.disk_percent AS mad_disk, max_memory_and_disk.occurred_at AS mad_at,
  max_heap.heap_percent, max_heap.occurred_at AS max_heap_at, max_cpu.cpu_percent, max_cpu.occurred_at AS max_cpu_at,
  offline.offline_events, bucket_start + INTERVAL '1 minute' AS bucket_end,
  replica_name_history.new_name AS name, replica_history.cluster_id, replica_history.size
FROM max_memory
JOIN max_disk USING (bucket_start, replica_id)
JOIN max_cpu USING (bucket_start, replica_id)
JOIN max_memory_and_disk USING (bucket_start, replica_id)
JOIN max_heap USING (bucket_start, replica_id)
JOIN replica_history USING (replica_id)
CROSS JOIN LATERAL (
  SELECT new_name FROM replica_name_history
  WHERE replica_id = replica_name_history.id
    AND bucket_start + INTERVAL '1 minute' >= COALESCE(replica_name_history.occurred_at, '1970-01-01'::timestamp)
  ORDER BY replica_name_history.occurred_at DESC LIMIT 1
) AS replica_name_history
LEFT JOIN offline USING (bucket_start, replica_id);

CREATE INDEX util_idx IN CLUSTER bench ON util (cluster_id);
```

### Q2. Per-cluster point lookup (before vs after)

The per-pageload query, and the one swept for throughput. "before" = no index on
`util`; "after" = identical statement once `util_idx` exists:

```sql
SELECT * FROM util WHERE cluster_id = $1;
```

### Q3. Design A — 14d/1h view alone

```sql
CREATE VIEW view_a AS
  -- five_max(src, 60), src = (SELECT *, date_bin('1 hour', occurred_at, '1970-01-01') AS b
  --   FROM (SELECT *, <memory_and_disk_percent CASE> FROM ( <rmh> ) rmh
  --         WHERE mz_now() <= date_bin('1 hour', occurred_at, '1970-01-01') + INTERVAL '14 days') z);
CREATE INDEX view_a_idx IN CLUSTER bench ON view_a (cluster_id);
```

### Q4. Design B — 1-min base

```sql
CREATE VIEW base_1min AS
  SELECT date_bin('1 minute', occurred_at, '1970-01-01'::timestamp) AS bucket_start,
    replica_id, cluster_id, size, occurred_at,
    cpu_percent, memory_percent, disk_percent, heap_percent, memory_bytes, disk_bytes, heap_bytes,
    total_memory_bytes, total_disk_bytes,
    CASE WHEN disk_bytes IS NULL AND memory_bytes IS NULL THEN NULL
      ELSE (COALESCE(memory_bytes,0)+COALESCE(disk_bytes,0))/NULLIF((total_memory_bytes+total_disk_bytes),0) END AS memory_and_disk_percent
  FROM ( <rmh> ) rmh
  WHERE mz_now() <= date_bin('1 minute', occurred_at, '1970-01-01'::timestamp) + INTERVAL '14 days';
CREATE INDEX base_1min_idx IN CLUSTER bench ON base_1min (cluster_id);
```

### Q5. Design B — unmaterialized re-bin (collapses under SUBSCRIBE)

```sql
CREATE VIEW rebin_1h AS
  -- five_max(src, 60), src = (SELECT *, date_bin('1 hour', occurred_at, '1970-01-01') AS b FROM base_1min)
SELECT * FROM rebin_1h WHERE cluster_id = $1;   -- per-query top-1, NOT maintained
```

### Q6. SUBSCRIBE probe

```sql
-- design A: v = maintained five_max view (indexed); design B: v = unmaterialized rebin_1h
BEGIN;
DECLARE cur CURSOR FOR SUBSCRIBE (SELECT * FROM v WHERE cluster_id = 'c<i>');
FETCH ALL cur;   -- time to first full snapshot
```

### Q7. Base + 1h chain

```sql
-- base_1min + index (Q4), plus:
CREATE VIEW hour AS
  -- five_max(src, 60), src = (SELECT *, date_bin('1 hour', occurred_at, '1970-01-01') AS b FROM base_1min)
CREATE INDEX hour_idx IN CLUSTER bench ON hour (cluster_id);
```

### Q8. Offline jsonb on vs off

The "off" variant drops exactly the `offline` CTE, its `offline.offline_events`
projection, and the trailing `LEFT JOIN offline USING (bucket_start, replica_id)`
from Q1. `EXPLAIN`: with = 13 jsonb refs / 3 Reduce / 6 Join; without = 0 / 2 / 5.

</details>
