# Cluster-utilization: raw measurement logs

Verbatim stdout from every measurement run behind `cluster-utilization-findings.md`,
recovered from the background-task output files (the session was compacted; these
logs were not). Setup for all runs: local `environmentd`, dedicated bench cluster
`scale=1,workers=1` (1 worker, matching `mz_catalog_server`), synthetic fleet,
14 days of metrics at 1/min unless noted. 2 replicas per cluster.

Harness scripts live in `~/.claude/jobs/e1c9b6f5/tmp/` (also referenced inline):
run_bench.sh + bench_driver.py + bench_setup.sql (before/after throughput),
measure_base.py (A vs 1-min base), subscribe_concurrency.py, measure_chain.py,
measure_three.py, measure_cost.py, measure_offline_ab.py, verify_offline.py.

Order below = logical (problem → fix → throughput → A/B → concurrency → chain →
jsonb), not chronological.

---

## 1. EXPLAIN — original query, BEFORE (no index) vs AFTER (indexed)

`b6n8ohuc7` — plan shapes proving the unindexed path full-scans / recomputes the fleet.

```
=== sample query (adhoc) count ===

    
576
=== EXPLAIN BEFORE (no index) ===

    
                                                                                                                                                                                                                                                                                    Physical Plan                                                                                                                                                                                                                                                                                    
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Explained Query:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   +
   →With                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            +
     cte l0 =                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
       →Differential Join %2:replica_metrics_history[#1{replica_id}] » %0:replica_history[#0{replica_id}] » %1:replica_sizes[#0{size}]                                                                                                                                                                                                                                                                                                                                                                                                                                              +
         →Arrange (#0{replica_id})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  +
           →Fused with Child Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     +
             Project: #0, #2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        +
               →Read materialize.bench.replica_history                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              +
         →Arrange (#0{size})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        +
           →Stream materialize.bench.replica_sizes                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  +
         →Arrange (#1{replica_id})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  +
           →Read materialize.bench.replica_metrics_history                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          +
     cte l1 =                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
       →Differential Join %0[#0..=#6] » %1[#0..=#6]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 +
         after %1:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  +
           Project: #0, #1, #7, #10..=#18                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           +
           Map: (#9 / uint8_to_double((case when (0 = uint8_to_numeric(#3{cpu_nano_cores})) then null else #3{cpu_nano_cores} end * #6{processes}))), (#10 / uint8_to_double((case when (0 = uint8_to_numeric(#4{memory_bytes})) then null else #4{memory_bytes} end * #6{processes}))), (#11 / uint8_to_double((case when (0 = uint8_to_numeric(#5{disk_bytes})) then null else #5{disk_bytes} end * #6{processes}))), (#5{disk_bytes} * #6{processes}), (#4{memory_bytes} * #6{processes}), coalesce(#8, #13), timestamptz_bin(00:01:00, #0{occurred_at}, 1970-01-01 00:00:00 UTC)+
         →Consolidating Monotonic GroupAggregate                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    +
           Aggregations: max, max                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   +
           Key:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     +
             Project: #6, #0, #1, #3..=#5, #2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
           →Fused with Child Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     +
             Project: #0..=#6, #10, #11                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             +
               →Read l0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             +
         →Accumulable GroupAggregate                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                +
           Simple aggregates: sum(#7{cpu_nano_cores}), sum(#8{memory_bytes}), sum(#9{disk_bytes})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   +
           Key:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     +
             Project: #6, #0, #1, #3..=#5, #2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
           →Fused with Child Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     +
             Project: #0..=#9                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
               →Read l0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             +
     cte l2 =                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
       →Differential Join %0[#1{replica_id}, #3{bucket_start}] » %1[#1{replica_id}, #3{bucket_start}] » %2[#1{replica_id}, #3{bucket_start}] » %3[#1{replica_id}, #4{bucket_start}] » %4[#1{replica_id}, #3{bucket_start}] » %5:replica_history[#0{replica_id}]                                                                                                                                                                                                                                                                                                                     +
         →Arrange (#1{replica_id}, #3{bucket_start})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                +
           →Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      +
             Project: #0..=#3                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
               →Monotonic Top1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      +
                 Group By #3, #1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    +
                 Order By #4 desc nulls_first                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
                 →Fused with Child Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               +
                   Project: #0, #1, #6, #11, #12                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    +
                   Map: coalesce(#3{memory_bytes}, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               +
                     →Read l1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
         →Arrange (#1{replica_id}, #3{bucket_start})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                +
           →Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      +
             Project: #0..=#3                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
               →Monotonic Top1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      +
                 Group By #3, #1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    +
                 Order By #4 desc nulls_first                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
                 →Fused with Child Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               +
                   Project: #0, #1, #7, #11, #12                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    +
                   Map: coalesce(#4{disk_bytes}, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 +
                     →Read l1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
         →Arrange (#1{replica_id}, #3{bucket_start})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                +
           →Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      +
             Project: #0..=#3                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
               →Monotonic Top1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      +
                 Group By #3, #1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    +
                 Order By #4 desc nulls_first                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
                 →Fused with Child Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               +
                   Project: #0, #1, #5, #11, #12                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    +
                   Map: coalesce(#5{cpu_percent}, 0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                +
                     →Read l1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
         →Arrange (#1{replica_id}, #4{bucket_start})                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                +
           →Map/Filter/Project                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      +
             Project: #0..=#5                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       +
```

## 2. Before/after throughput sweep (bench_driver, 7d @ 5min)

`bu5e6uk1c` — clusters=25 then clusters=100 (the 100-cluster BEFORE crashes the server at N>=16).

```
============================================================
SCALE: clusters=25 replicas/cluster=2 days=7 sample_min=5 proc=1
--- load fake data ---
--- EXPLAIN (BEFORE: no index) ---
--- BEFORE: recompute (no index) ---
>>> before  clusters=25  concurrency=1
{"label": "before clusters=25", "concurrency": 1, "count": 7, "qps": 0.33, "p50_ms": 3502.4, "p95_ms": 3508.1, "p99_ms": 3508.1, "max_ms": 3508.1, "errors": []}
>>> before  clusters=25  concurrency=4
{"label": "before clusters=25", "concurrency": 4, "count": 8, "qps": 0.35, "p50_ms": 12913.6, "p95_ms": 12932.7, "p99_ms": 12932.7, "max_ms": 12932.7, "errors": []}
>>> before  clusters=25  concurrency=16
{"label": "before clusters=25", "concurrency": 16, "count": 16, "qps": 0.33, "p50_ms": 51819.4, "p95_ms": 51850.6, "p99_ms": 51855.3, "max_ms": 51855.3, "errors": []}
>>> before  clusters=25  concurrency=32
{"label": "before clusters=25", "concurrency": 32, "count": 32, "qps": 0.32, "p50_ms": 102232.5, "p95_ms": 102382.1, "p99_ms": 102397.0, "max_ms": 102397.0, "errors": []}
--- build index (standalone DDL, on bench cluster) + hydrate ---
{"event":"hydrate","clusters":25,"seconds":"8.14"}
--- EXPLAIN (AFTER: indexed) ---
--- AFTER: indexed lookup ---
>>> after  clusters=25  concurrency=1
{"label": "after clusters=25", "concurrency": 1, "count": 70, "qps": 3.5, "p50_ms": 353.5, "p95_ms": 547.7, "p99_ms": 555.4, "max_ms": 558.8, "errors": []}
>>> after  clusters=25  concurrency=4
{"label": "after clusters=25", "concurrency": 4, "count": 1179, "qps": 58.84, "p50_ms": 58.1, "p95_ms": 75.2, "p99_ms": 498.1, "max_ms": 555.2, "errors": []}
>>> after  clusters=25  concurrency=16
{"label": "after clusters=25", "concurrency": 16, "count": 2514, "qps": 125.06, "p50_ms": 123.7, "p95_ms": 170.8, "p99_ms": 203.4, "max_ms": 269.1, "errors": []}
>>> after  clusters=25  concurrency=32
{"label": "after clusters=25", "concurrency": 32, "count": 2389, "qps": 118.08, "p50_ms": 236.2, "p95_ms": 645.5, "p99_ms": 962.1, "max_ms": 1349.0, "errors": []}
============================================================
SCALE: clusters=100 replicas/cluster=2 days=7 sample_min=5 proc=1
--- load fake data ---
--- EXPLAIN (BEFORE: no index) ---
--- BEFORE: recompute (no index) ---
>>> before  clusters=100  concurrency=1
{"label": "before clusters=100", "concurrency": 1, "count": 2, "qps": 0.09, "p50_ms": 12941.2, "p95_ms": 13085.7, "p99_ms": 13085.7, "max_ms": 13085.7, "errors": []}
>>> before  clusters=100  concurrency=4
{"label": "before clusters=100", "concurrency": 4, "count": 4, "qps": 0.08, "p50_ms": 51641.2, "p95_ms": 51647.3, "p99_ms": 51647.3, "max_ms": 51647.3, "errors": []}
>>> before  clusters=100  concurrency=16
{"label": "before clusters=100", "concurrency": 16, "count": 0, "qps": 0.0, "p50_ms": null, "p95_ms": null, "p99_ms": null, "max_ms": null, "errors": ["query: consuming input failed: server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally\n\tbefore or while processing the request.", "1 query errors", "query: consuming input failed: server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally\n\tbefore or while processing the request."]}
>>> before  clusters=100  concurrency=32
```

`bbszkxhuh` — clusters=400, after-index sweep stays healthy (~131 qps at N=32).

```
[400] setup...
[400] before c=1 (single recompute)...
{"label": "before clusters=400", "concurrency": 1, "count": 2, "qps": 0.02, "p50_ms": 52034.2, "p95_ms": 52199.9, "p99_ms": 52199.9, "max_ms": 52199.9, "errors": []}
[400] create index + hydrate...
{"event":"hydrate","clusters":400,"seconds":""}
[400] after sweep...
{"label": "after clusters=400", "concurrency": 1, "count": 52, "qps": 4.26, "p50_ms": 248.1, "p95_ms": 283.8, "p99_ms": 304.2, "max_ms": 315.2, "errors": []}
{"label": "after clusters=400", "concurrency": 4, "count": 696, "qps": 57.78, "p50_ms": 64.0, "p95_ms": 83.8, "p99_ms": 255.2, "max_ms": 606.8, "errors": []}
{"label": "after clusters=400", "concurrency": 16, "count": 1521, "qps": 125.48, "p50_ms": 123.9, "p95_ms": 164.0, "p99_ms": 190.2, "max_ms": 242.6, "errors": []}
{"label": "after clusters=400", "concurrency": 32, "count": 1607, "qps": 131.16, "p50_ms": 242.9, "p95_ms": 304.5, "p99_ms": 340.3, "max_ms": 383.1, "errors": []}
[400] DONE
```

`results.jsonl` (25 + 100 clusters) and `results_400.jsonl` (400) — machine-readable copies:

```json
{"label": "before clusters=25", "concurrency": 1, "count": 7, "qps": 0.33, "p50_ms": 3502.4, "p95_ms": 3508.1, "p99_ms": 3508.1, "max_ms": 3508.1, "errors": []}
{"label": "before clusters=25", "concurrency": 4, "count": 8, "qps": 0.35, "p50_ms": 12913.6, "p95_ms": 12932.7, "p99_ms": 12932.7, "max_ms": 12932.7, "errors": []}
{"label": "before clusters=25", "concurrency": 16, "count": 16, "qps": 0.33, "p50_ms": 51819.4, "p95_ms": 51850.6, "p99_ms": 51855.3, "max_ms": 51855.3, "errors": []}
{"label": "before clusters=25", "concurrency": 32, "count": 32, "qps": 0.32, "p50_ms": 102232.5, "p95_ms": 102382.1, "p99_ms": 102397.0, "max_ms": 102397.0, "errors": []}
{"event":"hydrate","clusters":25,"seconds":"8.14"}
{"label": "after clusters=25", "concurrency": 1, "count": 70, "qps": 3.5, "p50_ms": 353.5, "p95_ms": 547.7, "p99_ms": 555.4, "max_ms": 558.8, "errors": []}
{"label": "after clusters=25", "concurrency": 4, "count": 1179, "qps": 58.84, "p50_ms": 58.1, "p95_ms": 75.2, "p99_ms": 498.1, "max_ms": 555.2, "errors": []}
{"label": "after clusters=25", "concurrency": 16, "count": 2514, "qps": 125.06, "p50_ms": 123.7, "p95_ms": 170.8, "p99_ms": 203.4, "max_ms": 269.1, "errors": []}
{"label": "after clusters=25", "concurrency": 32, "count": 2389, "qps": 118.08, "p50_ms": 236.2, "p95_ms": 645.5, "p99_ms": 962.1, "max_ms": 1349.0, "errors": []}
{"label": "before clusters=100", "concurrency": 1, "count": 2, "qps": 0.09, "p50_ms": 12941.2, "p95_ms": 13085.7, "p99_ms": 13085.7, "max_ms": 13085.7, "errors": []}
{"label": "before clusters=100", "concurrency": 4, "count": 4, "qps": 0.08, "p50_ms": 51641.2, "p95_ms": 51647.3, "p99_ms": 51647.3, "max_ms": 51647.3, "errors": []}
{"label": "before clusters=100", "concurrency": 16, "count": 0, "qps": 0.0, "p50_ms": null, "p95_ms": null, "p99_ms": null, "max_ms": null, "errors": ["query: consuming input failed: server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally\n\tbefore or while processing the request.", "1 query errors", "query: consuming input failed: server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally\n\tbefore or while processing the request."]}

{"label": "before clusters=400", "concurrency": 1, "count": 2, "qps": 0.02, "p50_ms": 52034.2, "p95_ms": 52199.9, "p99_ms": 52199.9, "max_ms": 52199.9, "errors": []}
{"event":"hydrate","clusters":400,"seconds":""}
{"label": "after clusters=400", "concurrency": 1, "count": 52, "qps": 4.26, "p50_ms": 248.1, "p95_ms": 283.8, "p99_ms": 304.2, "max_ms": 315.2, "errors": []}
{"label": "after clusters=400", "concurrency": 4, "count": 696, "qps": 57.78, "p50_ms": 64.0, "p95_ms": 83.8, "p99_ms": 255.2, "max_ms": 606.8, "errors": []}
{"label": "after clusters=400", "concurrency": 16, "count": 1521, "qps": 125.48, "p50_ms": 123.9, "p95_ms": 164.0, "p99_ms": 190.2, "max_ms": 242.6, "errors": []}
{"label": "after clusters=400", "concurrency": 32, "count": 1607, "qps": 131.16, "p50_ms": 242.9, "p95_ms": 304.5, "p99_ms": 340.3, "max_ms": 383.1, "errors": []}
```

## 3. Design A (14d/1h view) vs B (1-min base) — single-user scaling

`b1c0vr9ok` — 50 & 100 replicas, BEFORE the re-bin pushdown fix: note the unmaterialized

1h re-bin at **7118 ms / 14326 ms** (full-scan trap), and the 100-replica server crash.

```
############### replicas=50 ###############
== 50 replicas, 14d @ 1min = 1,008,000 rows (load 5.4s) ==

A) current 14d/1h (maintained 5x top-1):  hydrate   33.3s  mem   1190.0MB  out    16,800  | query p50  159.7ms p95  588.7ms rows/q 672
B) 1-min/14d base (indexed, no top-1):     hydrate   20.2s  mem   1190.0MB  out 1,008,000  | raw lookup p50  431.7ms p95  680.7ms rows/q 40318
   unmaterialized 1h re-bin over base:                                                  | query    p50 7118.2ms p95 7191.4ms rows/q 674

############### replicas=100 ###############
== 100 replicas, 14d @ 1min = 2,016,000 rows (load 10.1s) ==

A) current 14d/1h (maintained 5x top-1):  hydrate   67.4s  mem   2760.0MB  out    33,600  | query p50   88.3ms p95  563.3ms rows/q 672
B) 1-min/14d base (indexed, no top-1):     hydrate   41.1s  mem   2460.0MB  out 2,015,800  | raw lookup p50  584.2ms p95  601.2ms rows/q 40316
   unmaterialized 1h re-bin over base:                                                  | query    p50 14326.0ms p95 243843.0ms rows/q 672
Traceback (most recent call last):
  File "/Users/justin/.claude/jobs/e1c9b6f5/tmp/measure_base.py", line 163, in <module>
    main()
    ~~~~^^
  File "/Users/justin/.claude/jobs/e1c9b6f5/tmp/measure_base.py", line 157, in main
    x("DROP VIEW rebin_1h"); x("DROP INDEX base_1min_idx"); x("DROP VIEW base_1min")
    ~^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/justin/system/lib/python3.13/site-packages/psycopg/connection.py", line 300, in execute
    raise ex.with_traceback(None)
psycopg.OperationalError: consuming input failed: server closed the connection unexpectedly
	This probably means the server terminated abnormally
	before or while processing the request.
(scale 100 failed)

############### replicas=200 ###############
```

`b6pai71h8` — 100 & 200 replicas, AFTER making cluster_id the leading DISTINCT ON key:

re-bin drops to **329.9 ms / 319.5 ms** and goes flat across fleet size.

```
############### replicas=100 ###############
== 100 replicas, 14d @ 1min = 2,016,000 rows (load 10.2s) ==

A) current 14d/1h (maintained 5x top-1):  hydrate   67.5s  mem   2690.0MB  out    33,600  | query p50  155.1ms p95  536.1ms rows/q 672
B) 1-min/14d base (indexed, no top-1):     hydrate   40.6s  mem   2250.0MB  out 2,015,900  | raw lookup p50  426.9ms p95  608.8ms rows/q 40316
   unmaterialized 1h re-bin over base:                                                  | query    p50  329.9ms p95  388.8ms rows/q 674
############### replicas=200 ###############
== 200 replicas, 14d @ 1min = 4,032,000 rows (load 20.3s) ==

A) current 14d/1h (maintained 5x top-1):  hydrate  139.5s  mem   6670.0MB  out    67,200  | query p50  192.0ms p95  458.8ms rows/q 672
B) 1-min/14d base (indexed, no top-1):     hydrate   84.5s  mem   4760.0MB  out 4,031,400  | raw lookup p50  515.7ms p95  662.6ms rows/q 40312
   unmaterialized 1h re-bin over base:                                                  | query    p50  319.5ms p95  465.3ms rows/q 674
DONE
```

## 4. SUBSCRIBE concurrency (100 replicas / 50 clusters)

`bl1ruiywu` — design A (maintained view) vs B (unmaterialized re-bin), N=1/4/16/32.

```
############### design=a ###############
== SUBSCRIBE concurrency: A maintained view, 100 replicas (50 clusters) ==
   N   snap_p50_ms   snap_p95_ms  mem_delta_MB
   1          26.7          26.7         -92.6
   4          89.7          90.3        -319.1
  16         167.5         172.2         -92.6
  32         230.5         267.5         -92.6

############### design=b ###############
== SUBSCRIBE concurrency: B 1-min base + unmaterialized re-bin, 100 replicas (50 clusters) ==
   N   snap_p50_ms   snap_p95_ms  mem_delta_MB
   1         611.1         611.1         -83.8
   4        2348.7        2349.5         -17.3
  16        9824.0        9828.4         257.5
  32       18921.6       18935.2         628.4

SUBSCRIBE TEST DONE
```

## 5. Chain: 1-min base + 1h-from-base (both maintained)

`brzzvy4n9` (100 replicas) and `bnxyztjh2` (200 replicas) — base mem, 1h increment, total.

```
== 100 replicas, 14d @ 1min = 2,016,000 rows (load 10.4s) ==

base_1min (1-min/14d):            hydrate   40.7s   mem   2290.0MB
hour (1h re-bin OF base):          hydrate   30.8s   mem   1170.0MB  (increment on top of base)

TOTAL base + hour:                                    3460.0MB
  vs standalone 1h/14d view (design A @ this scale) for reference
  vs base + a SEPARATE standalone 1h = base + ~(design A)

=== EXPLAIN SELECT * FROM hour  (does it read base_1min_idx or raw?) ===
  Used Indexes:

== 200 replicas, 14d @ 1min = 4,032,000 rows (load 20.0s) ==

base_1min (1-min/14d):            hydrate   82.8s   mem   4690.0MB
hour (1h re-bin OF base):          hydrate   63.7s   mem   2390.0MB  (increment on top of base)

TOTAL base + hour:                                    7080.0MB
  vs standalone 1h/14d view (design A @ this scale) for reference
  vs base + a SEPARATE standalone 1h = base + ~(design A)

=== EXPLAIN SELECT * FROM hour  (does it read base_1min_idx or raw?) ===
  Used Indexes:
```

## 6. Three shipped views coexisting (measure_three)

`bsak82q0f` — per-view + total memory/hydration at 100 and 200 replicas.

```
== 100 replicas, 14d @ 1min = 2,016,000 rows (load 10.1s) ==

view      bin       window      hydrate_s    mem_MB
ov_3h     1 minute  3 hours           1.8      50.0
ov_24h    5 minutes 24 hours          5.6      90.0
ov_14d    1 hour    14 days          67.7    3290.0

TOTAL (3 views coexisting)                     75.2s      3430.0MB

== 200 replicas, 14d @ 1min = 4,032,000 rows (load 20.1s) ==

view      bin       window      hydrate_s    mem_MB
ov_3h     1 minute  3 hours           3.8      10.0
ov_24h    5 minutes 24 hours         11.3     490.0
ov_14d    1 hour    14 days         138.4    6730.0

TOTAL (3 views coexisting)                    153.5s      7230.0MB

DONE
```

## 7. Supporting: bin width and GROUP SIZE hint

`bu038zq0h` — 50 replicas: the 14d view at 1h bin (1700 MB) vs 8h bin (930 MB) — cost of

the bin change. (3h/24h views shown too.)

```
== fleet: 50 replicas, 14d @ 1min = 20160 samples/replica, 1,008,000 metric rows ==
   data load: 5.1s

view          bin       window    hydrate_s   out_rows    arr_MB
util_3h       1 minute  3 hours         1.0      9,000      20.0
util_24h      5 minutes 24 hours        2.8     14,400     130.0
util_14d_1h   1 hour    14 days        33.3     16,800    1700.0
util_14d_8h   8 hours   14 days        34.4      2,100     930.0
```

`bgl2z0p23` — 100 replicas WITHOUT the DISTINCT ON GROUP SIZE hint: 14d view hydrates in

**545.8 s** vs ~67 s with the hint — shows why the hint matters.

```
== fleet: 100 replicas, 14d @ 1min = 20160 samples/replica, 2,016,000 metric rows ==
   data load: 10.7s

view      bin         window      hydrate_s    out_rows
util_3h   1 minute    3 hours           2.5      18,000
util_24h  5 minutes   24 hours         10.3      28,800
util_14d  1 hour      14 days         545.8      33,600

(arrangement memory measured separately via introspection)
```

## 8. Offline jsonb_agg — flawed A/B, then corrected verification

`b6icwbt51` — the ORIGINAL flawed A/B (separate cluster states + first-read sampling):

it reported WITHOUT-offline using MORE memory (impossible).

```
== 100 replicas, 14d @ 1min, 100 offline events/replica (load 10.1s) ==

WITH offline jsonb:    total mem   3560.0MB   total hydrate   74.7s   14d query p50  224.6ms
    ov_3h    hydrate    2.0s  mem     30.0MB
    ov_24h   hydrate    5.6s  mem    210.0MB
    ov_14d   hydrate   67.1s  mem   3320.0MB
WITHOUT offline:       total mem   3600.0MB   total hydrate   74.8s   14d query p50  251.8ms
    ov_3h    hydrate    1.8s  mem     40.0MB
    ov_24h   hydrate    5.6s  mem    240.0MB
    ov_14d   hydrate   67.3s  mem   3320.0MB

DELTA (offline costs): mem -40.0MB (-1%)   hydrate -0.0s   14d query -27.2ms
```

`bpzbann92` — corrected verify_offline smoke (6 replicas): EXPLAIN proof both variants differ.

```
== 6 replicas, 14d @ 1min, 50 offline/replica = 300 status rows (load 1.0s) ==

EXPLAIN proof (14d view):
  ov_full : jsonb refs= 13  Reduce ops=3  Join ops=6
  ov_noff : jsonb refs=  0  Reduce ops=2  Join ops=5
  => offline adds 13 jsonb refs, 1 Reduce, 1 Join (OK: noff has ZERO jsonb)

14d view memory (coexisting, settled). hydrate full 7.5s / noff 0.0s
    [ov_full] settled at 200.0MB after 2 reads: 200, 200
    [ov_noff] settled at 200.0MB after 2 reads: 200, 200
  ov_full    200.0MB    ov_noff    200.0MB    offline costs +0.0MB (+0.0%)

pure offline-only jsonb arrangement (hydrate 0.0s):
    [off_only] did NOT settle in 240s, last 0.0MB: 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
  off_only      0.0MB over 300 (bucket,replica) jsonb rows

DONE
```

`verify_out.txt` — corrected verify_offline at 100 replicas, realistic (100/replica) +

stress (2000/replica): coexisting, settled. Offline costs +0.3% / +3.3%.

```
########## REALISTIC: 100 offline/replica ##########
== 100 replicas, 14d @ 1min, 100 offline/replica = 10,000 status rows (load 10.0s) ==

EXPLAIN proof (14d view):
  ov_full : jsonb refs= 13  Reduce ops=3  Join ops=6
  ov_noff : jsonb refs=  0  Reduce ops=2  Join ops=5
  => offline adds 13 jsonb refs, 1 Reduce, 1 Join (OK: noff has ZERO jsonb)

14d view memory (coexisting, settled). hydrate full 135.2s / noff 0.0s
    [ov_full] settled at 3330.0MB after 3 reads: 3300, 3330, 3330
    [ov_noff] settled at 3320.0MB after 3 reads: 3320, 3320, 3320
  ov_full   3330.0MB    ov_noff   3320.0MB    offline costs +10.0MB (+0.3%)

pure offline-only jsonb arrangement (hydrate 0.2s):
    [off_only] settled at 0.0MB after 9 reads: 0, 0, 0, 0, 0, 0, 0, 0, 0
  off_only      0.0MB over 10,000 (bucket,replica) jsonb rows

DONE

########## STRESS: 2000 offline/replica ##########
== 100 replicas, 14d @ 1min, 2000 offline/replica = 200,000 status rows (load 10.1s) ==

EXPLAIN proof (14d view):
  ov_full : jsonb refs= 13  Reduce ops=3  Join ops=6
  ov_noff : jsonb refs=  0  Reduce ops=2  Join ops=5
  => offline adds 13 jsonb refs, 1 Reduce, 1 Join (OK: noff has ZERO jsonb)

14d view memory (coexisting, settled). hydrate full 137.2s / noff 0.0s
    [ov_full] settled at 3430.0MB after 4 reads: 3390, 3430, 3430, 3430
    [ov_noff] settled at 3320.0MB after 3 reads: 3320, 3320, 3320
  ov_full   3430.0MB    ov_noff   3320.0MB    offline costs +110.0MB (+3.3%)

pure offline-only jsonb arrangement (hydrate 1.2s):
    [off_only] settled at 90.0MB after 4 reads: 0, 90, 90, 90
  off_only     90.0MB over 33,500 (bucket,replica) jsonb rows

DONE
```

---

### Not included here (available on disk)

- Full `environmentd` run log: `b21d2i57m.output` (~1.2 MB).

- Test/infra runs (sqllogictest, catalog-server-explain regen, builds): `b3d1pqijw`,

  `bpe6wou27`, `b5jtiyuk9`, `bw898xmbs`, `bgvjx6137`, `bw9t6v2cf`, `bc8chnb2w`, `bedm1kt22`.

- Full session transcript JSONL (6.2 MB):

  `~/.claude/projects/-Users-justin-work-materialize2/e1c9b6f5-0899-40c2-b421-b32da886bc67.jsonl`
