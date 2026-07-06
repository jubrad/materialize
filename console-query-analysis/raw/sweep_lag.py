#!/usr/bin/env python3
"""lagHistory rewrite sweep — OLD (cluster filter after the Top-1, via join) vs
NEW (object_id restricted to the cluster's objects before the bin+Top-1).

FAITHFUL: the lag shadow table is indexed on object_id, matching
mz_wallclock_global_lag_recent_history_ind on mz_catalog_server. Both variants
are ad-hoc peeks on a single-worker bench cluster; only the SQL differs.

Usage: sweep_lag.py [--duration S]
"""
import argparse
import json
import random
import sys
import threading
import time

sys.path.insert(0, "/Users/justin/.claude/jobs/e1c9b6f5/tmp")
import psycopg

DSN = "host=localhost port=6875 user=materialize dbname=materialize"
CLUSTER_SIZE = "scale=1,workers=1,mem=32GiB"
OBJECTS_PER_CLUSTER = 10
LAG_SAMPLES_PER_OBJECT = 120          # last hour, every 30s
BUCKET_MS = 60000


def gen_fleet(conn, clusters):
    x = conn.execute
    x("DROP CLUSTER IF EXISTS bench CASCADE"); x("DROP SCHEMA IF EXISTS bench CASCADE")
    x(f"CREATE CLUSTER bench SIZE = '{CLUSTER_SIZE}'")
    x("CREATE SCHEMA bench"); x("SET search_path = bench"); x("SET cluster = bench")
    # shadow mz_clusters / mz_objects / mz_object_fully_qualified_names / lag history
    x("CREATE TABLE clusters (id text, name text)")
    x(f"INSERT INTO clusters SELECT 'c'||g, 'cluster-'||g FROM generate_series(0,{clusters-1}) g")
    x("CREATE TABLE objects (id text, cluster_id text)")
    x(f"""INSERT INTO objects
        SELECT 'o'||(c*{OBJECTS_PER_CLUSTER}+k), 'c'||c
        FROM generate_series(0,{clusters-1}) c, generate_series(0,{OBJECTS_PER_CLUSTER-1}) k""")
    x("CREATE TABLE object_names (id text, database_name text, schema_name text, name text)")
    x("INSERT INTO object_names SELECT id, 'db', 'public', 'name-'||id FROM objects")
    x("CREATE TABLE wallclock_lag (occurred_at timestamptz, lag interval, object_id text)")
    t0 = time.perf_counter()
    x(f"""INSERT INTO wallclock_lag
        SELECT now() - (s * INTERVAL '30 seconds'),
               (s % 50) * INTERVAL '1 second',
               o.id
        FROM objects o, generate_series(0,{LAG_SAMPLES_PER_OBJECT-1}) s""")
    # FAITHFUL: prod indexes mz_wallclock_global_lag_recent_history on object_id
    x("CREATE INDEX wallclock_lag_idx IN CLUSTER bench ON wallclock_lag (object_id)")
    x("SELECT count(*) FROM wallclock_lag").fetchone()
    nrows = conn.execute("SELECT count(*) FROM wallclock_lag").fetchone()[0]
    print(f"== {clusters} clusters, {OBJECTS_PER_CLUSTER} obj/cluster, "
          f"{LAG_SAMPLES_PER_OBJECT} lag samples/obj = {nrows:,} lag rows "
          f"(load {time.perf_counter()-t0:.1f}s) ==", flush=True)


def build_sql(cluster, pushdown):
    pushdown_clause = (
        f"AND object_id IN (SELECT id FROM objects WHERE cluster_id = '{cluster}')"
        if pushdown else "")
    tail = "" if pushdown else f"WHERE clusters.id = '{cluster}'"
    return f"""
WITH
lag_history_with_temporal_filter AS (
  SELECT occurred_at, lag, object_id
  FROM wallclock_lag
  WHERE occurred_at + INTERVAL '3600000 MILLISECONDS' >= mz_now()
    {pushdown_clause}
),
lag_history_binned AS (
  SELECT date_bin('{BUCKET_MS} milliseconds', occurred_at, '1970-01-01'::timestamp) AS bucket_start,
         lag, object_id
  FROM lag_history_with_temporal_filter
),
lag_history_binned_by_max_lag AS (
  SELECT DISTINCT ON (bucket_start, object_id) bucket_start, object_id, lag
  FROM lag_history_binned
  ORDER BY bucket_start DESC, object_id, lag DESC
),
lag_history AS (
  SELECT lag_history.bucket_start AS "bucketStart", clusters.id AS "clusterId",
    lag_history.lag, lag_history.object_id AS "objectId", clusters.name AS "clusterName",
    object_names.database_name AS "databaseName", object_names.schema_name AS "schemaName",
    object_names.name AS "objectName"
  FROM lag_history_binned_by_max_lag AS lag_history
    JOIN objects ON lag_history.object_id = objects.id
    JOIN clusters ON clusters.id = objects.cluster_id
    JOIN object_names ON lag_history.object_id = object_names.id
  {tail}
)
SELECT * FROM lag_history ORDER BY "bucketStart" ASC, lag DESC;"""


def worker(stop_at, n_clusters, pushdown, lats, errors, lock):
    try:
        conn = psycopg.connect(DSN, autocommit=True)
        conn.execute("SET cluster = bench"); conn.execute("SET search_path = bench")
        conn.execute("SET statement_timeout = '120s'")
    except Exception as e:
        with lock: errors.append(str(e)[:120]); return
    while time.perf_counter() < stop_at:
        c = "c" + str(random.randrange(n_clusters))
        t0 = time.perf_counter()
        try:
            conn.execute(build_sql(c, pushdown)).fetchall()
            with lock: lats.append((time.perf_counter() - t0) * 1000)
        except Exception as e:
            with lock: errors.append(str(e)[:120])
            try:
                conn = psycopg.connect(DSN, autocommit=True)
                conn.execute("SET cluster=bench"); conn.execute("SET search_path=bench"); conn.execute("SET statement_timeout='120s'")
            except Exception: return


def measure(n_clusters, pushdown, conc, duration):
    lats, errors, lock = [], [], threading.Lock()
    stop_at = time.perf_counter() + duration
    threads = [threading.Thread(target=worker, args=(stop_at, n_clusters, pushdown, lats, errors, lock)) for _ in range(conc)]
    for t in threads: t.start()
    for t in threads: t.join()
    lats.sort(); n = len(lats)
    pct = lambda p: round(lats[min(n-1, int(n*p))], 1) if n else None
    return {"qps": round(n/duration, 2), "n": n, "p50": pct(0.5), "p95": pct(0.95), "errs": len(errors)}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--duration", type=int, default=10); args = ap.parse_args()
    SCALES = [25, 100, 200]; CONC = [1, 4, 16, 32]
    conn = psycopg.connect(DSN, autocommit=True)
    out = open("/Users/justin/.claude/jobs/e1c9b6f5/tmp/results_lag.jsonl", "w")
    print(f"{'variant':<7}{'clusters':>9}{'conc':>6}{'qps':>9}{'p50ms':>10}{'p95ms':>11}{'errs':>6}", flush=True)
    for ncl in SCALES:
        gen_fleet(conn, ncl)
        for pd, tag in ((False, "old"), (True, "new")):
            raw = "\n".join(r[0] for r in conn.execute("EXPLAIN " + build_sql("c0", pd)).fetchall())
            open(f"/Users/justin/.claude/jobs/e1c9b6f5/tmp/explain_lag_{tag}_{ncl}.txt", "w").write(raw)
        for pd, label in ((False, "old"), (True, "new")):
            for c in CONC:
                r = measure(ncl, pd, c, args.duration)
                r.update({"variant": label, "clusters": ncl, "concurrency": c})
                out.write(json.dumps(r)+"\n"); out.flush()
                print(f"{label:<7}{ncl:>9}{c:>6}{r['qps']:>9}{str(r['p50']):>10}{str(r['p95']):>11}{r['errs']:>6}", flush=True)
    conn.execute("DROP CLUSTER IF EXISTS bench CASCADE"); conn.execute("DROP SCHEMA IF EXISTS bench CASCADE")
    out.close(); print("DONE", flush=True)


if __name__ == "__main__":
    main()
