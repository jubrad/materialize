#!/usr/bin/env python3
"""largestClusterReplica rewrite sweep. OLD: hydration + heap-metrics subqueries
computed fleet-wide, cluster filter applied after via LEFT JOIN. NEW: restrict
both subqueries to the cluster's replicas first.

FAITHFUL: hydration shadow indexed (object_id, replica_id) and metrics shadow
indexed (replica_id), matching mz_catalog_server. Both ad-hoc peeks on a
single-worker bench cluster; only the SQL differs.
"""
import argparse, json, random, sys, threading, time
sys.path.insert(0, "/Users/justin/.claude/jobs/e1c9b6f5/tmp")
import psycopg

DSN = "host=localhost port=6875 user=materialize dbname=materialize"
CLUSTER_SIZE = "scale=1,workers=1,mem=32GiB"
REPLICAS_PER_CLUSTER = 2
OBJECTS_PER_REPLICA = 100   # hydration rows per replica


def gen_fleet(conn, clusters):
    x = conn.execute
    x("DROP CLUSTER IF EXISTS bench CASCADE"); x("DROP SCHEMA IF EXISTS bench CASCADE")
    x(f"CREATE CLUSTER bench SIZE = '{CLUSTER_SIZE}'")
    x("CREATE SCHEMA bench"); x("SET search_path = bench"); x("SET cluster = bench")
    x("CREATE TABLE cluster_replicas (id text, cluster_id text, size text)")
    x(f"""INSERT INTO cluster_replicas
        SELECT 'r'||(c*{REPLICAS_PER_CLUSTER}+k), 'c'||c, 'sz'
        FROM generate_series(0,{clusters-1}) c, generate_series(0,{REPLICAS_PER_CLUSTER-1}) k""")
    x("CREATE TABLE cluster_replica_sizes (size text, processes uint8, cpu_nano_cores uint8, memory_bytes uint8, disk_bytes uint8)")
    x("INSERT INTO cluster_replica_sizes VALUES ('sz', 1, 1000000000, 8000000000, 16000000000)")
    x("CREATE TABLE hydration_statuses (object_id text, replica_id text, hydrated bool)")
    x(f"""INSERT INTO hydration_statuses
        SELECT 'u'||(rownum*{OBJECTS_PER_REPLICA}+o), id, (o % 10 != 0)
        FROM (SELECT id, row_number() OVER () AS rownum FROM cluster_replicas) cr,
             generate_series(0,{OBJECTS_PER_REPLICA-1}) o""")
    x("CREATE INDEX hydration_statuses_idx IN CLUSTER bench ON hydration_statuses (object_id, replica_id)")
    x("CREATE TABLE cluster_replica_metrics (replica_id text, process_id uint8, heap_limit uint8, heap_bytes uint8)")
    x("INSERT INTO cluster_replica_metrics SELECT id, 0, 8000000000, 4000000000 FROM cluster_replicas")
    x("CREATE INDEX cluster_replica_metrics_idx IN CLUSTER bench ON cluster_replica_metrics (replica_id)")
    nh = conn.execute("SELECT count(*) FROM hydration_statuses").fetchone()[0]
    print(f"== {clusters} clusters, {REPLICAS_PER_CLUSTER} rep/cluster, "
          f"{OBJECTS_PER_REPLICA} obj/replica = {nh:,} hydration rows ==", flush=True)


def build_sql(cluster, pushdown):
    hy_scope = (f"AND replica_id IN (SELECT id FROM cluster_replicas WHERE cluster_id = '{cluster}')"
                if pushdown else "")
    mx_scope = (f"WHERE crm.replica_id IN (SELECT id FROM cluster_replicas WHERE cluster_id = '{cluster}')"
                if pushdown else "")
    return f"""
SELECT cr.id AS name, cr.size, crhm.heap_limit::text AS "heapLimit",
       bool_and(hs.hydrated) AS "isHydrated"
FROM cluster_replicas AS cr
JOIN cluster_replica_sizes AS crs ON cr.size = crs.size
LEFT JOIN (
  SELECT replica_id, hydrated FROM hydration_statuses
  WHERE object_id NOT LIKE 's%' {hy_scope}
) AS hs ON cr.id = hs.replica_id
LEFT JOIN (
  SELECT crm.replica_id, MAX(crm.heap_limit) AS heap_limit, MAX(crm.heap_bytes) AS heap_bytes
  FROM cluster_replica_metrics AS crm {mx_scope}
  GROUP BY crm.replica_id
) AS crhm ON crhm.replica_id = cr.id
WHERE cr.cluster_id = '{cluster}'
GROUP BY cr.id, cr.size, crs.memory_bytes, crs.disk_bytes, crs.processes, crhm.heap_limit
ORDER BY "isHydrated" DESC NULLS LAST, "heapLimit" DESC NULLS LAST
LIMIT 1;"""


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
    ts = [threading.Thread(target=worker, args=(stop_at, n_clusters, pushdown, lats, errors, lock)) for _ in range(conc)]
    for t in ts: t.start()
    for t in ts: t.join()
    lats.sort(); n = len(lats)
    pct = lambda p: round(lats[min(n-1, int(n*p))], 1) if n else None
    return {"qps": round(n/duration, 2), "n": n, "p50": pct(0.5), "p95": pct(0.95), "errs": len(errors)}


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--duration", type=int, default=10); a = ap.parse_args()
    SCALES = [25, 100, 200]; CONC = [1, 4, 16, 32]
    conn = psycopg.connect(DSN, autocommit=True)
    out = open("/Users/justin/.claude/jobs/e1c9b6f5/tmp/results_largest.jsonl", "w")
    print(f"{'variant':<7}{'clusters':>9}{'conc':>6}{'qps':>9}{'p50ms':>10}{'p95ms':>11}{'errs':>6}", flush=True)
    for ncl in SCALES:
        gen_fleet(conn, ncl)
        for pd, tag in ((False, "old"), (True, "new")):
            raw = "\n".join(r[0] for r in conn.execute("EXPLAIN " + build_sql("c0", pd)).fetchall())
            open(f"/Users/justin/.claude/jobs/e1c9b6f5/tmp/explain_largest_{tag}_{ncl}.txt", "w").write(raw)
        for pd, label in ((False, "old"), (True, "new")):
            for c in CONC:
                r = measure(ncl, pd, c, a.duration)
                r.update({"variant": label, "clusters": ncl, "concurrency": c})
                out.write(json.dumps(r)+"\n"); out.flush()
                print(f"{label:<7}{ncl:>9}{c:>6}{r['qps']:>9}{str(r['p50']):>10}{str(r['p95']):>11}{r['errs']:>6}", flush=True)
    conn.execute("DROP CLUSTER IF EXISTS bench CASCADE"); conn.execute("DROP SCHEMA IF EXISTS bench CASCADE")
    out.close(); print("DONE", flush=True)


if __name__ == "__main__":
    main()
