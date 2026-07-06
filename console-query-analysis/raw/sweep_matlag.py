#!/usr/bin/env python3
"""materializationLag rewrite sweep. OLD: ml (latest-lag-per-object) + hs
(hydration) subqueries computed fleet-wide, joined to the objectIds-filtered
objects. NEW: push object_id IN objectIds into both subqueries.

FAITHFUL: wallclock_lag indexed (object_id), hydration indexed (object_id,
replica_id) — matching mz_catalog_server. objectIds = one cluster's objects
(how IndexList / LargestMaintainedQueries call it). Single-worker bench cluster.
"""
import argparse, json, random, sys, threading, time
sys.path.insert(0, "/Users/justin/.claude/jobs/e1c9b6f5/tmp")
import psycopg
DSN = "host=localhost port=6875 user=materialize dbname=materialize"
CLUSTER_SIZE = "scale=1,workers=1,mem=32GiB"
OBJ_PER_CLUSTER = 20
REPLICAS = 2
LAG_SAMPLES = 10   # last ~10 min @1/min; ~5 land in the 5-min window


def gen_fleet(conn, clusters):
    x = conn.execute
    x("DROP CLUSTER IF EXISTS bench CASCADE"); x("DROP SCHEMA IF EXISTS bench CASCADE")
    x(f"CREATE CLUSTER bench SIZE = '{CLUSTER_SIZE}'")
    x("CREATE SCHEMA bench"); x("SET search_path = bench"); x("SET cluster = bench")
    x("CREATE TABLE objects (id text, type text, cluster_id text)")
    x(f"INSERT INTO objects SELECT 'o'||(c*{OBJ_PER_CLUSTER}+k), 'index', 'c'||c "
      f"FROM generate_series(0,{clusters-1}) c, generate_series(0,{OBJ_PER_CLUSTER-1}) k")
    x("CREATE TABLE hydration (object_id text, replica_id text, hydrated bool)")
    x(f"INSERT INTO hydration SELECT id, 'r'||rr, true FROM objects, generate_series(0,{REPLICAS-1}) rr")
    x("CREATE INDEX hydration_idx IN CLUSTER bench ON hydration (object_id, replica_id)")
    x("CREATE TABLE wallclock_lag (occurred_at timestamptz, lag interval, object_id text)")
    x(f"INSERT INTO wallclock_lag SELECT now() - (s*INTERVAL '1 minute'), mod(s,7)*INTERVAL '1 second', id "
      f"FROM objects, generate_series(0,{LAG_SAMPLES-1}) s")
    x("CREATE INDEX wallclock_lag_idx IN CLUSTER bench ON wallclock_lag (object_id)")
    n = conn.execute("SELECT count(*) FROM wallclock_lag").fetchone()[0]
    print(f"== {clusters} clusters x {OBJ_PER_CLUSTER} obj = {clusters*OBJ_PER_CLUSTER} objects, {n:,} lag rows ==", flush=True)


def build_sql(cluster_idx, pushdown):
    ids = [f"'o{cluster_idx*OBJ_PER_CLUSTER+k}'" for k in range(OBJ_PER_CLUSTER)]
    inlist = ",".join(ids)
    hs_scope = f"WHERE object_id IN ({inlist})" if pushdown else ""
    ml_scope = f"AND object_id IN ({inlist})" if pushdown else ""
    return f"""
WITH materializationLag AS (
  SELECT objects.id AS "targetObjectId", objects.type, hs.hydrated, ml.lag AS lag
  FROM (SELECT * FROM objects WHERE id IN ({inlist})) AS objects
  LEFT JOIN (SELECT * FROM hydration {hs_scope}) AS hs ON hs.object_id = objects.id
  LEFT JOIN (
    SELECT DISTINCT ON (object_id) object_id, lag FROM wallclock_lag
    WHERE occurred_at + INTERVAL '5 minutes' >= mz_now() {ml_scope}
    ORDER BY object_id, occurred_at DESC
  ) AS ml ON ml.object_id = objects.id
)
SELECT *, lag >= INTERVAL '10 seconds' AS "isOutdated" FROM materializationLag;"""


def worker(stop_at, n_clusters, pushdown, lats, errors, lock):
    try:
        conn = psycopg.connect(DSN, autocommit=True)
        conn.execute("SET cluster = bench"); conn.execute("SET search_path = bench")
        conn.execute("SET statement_timeout = '120s'")
    except Exception as e:
        with lock: errors.append(str(e)[:120]); return
    while time.perf_counter() < stop_at:
        c = random.randrange(n_clusters)
        t0 = time.perf_counter()
        try:
            conn.execute(build_sql(c, pushdown)).fetchall()
            with lock: lats.append((time.perf_counter()-t0)*1000)
        except Exception as e:
            with lock: errors.append(str(e)[:120])
            try:
                conn = psycopg.connect(DSN, autocommit=True)
                conn.execute("SET cluster=bench"); conn.execute("SET search_path=bench"); conn.execute("SET statement_timeout='120s'")
            except Exception: return


def measure(n_clusters, pushdown, conc, duration):
    lats, errors, lock = [], [], threading.Lock()
    stop = time.perf_counter()+duration
    ts=[threading.Thread(target=worker,args=(stop,n_clusters,pushdown,lats,errors,lock)) for _ in range(conc)]
    for t in ts: t.start()
    for t in ts: t.join()
    lats.sort(); n=len(lats)
    pct=lambda p: round(lats[min(n-1,int(n*p))],1) if n else None
    return {"qps": round(n/duration,2),"n":n,"p50":pct(0.5),"p95":pct(0.95),"errs":len(errors)}


def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--duration",type=int,default=10); a=ap.parse_args()
    SCALES=[25,100,200]; CONC=[1,4,16,32]
    conn=psycopg.connect(DSN,autocommit=True)
    out=open("/Users/justin/.claude/jobs/e1c9b6f5/tmp/results_matlag.jsonl","w")
    print(f"{'variant':<7}{'clusters':>9}{'conc':>6}{'qps':>9}{'p50ms':>10}{'p95ms':>11}{'errs':>6}",flush=True)
    for ncl in SCALES:
        gen_fleet(conn,ncl)
        for pd,tag in ((False,"old"),(True,"new")):
            raw="\n".join(r[0] for r in conn.execute("EXPLAIN "+build_sql(0,pd)).fetchall())
            open(f"/Users/justin/.claude/jobs/e1c9b6f5/tmp/explain_matlag_{tag}_{ncl}.txt","w").write(raw)
        for pd,label in ((False,"old"),(True,"new")):
            for c in CONC:
                r=measure(ncl,pd,c,a.duration); r.update({"variant":label,"clusters":ncl,"concurrency":c})
                out.write(json.dumps(r)+"\n"); out.flush()
                print(f"{label:<7}{ncl:>9}{c:>6}{r['qps']:>9}{str(r['p50']):>10}{str(r['p95']):>11}{r['errs']:>6}",flush=True)
    conn.execute("DROP CLUSTER IF EXISTS bench CASCADE"); conn.execute("DROP SCHEMA IF EXISTS bench CASCADE")
    out.close(); print("DONE",flush=True)

if __name__=="__main__": main()
