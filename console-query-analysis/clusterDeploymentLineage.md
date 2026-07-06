# clusterDeploymentLineage

`clusterDeploymentLineageQuery.sql`. Per-cluster lookup:

```sql
SELECT cluster_id, current_deployment_cluster_id, cluster_name
FROM mz_cluster_deployment_lineage
WHERE current_deployment_cluster_id IN ('s2');
```

## Plan finding (Phase 1)

From `raw/clusterDeploymentLineageQuery.explain.txt`:

```
Explained Query (fast path):
  →Map/Filter/Project
    Filter: (#1{current_deployment_cluster_id} = "s2")
      →Indexed mz_internal.mz_cluster_deployment_lineage (using ..._ind)
Used Indexes:
  - mz_internal.mz_cluster_deployment_lineage_ind (*** full scan ***)
```

Fast path, but a **full scan** of the index then a filter, because:

- the index `mz_cluster_deployment_lineage_ind` is keyed on **`cluster_id`**
  (`mz_internal.rs:7506`), but
- the query filters on **`current_deployment_cluster_id`**.

So the key doesn't match the predicate and the whole index arrangement is scanned.

## Assessment

Low priority. `mz_cluster_deployment_lineage` is a small relation (one row per
cluster in a lineage, bounded by cluster count, no time series), so a full scan is
cheap in absolute terms. A second index keyed on `current_deployment_cluster_id`
would turn it into a lookup, but adding a builtin index (with its fingerprint /
migration cost and steady-state memory) is unlikely to be worth it for a
cluster-count-sized relation. Note it, don't act unless cluster counts get large.

No rewrite at the query level helps — the predicate column simply isn't the
indexed one; only an index change (DB side) would.
