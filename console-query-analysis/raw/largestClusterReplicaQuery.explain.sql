SET search_path = mz_catalog, mz_internal, pg_catalog;
SET cluster = mz_catalog_server;
EXPLAIN
SELECT
    cr.name,
    cr.size,
    crhm.heap_limit::text AS "heapLimit",
    bool_and(hs.hydrated) AS "isHydrated"
FROM
    mz_cluster_replicas AS cr
        JOIN
            mz_cluster_replica_sizes AS crs
            ON cr.size = crs.size
        LEFT JOIN
            (
                    SELECT replica_id, hydrated
                    FROM mz_hydration_statuses
                    WHERE object_id NOT LIKE 's%'
                )
                AS hs
            ON cr.id = hs.replica_id
        LEFT JOIN
            (
                    SELECT
                        crm.replica_id,
                        max(crm.heap_limit) AS heap_limit,
                        max(crm.heap_bytes) AS heap_bytes
                    FROM mz_cluster_replica_metrics AS crm
                    GROUP BY crm.replica_id
                )
                AS crhm
            ON crhm.replica_id = cr.id
WHERE cr.cluster_id = 's2'
GROUP BY
    cr.name,
    cr.size,
    crs.memory_bytes,
    crs.disk_bytes,
    crs.processes,
    crhm.heap_limit
ORDER BY
    "isHydrated" DESC NULLS LAST,
    "heapLimit" DESC NULLS LAST
LIMIT 1;