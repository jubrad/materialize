SET search_path = mz_catalog, mz_internal, pg_catalog;
SET cluster = mz_catalog_server;
EXPLAIN
SELECT
    bucket_start AS "bucketStart",
    replica_id AS "replicaId",
    memory_percent AS "maxMemoryPercent",
    max_memory_at AS "maxMemoryAt",
    disk_percent AS "maxDiskPercent",
    max_disk_at AS "maxDiskAt",
    max_cpu_percent AS "maxCpuPercent",
    max_cpu_at AS "maxCpuAt",
    heap_percent AS "maxHeapPercent",
    max_heap_at AS "maxHeapAt",
    memory_and_disk_percent AS "maxMemoryAndDiskPercent",
    max_memory_and_disk_memory_percent
        AS "maxMemoryAndDiskMemoryPercent",
    max_memory_and_disk_disk_percent
        AS "maxMemoryAndDiskDiskPercent",
    max_memory_and_disk_at AS "maxMemoryAndDiskAt",
    offline_events AS "offlineEvents",
    bucket_end AS "bucketEnd",
    name,
    cluster_id AS "clusterId",
    size
FROM mz_console_cluster_utilization_overview
WHERE cluster_id IN ( 's2' )
ORDER BY "bucketStart";