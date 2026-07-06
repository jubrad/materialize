SET search_path = mz_catalog, mz_internal, pg_catalog;
SET cluster = mz_catalog_server;
EXPLAIN
WITH
    lag_history_with_temporal_filter AS
    (
        SELECT occurred_at, lag, object_id
        FROM mz_wallclock_global_lag_recent_history
        WHERE
            occurred_at + INTERVAL '3600000 MILLISECONDS'
            >= mz_now()
    ),
    lag_history_binned AS
    (
        SELECT
            date_bin(
                    '60000 MILLISECONDS',
                    occurred_at,
                    '1970-01-01'::timestamp
                )
                AS bucket_start,
            lag,
            object_id
        FROM lag_history_with_temporal_filter
    ),
    lag_history_binned_by_max_lag AS
    (
        SELECT DISTINCT ON (bucket_start, object_id)
            bucket_start, object_id, lag
        FROM lag_history_binned
        ORDER BY bucket_start DESC, object_id, lag DESC
    ),
    lag_history AS
    (
        SELECT
            lag_history.bucket_start AS "bucketStart",
            clusters.id AS "clusterId",
            lag_history.lag,
            lag_history.object_id AS "objectId",
            clusters.name AS "clusterName",
            object_names.database_name AS "databaseName",
            object_names.schema_name AS "schemaName",
            object_names.name AS "objectName"
        FROM
            lag_history_binned_by_max_lag AS lag_history
                JOIN
                    mz_objects AS objects
                    ON lag_history.object_id = objects.id
                JOIN
                    mz_clusters AS clusters
                    ON clusters.id = objects.cluster_id
                JOIN
                    mz_object_fully_qualified_names
                        AS object_names
                    ON
                        lag_history.object_id
                        = object_names.id
        WHERE clusters.id = 's2'
    )
SELECT * FROM lag_history
ORDER BY "bucketStart" ASC, lag DESC;