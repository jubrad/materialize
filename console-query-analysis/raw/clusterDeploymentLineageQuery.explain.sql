SET search_path = mz_catalog, mz_internal, pg_catalog;
SET cluster = mz_catalog_server;
EXPLAIN
SELECT
    cluster_id AS "clusterId",
    current_deployment_cluster_id
        AS "currentDeploymentClusterId",
    cluster_name AS "clusterName"
FROM mz_cluster_deployment_lineage
WHERE current_deployment_cluster_id IN ( 's2' );