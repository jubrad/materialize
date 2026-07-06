SELECT
    owners."isOwner",
    c.id,
    c.name,
    c.disk,
    c.managed,
    c.size,
    (
            SELECT COALESCE(jsonb_agg(agg), '[]')
            FROM
                (
                            SELECT
                                cr.id,
                                cr.name,
                                cr.size,
                                cr.disk,
                                (
                                        SELECT
                                            COALESCE(
                                                jsonb_agg(
                                                    agg
                                                ),
                                                '[]'
                                            )
                                        FROM
                                            (
                                                        SELECT
                                                            crs_inner.replica_id,
                                                            crs_inner.process_id,
                                                            crs_inner.status,
                                                            crs_inner.reason,
                                                            crs_inner.updated_at
                                                        FROM
                                                            mz_cluster_replica_statuses
                                                                    AS crs_inner
                                                        WHERE
                                                            crs_inner.replica_id
                                                            = c.id
                                                    )
                                                    AS agg
                                    )
                                    AS statuses
                            FROM mz_cluster_replicas AS cr
                            WHERE cr.cluster_id = c.id
                            ORDER BY cr.id
                        )
                        AS agg
        )
        AS replicas,
    latest_cluster_status_update.latest_status_update
        AS "latestStatusUpdate"
FROM
    mz_clusters AS c
        JOIN
            (
                    SELECT
                        c.id AS cluster_id,
                        max(crsh.occurred_at)
                            AS latest_status_update
                    FROM
                        mz_clusters AS c
                            LEFT JOIN
                                mz_cluster_replica_history
                                    AS crh
                                ON crh.cluster_id = c.id
                            LEFT JOIN
                                mz_cluster_replica_status_history
                                    AS crsh
                                ON
                                    crh.replica_id
                                    = crsh.replica_id
                    GROUP BY c.id
                )
                AS latest_cluster_status_update
            ON
                latest_cluster_status_update.cluster_id
                = c.id
        JOIN
            (
                    SELECT
                        r.id,
                        r.name,
                        (
                                (
                                    SELECT
                                        mz_is_superuser()
                                            OR
                                        current_setting(
                                            'enable_rbac_checks'
                                        )
                                        = 'off'
                                )
                                    OR
                                has_role(
                                    current_user,
                                    r.oid,
                                    'USAGE'
                                )
                            )
                            AS "isOwner"
                    FROM mz_roles AS r
                )
                AS owners
            ON owners.id = c.owner_id
ORDER BY c.name;