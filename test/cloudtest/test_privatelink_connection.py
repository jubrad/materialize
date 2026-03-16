# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import random
import time
from textwrap import dedent

import pytest
import requests
from pg8000.dbapi import DatabaseError, ProgrammingError

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.app.materialize_application import MaterializeApplication
from materialize.cloudtest.k8s.toxiproxy import (
    PrivateLinkExternalNameService,
    ToxiproxyDeployment,
    ToxiproxyService,
)
from materialize.cloudtest.util.common import retry
from materialize.cloudtest.util.exists import exists, not_exists
from materialize.cloudtest.util.wait import wait
from materialize.ui import UIError


def test_create_privatelink_connection(mz: MaterializeApplication) -> None:
    # Create a PrivateLink SQL connection object,
    # which should create a K8S VpcEndpoint object.
    # We don't run the environment-controller,
    # so no AWS VPC Endpoint will be created.
    # so we don't need the named service to actually exist.
    create_connection_statement = dedent(
        """\
        CREATE CONNECTION privatelinkconn
        TO AWS PRIVATELINK (
            SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
            AVAILABILITY ZONES ('use1-az1', 'use1-az2')
        )
        """
    )

    # This should fail until max_aws_privatelink_connections is increased.
    with pytest.raises(
        ProgrammingError,
        match="creating AWS PrivateLink Connection would violate max_aws_privatelink_connections limit",
    ):
        mz.environmentd.sql(create_connection_statement)

    next_gid = mz.environmentd.sql_query(
        "SELECT MAX(SUBSTR(id, 2, LENGTH(id) - 1)::int) + 1 FROM mz_objects WHERE id LIKE 'u%'"
    )[0][0]

    not_exists(resource=f"vpcendpoint/connection-u{next_gid}")

    mz.environmentd.sql(
        "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
        port="internal",
        user="mz_system",
    )
    mz.environmentd.sql(create_connection_statement)

    aws_connection_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'privatelinkconn'"
    )[0][0]

    exists(resource=f"vpcendpoint/connection-{aws_connection_id}")

    # Less flaky if we sleep before checking the status
    time.sleep(5)

    assert (
        "unknown"
        == mz.environmentd.sql_query(
            f"SELECT status FROM mz_internal.mz_aws_privatelink_connection_status_history WHERE connection_id = '{aws_connection_id}'"
        )[0][0]
    )

    # TODO: validate the contents of the VPC endpoint resource, rather than just
    # its existence.

    mz.environmentd.sql(
        "ALTER SYSTEM SET enable_connection_validation_syntax = true",
        port="internal",
        user="mz_system",
    )
    mz.environmentd.sql(
        dedent(
            """\
            CREATE CONNECTION kafkaconn TO KAFKA (
                BROKERS (
                    'customer-hostname-1:9092' USING AWS PRIVATELINK privatelinkconn,
                    'customer-hostname-2:9092' USING AWS PRIVATELINK privatelinkconn (PORT 9093),
                    'customer-hostname-3:9092' USING AWS PRIVATELINK privatelinkconn (AVAILABILITY ZONE 'use1-az1', PORT 9093),
                    'customer-hostname-4:9094'
                ),
                SECURITY PROTOCOL PLAINTEXT
            ) WITH (VALIDATE = false);
            """
        )
    )
    mz.environmentd.sql_query("SELECT id FROM mz_connections WHERE name = 'kafkaconn'")[
        0
    ][0]

    principal = mz.environmentd.sql_query(
        "SELECT principal FROM mz_aws_privatelink_connections"
    )[0][0]

    assert principal == (
        f"arn:aws:iam::123456789000:role/mz_eb5cb59b-e2fe-41f3-87ca-d2176a495345_{aws_connection_id}"
    )

    # Validate default privatelink connections for kafka
    mz.environmentd.sql(
        dedent(
            """\
            CREATE CONNECTION kafkaconn_alt TO KAFKA (
                AWS PRIVATELINK privatelinkconn (PORT 9092),
                SECURITY PROTOCOL PLAINTEXT
            ) WITH (VALIDATE = false);
            """
        )
    )
    mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'kafkaconn_alt'"
    )[0][0]

    mz.environmentd.sql(
        dedent(
            """\
            CREATE CONNECTION sshconn TO SSH TUNNEL (
                HOST 'ssh-bastion-host',
                USER 'mz',
                PORT 22
            );
            """
        )
    )
    with pytest.raises(
        ProgrammingError, match="cannot specify both SSH TUNNEL and AWS PRIVATELINK"
    ):
        mz.environmentd.sql(
            dedent(
                """\
            CREATE CONNECTION pg TO POSTGRES (
                HOST 'postgres',
                DATABASE postgres,
                USER postgres,
                AWS PRIVATELINK privatelinkconn,
                SSH TUNNEL sshconn
            ) WITH (VALIDATE = false);
            """
            )
        )

    with pytest.raises(
        ProgrammingError, match='invalid AWS PrivateLink availability zone "us-east-1a"'
    ):
        mz.environmentd.sql(
            dedent(
                """\
                CREATE CONNECTION privatelinkconn2
                TO AWS PRIVATELINK (
                SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
                AVAILABILITY ZONES ('use1-az2', 'us-east-1a')
                );
                """
            )
        )

    with pytest.raises(
        ProgrammingError,
        match="connection cannot contain duplicate availability zones",
    ):
        mz.environmentd.sql(
            dedent(
                """\
                CREATE CONNECTION privatelinkconn2
                TO AWS PRIVATELINK (
                SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
                AVAILABILITY ZONES ('use1-az1', 'use1-az1', 'use1-az2')
                );
                """
            )
        )

    with pytest.raises(
        ProgrammingError,
        match='AWS PrivateLink availability zone "use1-az3" does not match any of the availability zones on the AWS PrivateLink connection',
    ):
        mz.environmentd.sql(
            dedent(
                """\
                CREATE CONNECTION kafkaconn2 TO KAFKA (
                    BROKERS (
                        'customer-hostname-3:9092' USING AWS PRIVATELINK privatelinkconn (AVAILABILITY ZONE 'use1-az3', PORT 9093)
                    ),
                    SECURITY PROTOCOL PLAINTEXT
                ) WITH (VALIDATE = false);
                """
            )
        )

    with pytest.raises(
        DatabaseError,
        match="invalid CONNECTION: can only set one of BROKER, BROKERS, or AWS PRIVATELINK",
    ):
        mz.environmentd.sql(
            dedent(
                """\
                CREATE CONNECTION kafkaconn2_alt TO KAFKA (
                    AWS PRIVATELINK privatelinkconn (PORT 9092),
                    BROKERS (
                        'customer-hostname-3:9092' USING AWS PRIVATELINK privatelinkconn (PORT 9093)
                    ),
                    SECURITY PROTOCOL PLAINTEXT
                ) WITH (VALIDATE = false);
                """
            )
        )
    with pytest.raises(
        ProgrammingError,
        match="invalid CONNECTION: POSTGRES does not support PORT for AWS PRIVATELINK",
    ):
        mz.environmentd.sql(
            dedent(
                """\
            CREATE CONNECTION pg TO POSTGRES (
                HOST 'postgres',
                DATABASE postgres,
                USER postgres,
                AWS PRIVATELINK privatelinkconn ( PORT 1234 ),
                PORT 1234
            ) WITH (VALIDATE = false);
            """
            )
        )

    mz.environmentd.sql("DROP CONNECTION kafkaconn CASCADE")
    mz.environmentd.sql("DROP CONNECTION privatelinkconn CASCADE")

    not_exists(resource=f"vpcendpoint/connection-{aws_connection_id}")


def test_background_drop_privatelink_connection(mz: MaterializeApplication) -> None:
    # Ensure that privatelink connections are
    # deleted in a background task
    mz.environmentd.sql(
        "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
        port="internal",
        user="mz_system",
    )
    create_connection_statement = dedent(
        """\
        CREATE CONNECTION privatelinkconn
        TO AWS PRIVATELINK (
            SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
            AVAILABILITY ZONES ('use1-az1', 'use1-az2')
        )
        """
    )
    mz.environmentd.sql(create_connection_statement)
    aws_connection_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'privatelinkconn'"
    )[0][0]
    mz.environmentd.sql("SET FAILPOINTS = 'drop_vpc_endpoint=pause'")
    mz.environmentd.sql("DROP CONNECTION privatelinkconn CASCADE")
    exists(resource=f"vpcendpoint/connection-{aws_connection_id}")
    mz.environmentd.sql("SET FAILPOINTS = 'drop_vpc_endpoint=off'")
    not_exists(resource=f"vpcendpoint/connection-{aws_connection_id}")


def test_retry_drop_privatelink_connection(mz: MaterializeApplication) -> None:
    # Ensure that privatelink connections are
    # deleted in a background task
    mz.environmentd.sql(
        "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
        port="internal",
        user="mz_system",
    )
    create_connection_statement = dedent(
        """\
        CREATE CONNECTION privatelinkconn
        TO AWS PRIVATELINK (
            SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
            AVAILABILITY ZONES ('use1-az1', 'use1-az2')
        )
        """
    )
    mz.environmentd.sql(create_connection_statement)
    aws_connection_id = mz.environmentd.sql_query(
        "SELECT id FROM mz_connections WHERE name = 'privatelinkconn'"
    )[0][0]
    mz.environmentd.sql("SET FAILPOINTS = 'drop_vpc_endpoint=return(failed)'")
    mz.environmentd.sql("DROP CONNECTION privatelinkconn CASCADE")
    exists(resource=f"vpcendpoint/connection-{aws_connection_id}")
    mz.environmentd.sql("SET FAILPOINTS = 'drop_vpc_endpoint=off'")
    retry(
        f=lambda: not_exists(resource=f"vpcendpoint/connection-{aws_connection_id}"),
        max_attempts=10,
        exception_types=[UIError],
    )


def test_privatelink_e2e_connectivity(mz: MaterializeApplication) -> None:
    """
    End-to-end test validating multi-AZ PrivateLink connectivity via Toxiproxy.

    This test simulates AWS PrivateLink with multiple availability zones by:
    1. Using separate Toxiproxy instances per AZ to simulate AZ-specific endpoints
    2. Creating ExternalName services that mimic VpcEndpoint DNS resolution per AZ
    3. Verifying Materialize correctly routes Kafka traffic through the PrivateLink endpoint

    Architecture (per AZ):
        Materialize --> ExternalName Service (connection-{id}-{az}) --> Toxiproxy-{az} --> Redpanda

    Note: This test validates the AWS PRIVATELINKS syntax and default fallback routing.
    The pattern matching rules (e.g., '*.use1-az1.*') only trigger when Redpanda advertises
    broker addresses containing those patterns. Since the standard Redpanda advertises
    'redpanda.default:9092', traffic goes through the default fallback rule. To test
    pattern-based AZ routing specifically, a custom Redpanda with AZ-specific advertised
    addresses would be needed (see PrivateLinkTestRedpandaDeployment).
    """
    namespace = DEFAULT_K8S_NAMESPACE
    availability_zones = ["use1-az1", "use1-az2"]

    # Track resources for cleanup
    toxiproxy_deployments: list[ToxiproxyDeployment] = []
    toxiproxy_services: list[ToxiproxyService] = []
    privatelink_svcs: list[PrivateLinkExternalNameService] = []

    # Step 1: Deploy Toxiproxy instances (one per AZ)
    for az in availability_zones:
        name = f"toxiproxy-{az}"
        deployment = ToxiproxyDeployment(namespace, name=name)
        service = ToxiproxyService(namespace, name=name)
        deployment.create()
        service.create()
        toxiproxy_deployments.append(deployment)
        toxiproxy_services.append(service)

    try:
        # Wait for all toxiproxy instances to be ready
        for az in availability_zones:
            wait(
                condition="condition=Available", resource=f"deployment/toxiproxy-{az}"
            )

        # Step 2: Enable PrivateLink connections and create connection
        mz.environmentd.sql(
            "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
            port="internal",
            user="mz_system",
        )
        mz.environmentd.sql(
            "ALTER SYSTEM SET enable_connection_validation_syntax = true",
            port="internal",
            user="mz_system",
        )

        az_list = ", ".join(f"'{az}'" for az in availability_zones)
        mz.environmentd.sql(
            dedent(
                f"""\
                CREATE CONNECTION privatelink_e2e_conn
                TO AWS PRIVATELINK (
                    SERVICE NAME 'com.amazonaws.vpce.test.vpce-svc-e2e-test',
                    AVAILABILITY ZONES ({az_list})
                )
                """
            )
        )

        connection_id = mz.environmentd.sql_query(
            "SELECT id FROM mz_connections WHERE name = 'privatelink_e2e_conn'"
        )[0][0]

        # Step 3: Verify VpcEndpoint resource exists
        exists(resource=f"vpcendpoint/connection-{connection_id}")

        # Step 4: Create ExternalName services to simulate VpcEndpoint controller
        # Each AZ gets its own service pointing to its toxiproxy instance
        for i, az in enumerate(availability_zones):
            privatelink_svc = PrivateLinkExternalNameService(
                connection_id=connection_id,
                target_service=f"toxiproxy-{az}.{namespace}.svc.cluster.local",
                namespace=namespace,
                availability_zone=az,
            )
            privatelink_svc.create()
            privatelink_svcs.append(privatelink_svc)

        # Also create the default (non-AZ-specific) service for the fallback rule
        # Since Redpanda advertises 'redpanda.default:9092', the AZ patterns won't match
        # and traffic will go through this default endpoint
        default_privatelink_svc = PrivateLinkExternalNameService(
            connection_id=connection_id,
            target_service=f"toxiproxy-{availability_zones[0]}.{namespace}.svc.cluster.local",
            namespace=namespace,
            availability_zone=None,  # No AZ = default service
        )
        default_privatelink_svc.create()
        privatelink_svcs.append(default_privatelink_svc)

        # Step 5: Configure Toxiproxy instances to proxy to Redpanda (initially ENABLED)
        # We need to enable them first so we can create the source, then disable to test
        toxiproxy_admin_ports = []
        for i, az in enumerate(availability_zones):
            admin_port = toxiproxy_services[i].node_port("admin")
            toxiproxy_admin_ports.append(admin_port)
            requests.post(
                f"http://localhost:{admin_port}/proxies",
                json={
                    "name": "kafka",
                    "listen": "0.0.0.0:9092",
                    "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                    "enabled": True,
                },
            )

        # Step 6: Create Kafka connection using AWS PRIVATELINKS with pattern-based routing
        # Patterns match broker addresses and route through AZ-specific endpoints
        mz.environmentd.sql(
            dedent(
                f"""\
                CREATE CONNECTION kafka_via_privatelink_e2e TO KAFKA (
                    AWS PRIVATELINKS (
                        '*.{availability_zones[0]}.*' TO privatelink_e2e_conn (
                            AVAILABILITY ZONE = '{availability_zones[0]}'
                        ),
                        '*.{availability_zones[1]}.*' TO privatelink_e2e_conn (
                            AVAILABILITY ZONE = '{availability_zones[1]}'
                        ),
                        privatelink_e2e_conn (PORT 9092)
                    ),
                    SECURITY PROTOCOL PLAINTEXT
                ) WITH (VALIDATE = false)
                """
            )
        )

        # Create a topic for testing with an explicit seed for reproducibility
        topic_base = "privatelink-e2e-test"
        seed = random.randint(0, 2**31 - 1)
        full_topic_name = f"testdrive-{topic_base}-{seed}"

        mz.testdrive.run(
            input=f"$ kafka-create-topic topic={topic_base}\n",
            no_reset=True,
            seed=seed,
        )

        # Step 7: Create source (proxies are enabled so this should succeed)
        mz.environmentd.sql(
            dedent(
                f"""\
                CREATE SOURCE privatelink_e2e_source
                FROM KAFKA CONNECTION kafka_via_privatelink_e2e (
                    TOPIC '{full_topic_name}'
                )
                """
            )
        )

        mz.environmentd.sql(
            dedent(
                f"""\
                CREATE TABLE privatelink_e2e_tbl
                FROM SOURCE privatelink_e2e_source (
                    REFERENCE "{full_topic_name}"
                )
                FORMAT BYTES
                ENVELOPE NONE
                """
            )
        )

        # Step 8: Now disable all proxies to test error handling
        for admin_port in toxiproxy_admin_ports:
            requests.post(
                f"http://localhost:{admin_port}/proxies/kafka",
                json={
                    "name": "kafka",
                    "listen": "0.0.0.0:9092",
                    "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                    "enabled": False,
                },
            )

        # Wait a bit for the source to detect connection loss
        time.sleep(5)

        # Verify source shows a stalled/error status (proxies are down)
        status = mz.environmentd.sql_query(
            "SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'privatelink_e2e_source'"
        )[0][0]
        assert status in (
            "stalled",
            "starting",
        ), f"Expected source to be stalled or starting when proxies are down, got: {status}"

        # Step 9: Enable one AZ's Toxiproxy (simulating partial AZ availability)
        requests.post(
            f"http://localhost:{toxiproxy_admin_ports[0]}/proxies/kafka",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": True,
            },
        )

        # Step 10: Verify data flows through the source (via the enabled AZ)
        mz.testdrive.run(
            input=dedent(
                f"""\
                $ kafka-ingest topic={topic_base} format=bytes
                test_data_via_privatelink

                > SELECT COUNT(*) FROM privatelink_e2e_tbl
                1
                """
            ),
            no_reset=True,
            seed=seed,
        )

        # Verify source is now running
        def check_source_running() -> None:
            status = mz.environmentd.sql_query(
                "SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'privatelink_e2e_source'"
            )[0][0]
            assert status == "running", f"Source status is {status}, expected running"

        retry(
            f=check_source_running,
            max_attempts=30,
            exception_types=[AssertionError],
        )

        # Step 11: Enable second AZ as well (full availability)
        requests.post(
            f"http://localhost:{toxiproxy_admin_ports[1]}/proxies/kafka",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": True,
            },
        )

        # Verify continued operation with both AZs available
        mz.testdrive.run(
            input=dedent(
                f"""\
                $ kafka-ingest topic={topic_base} format=bytes
                more_data_both_azs

                > SELECT COUNT(*) FROM privatelink_e2e_tbl
                2
                """
            ),
            no_reset=True,
            seed=seed,
        )

    finally:
        # Cleanup
        mz.environmentd.sql("DROP TABLE IF EXISTS privatelink_e2e_tbl CASCADE")
        mz.environmentd.sql("DROP SOURCE IF EXISTS privatelink_e2e_source CASCADE")
        mz.environmentd.sql(
            "DROP CONNECTION IF EXISTS kafka_via_privatelink_e2e CASCADE"
        )
        mz.environmentd.sql("DROP CONNECTION IF EXISTS privatelink_e2e_conn CASCADE")
        for svc in privatelink_svcs:
            svc.delete()
        for svc in toxiproxy_services:
            svc.delete()
        for dep in toxiproxy_deployments:
            dep.delete()


def test_privatelink_pattern_matching(mz: MaterializeApplication) -> None:
    """
    Test that pattern-based AZ routing works with AWS PRIVATELINKS.

    This test reconfigures the existing Redpanda to advertise an AZ-specific
    broker address, then verifies that the AWS PRIVATELINKS connection with
    pattern-based routing successfully routes traffic through toxiproxy.

    Architecture:
        1. Create K8s service alias 'redpanda-use1-az1' -> Redpanda pod
        2. Patch Redpanda to advertise 'redpanda-use1-az1.default.svc.cluster.local:9092'
        3. Bootstrap connects via default ExternalName -> toxiproxy-default -> Redpanda
        4. Metadata returns broker addr 'redpanda-use1-az1.default...'
        5. Pattern '*use1-az1*' matches -> subsequent connections via connection-{id}-use1-az1
        6. ExternalName (connection-{id}-use1-az1) -> toxiproxy-use1-az1 -> Redpanda

    This validates that:
    - The AWS PRIVATELINKS syntax with patterns is parsed correctly
    - Redpanda with custom advertised address works through the proxy path
    - Data flows end-to-end through the PrivateLink simulation
    """
    namespace = DEFAULT_K8S_NAMESPACE
    az = "use1-az1"
    # Use a real K8s DNS name that will resolve, but contains the AZ pattern
    broker_alias = f"redpanda-{az}"
    advertised_addr = f"{broker_alias}.{namespace}.svc.cluster.local:9092"

    # Track resources for cleanup
    toxiproxy_az_deployment = None
    toxiproxy_az_service = None
    toxiproxy_default_deployment = None
    toxiproxy_default_service = None
    privatelink_svc_az = None
    privatelink_svc_default = None
    original_redpanda_args = None
    broker_alias_service_created = False

    try:
        # Step 1: Create a K8s service alias for Redpanda with an AZ-specific name
        # This allows Redpanda to advertise an address that:
        # - Contains the AZ pattern for Materialize to match
        # - Actually resolves in the cluster for testdrive to work
        import json
        import subprocess

        broker_alias_svc = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": broker_alias, "namespace": namespace},
            "spec": {
                "selector": {"app": "redpanda"},
                "ports": [{"port": 9092, "targetPort": 9092}],
            },
        }
        subprocess.run(
            ["kubectl", "--context=kind-mzcloud", "apply", "-f", "-"],
            input=json.dumps(broker_alias_svc),
            text=True,
            check=True,
        )
        broker_alias_service_created = True

        # Step 2: Get the original Redpanda deployment args (for restoration later)
        redpanda_deployment = mz.kubectl(
            "get", "deployment", "redpanda", "-o", "json"
        )
        redpanda_config = json.loads(redpanda_deployment)
        original_redpanda_args = redpanda_config["spec"]["template"]["spec"][
            "containers"
        ][0].get("command", [])

        # Step 3: Patch Redpanda to advertise AZ-specific address
        # We need to add --advertise-kafka-addr to the rpk command
        patch = {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "redpanda",
                                "command": [
                                    "/usr/bin/rpk",
                                    "redpanda",
                                    "start",
                                    "--overprovisioned",
                                    "--smp",
                                    "1",
                                    "--memory",
                                    "1G",
                                    "--reserve-memory",
                                    "0M",
                                    "--node-id",
                                    "0",
                                    "--check=false",
                                    "--set",
                                    "redpanda.enable_transactions=true",
                                    "--set",
                                    "redpanda.enable_idempotence=true",
                                    "--set",
                                    "redpanda.auto_create_topics_enabled=true",
                                    "--advertise-kafka-addr",
                                    advertised_addr,
                                ],
                            }
                        ]
                    }
                }
            }
        }
        mz.kubectl(
            "patch",
            "deployment",
            "redpanda",
            "--type=strategic",
            "-p",
            json.dumps(patch),
        )

        # Wait for Redpanda to restart with new config
        mz.kubectl("rollout", "status", "deployment/redpanda", "--timeout=120s")
        wait(condition="condition=Available", resource="deployment/redpanda")
        # Extra wait for pod to be fully ready
        time.sleep(5)

        # Step 5: Deploy two toxiproxy instances:
        # - toxiproxy-use1-az1: Routes to Redpanda (ENABLED) - for AZ pattern match
        # - toxiproxy-default: Disabled - for default fallback (should NOT be used)
        toxiproxy_az_deployment = ToxiproxyDeployment(
            namespace, name=f"toxiproxy-{az}"
        )
        toxiproxy_az_service = ToxiproxyService(namespace, name=f"toxiproxy-{az}")
        toxiproxy_az_deployment.create()
        toxiproxy_az_service.create()

        toxiproxy_default_deployment = ToxiproxyDeployment(
            namespace, name="toxiproxy-default"
        )
        toxiproxy_default_service = ToxiproxyService(
            namespace, name="toxiproxy-default"
        )
        toxiproxy_default_deployment.create()
        toxiproxy_default_service.create()

        wait(condition="condition=Available", resource=f"deployment/toxiproxy-{az}")
        wait(condition="condition=Available", resource="deployment/toxiproxy-default")

        # Wait for toxiproxy pods to be fully ready for API connections
        time.sleep(5)

        # Configure toxiproxy-az to proxy to Redpanda (ENABLED)
        az_admin_port = toxiproxy_az_service.node_port("admin")
        requests.post(
            f"http://localhost:{az_admin_port}/proxies",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": True,
            },
        )

        # Configure toxiproxy-default (ENABLED for bootstrap)
        # Bootstrap uses default endpoint, then pattern matching kicks in for broker connections
        default_admin_port = toxiproxy_default_service.node_port("admin")
        requests.post(
            f"http://localhost:{default_admin_port}/proxies",
            json={
                "name": "kafka",
                "listen": "0.0.0.0:9092",
                "upstream": f"redpanda.{namespace}.svc.cluster.local:9092",
                "enabled": True,
            },
        )

        # Step 4: Create PrivateLink connection
        mz.environmentd.sql(
            "ALTER SYSTEM SET max_aws_privatelink_connections = 5",
            port="internal",
            user="mz_system",
        )
        mz.environmentd.sql(
            "ALTER SYSTEM SET enable_connection_validation_syntax = true",
            port="internal",
            user="mz_system",
        )

        mz.environmentd.sql(
            dedent(
                f"""\
                CREATE CONNECTION privatelink_pattern_conn
                TO AWS PRIVATELINK (
                    SERVICE NAME 'com.amazonaws.vpce.test.vpce-svc-pattern-test',
                    AVAILABILITY ZONES ('{az}')
                )
                """
            )
        )

        connection_id = mz.environmentd.sql_query(
            "SELECT id FROM mz_connections WHERE name = 'privatelink_pattern_conn'"
        )[0][0]

        # Step 5: Create ExternalName services
        # AZ-specific endpoint -> working toxiproxy
        privatelink_svc_az = PrivateLinkExternalNameService(
            connection_id=connection_id,
            target_service=f"toxiproxy-{az}.{namespace}.svc.cluster.local",
            namespace=namespace,
            availability_zone=az,
        )
        privatelink_svc_az.create()

        # Default endpoint -> DISABLED toxiproxy (to prove pattern matching works)
        privatelink_svc_default = PrivateLinkExternalNameService(
            connection_id=connection_id,
            target_service=f"toxiproxy-default.{namespace}.svc.cluster.local",
            namespace=namespace,
            availability_zone=None,
        )
        privatelink_svc_default.create()

        # Step 6: Create Kafka connection with pattern-based routing
        # The pattern '*.use1-az1.*' should match 'broker.use1-az1.internal:9092'
        mz.environmentd.sql(
            dedent(
                f"""\
                CREATE CONNECTION kafka_pattern_test TO KAFKA (
                    AWS PRIVATELINKS (
                        '*.{az}.*' TO privatelink_pattern_conn (
                            AVAILABILITY ZONE = '{az}'
                        ),
                        privatelink_pattern_conn (PORT 9092)
                    ),
                    SECURITY PROTOCOL PLAINTEXT
                ) WITH (VALIDATE = false)
                """
            )
        )

        # Step 7: Create topic and source
        topic_base = "privatelink-pattern-test"
        seed = random.randint(0, 2**31 - 1)
        full_topic_name = f"testdrive-{topic_base}-{seed}"

        mz.testdrive.run(
            input=f"$ kafka-create-topic topic={topic_base}\n",
            no_reset=True,
            seed=seed,
        )

        mz.environmentd.sql(
            dedent(
                f"""\
                CREATE SOURCE privatelink_pattern_source
                FROM KAFKA CONNECTION kafka_pattern_test (
                    TOPIC '{full_topic_name}'
                )
                """
            )
        )

        mz.environmentd.sql(
            dedent(
                f"""\
                CREATE TABLE privatelink_pattern_tbl
                FROM SOURCE privatelink_pattern_source (
                    REFERENCE "{full_topic_name}"
                )
                FORMAT BYTES
                ENVELOPE NONE
                """
            )
        )

        # Step 8: Verify data flows through the AZ-specific path
        # If pattern matching failed and fell back to default, this would fail
        # because toxiproxy-default is DISABLED
        mz.testdrive.run(
            input=dedent(
                f"""\
                $ kafka-ingest topic={topic_base} format=bytes
                pattern_matching_works

                > SELECT COUNT(*) FROM privatelink_pattern_tbl
                1
                """
            ),
            no_reset=True,
            seed=seed,
        )

        # Verify source is running
        status = mz.environmentd.sql_query(
            "SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'privatelink_pattern_source'"
        )[0][0]
        assert (
            status == "running"
        ), f"Source should be running (pattern matching worked!), got: {status}"

    finally:
        # Cleanup SQL objects
        mz.environmentd.sql("DROP TABLE IF EXISTS privatelink_pattern_tbl CASCADE")
        mz.environmentd.sql(
            "DROP SOURCE IF EXISTS privatelink_pattern_source CASCADE"
        )
        mz.environmentd.sql(
            "DROP CONNECTION IF EXISTS kafka_pattern_test CASCADE"
        )
        mz.environmentd.sql(
            "DROP CONNECTION IF EXISTS privatelink_pattern_conn CASCADE"
        )

        # Cleanup ExternalName services
        if privatelink_svc_az is not None:
            privatelink_svc_az.delete()
        if privatelink_svc_default is not None:
            privatelink_svc_default.delete()

        # Cleanup toxiproxy
        if toxiproxy_az_service is not None:
            toxiproxy_az_service.delete()
        if toxiproxy_az_deployment is not None:
            toxiproxy_az_deployment.delete()
        if toxiproxy_default_service is not None:
            toxiproxy_default_service.delete()
        if toxiproxy_default_deployment is not None:
            toxiproxy_default_deployment.delete()

        # Cleanup broker alias service
        if broker_alias_service_created:
            try:
                mz.kubectl("delete", "service", broker_alias)
            except Exception:
                pass  # Best effort cleanup

        # Restore original Redpanda configuration
        if original_redpanda_args:
            import json

            restore_patch = {
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [
                                {"name": "redpanda", "command": original_redpanda_args}
                            ]
                        }
                    }
                }
            }
            try:
                mz.kubectl(
                    "patch",
                    "deployment",
                    "redpanda",
                    "--type=strategic",
                    "-p",
                    json.dumps(restore_patch),
                )
                mz.kubectl(
                    "rollout", "status", "deployment/redpanda", "--timeout=120s"
                )
            except Exception:
                pass  # Best effort restoration
