// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for balancerd.

#![recursion_limit = "256"]

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use wtransport::{ClientConfig, Endpoint, RecvStream, SendStream};

use chrono::Utc;
use domain::resolv::StubResolver;
use futures::StreamExt;
use jsonwebtoken::{DecodingKey, EncodingKey};
use mz_balancerd::{
    BUILD_INFO, BalancerConfig, BalancerService, CancellationResolver, FronteggResolver, Resolver,
    SniResolver,
};
use mz_environmentd::test_util::{self, Ca, make_pg_tls};
use mz_frontegg_auth::{
    Authenticator as FronteggAuthentication, AuthenticatorConfig as FronteggConfig,
    DEFAULT_REFRESH_DROP_FACTOR, DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
};
use mz_frontegg_mock::{FronteggMockServer, models::ApiToken, models::UserConfig};
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::id_gen::{conn_id_org_uuid, org_id_conn_bits};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_ore::tracing::TracingHandle;
use mz_ore::{assert_contains, assert_err, assert_ok, task};
use mz_server_core::TlsCertConfig;
use openssl::ssl::{SslConnectorBuilder, SslVerifyMode};
use openssl::x509::X509;
use tokio::sync::oneshot;
use uuid::Uuid;

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 1))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_balancer() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    let tenant_id = Uuid::new_v4();
    let email = "user@_.com".to_string();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let initial_api_tokens = vec![ApiToken {
        client_id: client_id.clone(),
        secret: secret.clone(),
        description: None,
        created_at: Utc::now(),
    }];
    let roles = Vec::new();
    let users = BTreeMap::from([(
        email.clone(),
        UserConfig {
            id: Uuid::new_v4(),
            email,
            password,
            tenant_id,
            initial_api_tokens,
            roles,
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);

    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();

    const EXPIRES_IN_SECS: i64 = 50;
    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        SYSTEM_TIME.clone(),
        EXPIRES_IN_SECS,
        // Add a bit of delay so we can test connection de-duplication.
        Some(Duration::from_millis(100)),
        None,
    )
    .await
    .unwrap();

    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_user = "user@_.com";
    let frontegg_password = format!("mzp_{client_id}{secret}");

    let config = test_util::TestHarness::default()
        // Enable SSL on the main port. There should be a balancerd port with no SSL.
        .with_tls(server_cert.clone(), server_key.clone())
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry);
    let envid = config.environment_id.clone();
    let envd_server = config.start().await;

    let cancel_dir = tempfile::tempdir().unwrap();
    let cancel_name = conn_id_org_uuid(org_id_conn_bits(&envid.organization_id()));
    std::fs::write(
        cancel_dir.path().join(cancel_name),
        format!(
            "{}\n{}",
            envd_server.sql_local_addr(),
            // Ensure that multiline files and non-existent addresses both work.
            "non-existent-addr:1234",
        ),
    )
    .unwrap();

    let resolvers = vec![
        (
            Resolver::Static(envd_server.sql_local_addr().to_string()),
            CancellationResolver::Static(envd_server.sql_local_addr().to_string()),
        ),
        (
            Resolver::MultiTenant(
                FronteggResolver {
                    auth: frontegg_auth,
                    addr_template: envd_server.sql_local_addr().to_string(),
                },
                Some(SniResolver {
                    resolver: StubResolver::new(),
                    template: envd_server.sql_local_addr().ip().to_string(),
                    port: envd_server.sql_local_addr().port(),
                }),
            ),
            CancellationResolver::Directory(cancel_dir.path().to_owned()),
        ),
    ];
    let cert_config = Some(TlsCertConfig {
        cert: server_cert.clone(),
        key: server_key.clone(),
    });

    let body = r#"{"query": "select 12234"}"#;
    let ca_cert = reqwest::Certificate::from_pem(&ca.cert.to_pem().unwrap()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca_cert)
        // No pool so that connections are never re-used which can use old ssl certs.
        .pool_max_idle_per_host(0)
        .tls_info(true)
        .build()
        .unwrap();

    for (resolver, cancellation_resolver) in resolvers {
        let (mut reload_tx, reload_rx) = futures::channel::mpsc::channel(1);
        let ticker = Box::pin(reload_rx);
        let is_multi_tenant_resolver = matches!(resolver, Resolver::MultiTenant(_, _));
        let balancer_cfg = BalancerConfig::new(
            &BUILD_INFO,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            None, // webtransport_listen_addr: disabled in tests
            cancellation_resolver,
            resolver,
            envd_server.http_local_addr().to_string(),
            cert_config.clone(),
            true,
            MetricsRegistry::new(),
            ticker,
            None,
            None,
            Duration::ZERO,
            None,
            None,
            None,
            TracingHandle::disabled(),
            vec![],
        );
        let balancer_server = BalancerService::new(balancer_cfg).await.unwrap();
        let balancer_pgwire_listen = balancer_server.pgwire.0.local_addr();
        let balancer_https_listen = balancer_server.https.0.local_addr();
        let balancer_https_internal = balancer_server.internal_http.0.local_addr();
        task::spawn(|| "balancer", async {
            balancer_server.serve().await.unwrap();
        });

        let conn_str = Arc::new(format!(
            "user={frontegg_user} password={frontegg_password} host={} port={} sslmode=require",
            balancer_pgwire_listen.ip(),
            balancer_pgwire_listen.port()
        ));

        let tls = make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
            Ok(b.set_verify(SslVerifyMode::NONE))
        }));

        let (pg_client, conn) = tokio_postgres::connect(&conn_str, tls.clone())
            .await
            .unwrap();
        task::spawn(|| "balancer-pg_client", async move {
            let _ = conn.await;
        });

        let res: i32 = pg_client.query_one("SELECT 2", &[]).await.unwrap().get(0);
        assert_eq!(res, 2);

        // Assert cancellation is propagated.
        let cancel = pg_client.cancel_token();
        let copy = pg_client
            .copy_out("copy (subscribe (select * from mz_kafka_sinks)) to stdout")
            .await
            .unwrap();
        let _ = cancel.cancel_query(tls).await;
        let e = pin!(copy).next().await.unwrap().unwrap_err();
        assert_contains!(
            e.to_string_with_causes(),
            "canceling statement due to user request"
        );

        // Various tests about reloading of certs.

        // Assert the current certificate is as expected.
        let https_url = format!(
            "https://{host}:{port}/api/sql",
            host = balancer_https_listen.ip(),
            port = balancer_https_listen.port()
        );
        let resp = client
            .post(&https_url)
            .header("Content-Type", "application/json")
            .basic_auth(frontegg_user, Some(&frontegg_password))
            .body(body)
            .send()
            .await
            .unwrap();
        let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
        let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
        let server_x509 = X509::from_pem(&std::fs::read(&server_cert).unwrap()).unwrap();
        assert_eq!(resp_x509, server_x509);
        assert_contains!(resp.text().await.unwrap(), "12234");

        // Generate new certs. Install only the key, reload, and make sure the old cert is still in
        // use.
        let (next_cert, next_key) = ca
            .request_cert("next", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
            .unwrap();
        let next_x509 = X509::from_pem(&std::fs::read(&next_cert).unwrap()).unwrap();
        assert_ne!(next_x509, server_x509);
        std::fs::copy(next_key, &server_key).unwrap();
        let (tx, rx) = oneshot::channel();
        reload_tx.try_send(Some(tx)).unwrap();
        let res = rx.await.unwrap();
        assert_err!(res);

        // We should still be on the old cert because now the cert and key mismatch.
        let resp = client
            .post(&https_url)
            .header("Content-Type", "application/json")
            .basic_auth(frontegg_user, Some(&frontegg_password))
            .body(body)
            .send()
            .await
            .unwrap();
        let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
        let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
        assert_eq!(resp_x509, server_x509);

        // Now move the cert too. Reloading should succeed and the response should have the new
        // cert.
        std::fs::copy(next_cert, &server_cert).unwrap();
        let (tx, rx) = oneshot::channel();
        reload_tx.try_send(Some(tx)).unwrap();
        let res = rx.await.unwrap();
        assert_ok!(res);
        let resp = client
            .post(&https_url)
            .header("Content-Type", "application/json")
            .basic_auth(frontegg_user, Some(&frontegg_password))
            .body(body)
            .send()
            .await
            .unwrap();
        let tlsinfo = resp.extensions().get::<reqwest::tls::TlsInfo>().unwrap();
        let resp_x509 = X509::from_der(tlsinfo.peer_certificate().unwrap()).unwrap();
        assert_eq!(resp_x509, next_x509);

        if !is_multi_tenant_resolver {
            continue;
        }

        // Test de-duplication in the frontegg resolver. This is a bit racy so use a retry loop.
        Retry::default()
            .max_duration(Duration::from_secs(30))
            .retry_async(|_| async {
                let start_auth_count = *frontegg_server.auth_requests.lock().unwrap();
                const CONNS: u64 = 10;
                let mut handles = Vec::with_capacity(usize::cast_from(CONNS));
                for _ in 0..CONNS {
                    let conn_str = Arc::clone(&conn_str);
                    let handle = task::spawn(|| "test conn", async move {
                        let (pg_client, conn) = tokio_postgres::connect(
                            &conn_str,
                            make_pg_tls(Box::new(|b: &mut SslConnectorBuilder| {
                                Ok(b.set_verify(SslVerifyMode::NONE))
                            })),
                        )
                        .await
                        .unwrap();
                        task::spawn(|| "balancer-pg_client", async move {
                            let _ = conn.await;
                        });
                        let res: i32 = pg_client.query_one("SELECT 2", &[]).await.unwrap().get(0);
                        assert_eq!(res, 2);
                    });
                    handles.push(handle);
                }
                for handle in handles {
                    handle.await;
                }
                let end_auth_count = *frontegg_server.auth_requests.lock().unwrap();
                // We expect that the auth count increased by fewer than the number of connections.
                if end_auth_count == start_auth_count + CONNS {
                    // No deduplication was done, try again.
                    return Err("no auth dedup");
                }
                Ok(())
            })
            .await
            .unwrap();

        // Assert some metrics are being tracked.
        let metrics_url = format!(
            "http://{host}:{port}/metrics",
            host = balancer_https_internal.ip(),
            port = balancer_https_internal.port()
        );
        Retry::default()
            .max_duration(Duration::from_secs(30))
            .retry_async(|_| async {
                let resp = client
                    .get(&metrics_url)
                    .send()
                    .await
                    .unwrap()
                    .text()
                    .await
                    .unwrap();
                if !resp.contains("mz_balancer_tenant_connection_active") {
                    return Err("mz_balancer_tenant_connection_active");
                }
                if !resp.contains("mz_balancer_tenant_connection_rx") {
                    return Err("mz_balancer_tenant_connection_rx");
                }
                Ok(())
            })
            .await
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// WebTransport tests
// ---------------------------------------------------------------------------

/// Read one backend message: (type_byte, payload).
/// The payload does NOT include the 4-byte length prefix.
async fn wt_read_msg(recv: &mut RecvStream) -> (u8, Vec<u8>) {
    let mut header = [0u8; 5];
    recv.read_exact(&mut header).await.unwrap();
    let msg_type = header[0];
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut payload = vec![0u8; length - 4];
    if !payload.is_empty() {
        recv.read_exact(&mut payload).await.unwrap();
    }
    (msg_type, payload)
}

/// Run startup + auth (cleartext only) on a WebTransport stream pair.
/// Returns (conn_id, secret_key) from BackendKeyData.
async fn wt_startup(
    send: &mut SendStream,
    recv: &mut RecvStream,
    user: &str,
    database: &str,
    password: &str,
) -> (u32, u32) {
    // Encode startup message: 4-byte length + 4-byte version + params + double null.
    const VERSION_3: i32 = 196608; // 0x00030000
    let mut params_bytes = Vec::new();
    for (k, v) in [("user", user), ("database", database)] {
        params_bytes.extend_from_slice(k.as_bytes());
        params_bytes.push(0);
        params_bytes.extend_from_slice(v.as_bytes());
        params_bytes.push(0);
    }
    params_bytes.push(0); // end of params
    let length = (4 + 4 + params_bytes.len()) as u32;
    let mut msg = Vec::new();
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(&VERSION_3.to_be_bytes());
    msg.extend_from_slice(&params_bytes);
    send.write_all(&msg).await.unwrap();

    let mut conn_id = 0u32;
    let mut secret_key = 0u32;

    loop {
        let (msg_type, payload) = wt_read_msg(recv).await;
        match msg_type {
            b'R' => {
                // Authentication message
                let auth_type =
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                match auth_type {
                    0 => {} // AuthenticationOk — ignore
                    3 => {
                        // AuthenticationCleartextPassword — send password
                        let pwd = password.as_bytes();
                        let length = (4 + pwd.len() + 1) as u32;
                        let mut msg = vec![b'p'];
                        msg.extend_from_slice(&length.to_be_bytes());
                        msg.extend_from_slice(pwd);
                        msg.push(0);
                        send.write_all(&msg).await.unwrap();
                    }
                    _ => panic!("unexpected auth type {auth_type}"),
                }
            }
            b'K' => {
                // BackendKeyData
                conn_id =
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                secret_key =
                    u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            }
            b'S' => {} // ParameterStatus — ignore
            b'Z' => break, // ReadyForQuery
            b'E' => panic!("startup error: {}", parse_pg_error(&payload)),
            _ => {}
        }
    }

    (conn_id, secret_key)
}

/// Send a simple query and collect all DataRow columns as strings.
async fn wt_simple_query(
    send: &mut SendStream,
    recv: &mut RecvStream,
    query: &str,
) -> Vec<Vec<String>> {
    let q = query.as_bytes();
    let length = (4 + q.len() + 1) as u32;
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(q);
    msg.push(0);
    send.write_all(&msg).await.unwrap();

    let mut rows: Vec<Vec<String>> = Vec::new();
    loop {
        let (msg_type, payload) = wt_read_msg(recv).await;
        match msg_type {
            b'T' => {} // RowDescription — ignore field metadata
            b'D' => {
                // DataRow
                let n = u16::from_be_bytes([payload[0], payload[1]]) as usize;
                let mut row = Vec::with_capacity(n);
                let mut offset = 2;
                for _ in 0..n {
                    let len = i32::from_be_bytes([
                        payload[offset],
                        payload[offset + 1],
                        payload[offset + 2],
                        payload[offset + 3],
                    ]);
                    offset += 4;
                    if len < 0 {
                        row.push(String::new()); // NULL
                    } else {
                        let s = std::str::from_utf8(
                            &payload[offset..offset + len as usize],
                        )
                        .unwrap()
                        .to_owned();
                        offset += len as usize;
                        row.push(s);
                    }
                }
                rows.push(row);
            }
            b'C' => {} // CommandComplete
            b'Z' => break, // ReadyForQuery
            b'E' => panic!("query error: {}", parse_pg_error(&payload)),
            b'N' => {} // NoticeResponse
            _ => {}
        }
    }
    rows
}

/// Extract the 'M' (message) field from an ErrorResponse payload.
fn parse_pg_error(payload: &[u8]) -> String {
    let mut i = 0;
    while i < payload.len() && payload[i] != 0 {
        let field_type = payload[i];
        i += 1;
        let start = i;
        while i < payload.len() && payload[i] != 0 {
            i += 1;
        }
        if field_type == b'M' {
            return String::from_utf8_lossy(&payload[start..i]).into_owned();
        }
        i += 1; // skip null terminator
    }
    "<no message>".to_owned()
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
#[cfg_attr(miri, ignore)] // too slow
async fn test_webtransport() {
    let ca = Ca::new_root("test ca").unwrap();
    let (server_cert, server_key) = ca
        .request_cert("server", vec![IpAddr::V4(Ipv4Addr::LOCALHOST)])
        .unwrap();
    let metrics_registry = MetricsRegistry::new();

    // Frontegg setup (same as test_balancer).
    let tenant_id = Uuid::new_v4();
    let email = "wt_user@_.com".to_string();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let initial_api_tokens = vec![ApiToken {
        client_id: client_id.clone(),
        secret: secret.clone(),
        description: None,
        created_at: Utc::now(),
    }];
    let users = BTreeMap::from([(
        email.clone(),
        UserConfig {
            id: Uuid::new_v4(),
            email: email.clone(),
            password,
            tenant_id,
            initial_api_tokens,
            roles: Vec::new(),
            auth_provider: None,
            verified: None,
            metadata: None,
        },
    )]);
    let issuer = "frontegg-mock".to_owned();
    let encoding_key =
        EncodingKey::from_rsa_pem(&ca.pkey.private_key_to_pem_pkcs8().unwrap()).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap()).unwrap();
    let frontegg_server = FronteggMockServer::start(
        None,
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        SYSTEM_TIME.clone(),
        50,
        None,
        None,
    )
    .await
    .unwrap();
    let frontegg_auth = FronteggAuthentication::new(
        FronteggConfig {
            admin_api_token_url: frontegg_server.auth_api_token_url(),
            decoding_key: DecodingKey::from_rsa_pem(&ca.pkey.public_key_to_pem().unwrap())
                .unwrap(),
            tenant_id: Some(tenant_id),
            now: SYSTEM_TIME.clone(),
            admin_role: "mzadmin".to_string(),
            refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
            refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
        },
        mz_frontegg_auth::Client::default(),
        &metrics_registry,
    );
    let frontegg_password = format!("mzp_{client_id}{secret}");

    // Start environmentd without TLS — the WebTransport balancer uses
    // internal_tls=false so the backend connection is plain TCP.
    let envd_server = test_util::TestHarness::default()
        .with_frontegg_auth(&frontegg_auth)
        .with_metrics_registry(metrics_registry)
        .start()
        .await;

    // Start balancer with WebTransport enabled on an OS-assigned port.
    let (_, reload_rx) = futures::channel::mpsc::channel(1);
    let balancer_cfg = BalancerConfig::new(
        &BUILD_INFO,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
        CancellationResolver::Static(envd_server.sql_local_addr().to_string()),
        Resolver::Static(envd_server.sql_local_addr().to_string()),
        envd_server.http_local_addr().to_string(),
        Some(TlsCertConfig {
            cert: server_cert.clone(),
            key: server_key.clone(),
        }),
        false, // internal_tls: backend is plain TCP
        MetricsRegistry::new(),
        Box::pin(reload_rx),
        None,
        None,
        Duration::ZERO,
        None,
        None,
        None,
        TracingHandle::disabled(),
        vec![],
    );
    let balancer = BalancerService::new(balancer_cfg).await.unwrap();
    let wt_addr = balancer
        .webtransport_local_addr
        .expect("WebTransport listener should be bound");
    task::spawn(|| "wt_balancer", async move {
        balancer.serve().await.unwrap();
    });

    let wt_url = format!("https://127.0.0.1:{}/pgwire", wt_addr.port());

    // Build a wtransport client that skips certificate validation (test CA).
    let client_config = ClientConfig::builder()
        .with_bind_default()
        .with_no_cert_validation()
        .build();
    let endpoint = Endpoint::client(client_config).unwrap();
    let conn = endpoint.connect(&wt_url).await.unwrap();

    // --- Sub-test 1: basic query ---
    {
        let (mut send, mut recv) = conn.open_bi().await.unwrap().await.unwrap();
        let _ = wt_startup(&mut send, &mut recv, &email, "materialize", &frontegg_password).await;
        let rows = wt_simple_query(&mut send, &mut recv, "SELECT 1").await;
        assert_eq!(rows, vec![vec!["1".to_string()]]);
    }

    // --- Sub-test 2: multiple queries on a second stream ---
    {
        let (mut send2, mut recv2) = conn.open_bi().await.unwrap().await.unwrap();
        let (_conn_id, _secret_key) =
            wt_startup(&mut send2, &mut recv2, &email, "materialize", &frontegg_password).await;
        let rows = wt_simple_query(&mut send2, &mut recv2, "SELECT 2 AS two").await;
        assert_eq!(rows, vec![vec!["2".to_string()]]);
    }
}
