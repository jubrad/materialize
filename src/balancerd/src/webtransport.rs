// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WebTransport-to-pgwire proxy for balancerd.
//!
//! Accepts HTTP/3 WebTransport connections from browsers. Each bidirectional
//! stream maps to one full pgwire session (startup → auth → queries) forwarded
//! to a backend environmentd TCP port.
//!
//! WebTransport is already TLS-encrypted, so no `SslRequest` negotiation is
//! needed between the browser client and balancerd.
//!
//! # Resolver support
//!
//! Only [`crate::Resolver::Static`] is currently supported. The multi-tenant
//! Frontegg resolver requires an interactive password challenge that is not yet
//! implemented over WebTransport streams.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use bytes::BytesMut;
use mz_pgwire_common::{FrontendStartupMessage, VERSION_3, decode_startup};
use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpStream;
use tracing::{debug, info, warn};
use wtransport::endpoint::endpoint_side::Server as ServerSide;
use wtransport::tls::Identity;
use wtransport::{Endpoint, RecvStream, SendStream, ServerConfig};

use crate::Resolver;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// A WebTransport server endpoint that has been bound to a port and is ready
/// to serve. Created by [`BoundWebTransport::bind`].
pub struct BoundWebTransport {
    /// The actual local address the WebTransport listener is bound to.
    /// Useful for tests that bind to port 0.
    pub local_addr: SocketAddr,
    endpoint: Endpoint<ServerSide>,
}

impl BoundWebTransport {
    /// Bind a WebTransport (HTTP/3) server endpoint to `listen_addr`.
    ///
    /// Loads the TLS identity from the provided PEM files. Returns a
    /// [`BoundWebTransport`] whose `local_addr` reflects the actual bound
    /// address (useful when `listen_addr` has port 0).
    pub async fn bind(
        listen_addr: SocketAddr,
        tls_cert: &Path,
        tls_key: &Path,
    ) -> Result<Self, anyhow::Error> {
        let identity = Identity::load_pemfiles(tls_cert, tls_key)
            .await
            .context("loading TLS identity for WebTransport listener")?;

        let config = ServerConfig::builder()
            .with_bind_address(listen_addr)
            .with_identity(identity)
            .keep_alive_interval(Some(std::time::Duration::from_secs(15)))
            .build();

        let endpoint = Endpoint::server(config).context("creating WebTransport endpoint")?;
        let local_addr = endpoint
            .local_addr()
            .context("getting WebTransport local address")?;

        Ok(Self { local_addr, endpoint })
    }

    /// Run the WebTransport accept loop. Never returns under normal operation.
    pub(crate) async fn serve(
        self,
        resolver: Arc<Resolver>,
        internal_tls: bool,
    ) -> Result<(), anyhow::Error> {
        info!("WebTransport pgwire listening on {}", self.local_addr);

        loop {
            let incoming = self.endpoint.accept().await;
            let resolver = Arc::clone(&resolver);

            tokio::spawn(async move {
                let request = match incoming.await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("WebTransport session handshake error: {e}");
                        return;
                    }
                };

                if request.path() != "/pgwire" {
                    warn!(
                        "WebTransport: rejecting unexpected path {}",
                        request.path()
                    );
                    request.not_found().await;
                    return;
                }

                let connection = match request.accept().await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("WebTransport: failed to accept session: {e}");
                        return;
                    }
                };

                debug!(
                    "WebTransport: accepted session from {}",
                    connection.remote_address()
                );

                loop {
                    let (send, recv) = match connection.accept_bi().await {
                        Ok(s) => s,
                        Err(e) => {
                            debug!("WebTransport: session ended: {e}");
                            break;
                        }
                    };
                    let resolver = Arc::clone(&resolver);
                    tokio::spawn(async move {
                        if let Err(e) =
                            handle_stream(send, recv, &resolver, internal_tls).await
                        {
                            warn!("WebTransport pgwire stream error: {e:#}");
                        }
                    });
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Per-stream handler
// ---------------------------------------------------------------------------

async fn handle_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    resolver: &Resolver,
    _internal_tls: bool,
) -> Result<(), anyhow::Error> {
    let startup = decode_startup(&mut recv)
        .await
        .context("reading startup message")?
        .context("client closed before sending startup message")?;

    let (version, params) = match startup {
        FrontendStartupMessage::Startup { version, params } => (version, params),
        FrontendStartupMessage::SslRequest => {
            send.write_all(b"N").await?;
            anyhow::bail!("unexpected SslRequest over WebTransport");
        }
        FrontendStartupMessage::CancelRequest {
            conn_id,
            secret_key,
        } => {
            let backend_addr = match resolver {
                Resolver::Static(addr) => super::lookup(addr).await?,
                Resolver::MultiTenant(..) => {
                    anyhow::bail!("CancelRequest over WebTransport requires static resolver");
                }
            };
            let mut buf = BytesMut::new();
            FrontendStartupMessage::CancelRequest {
                conn_id,
                secret_key,
            }
            .encode(&mut buf)
            .context("encoding CancelRequest")?;
            let mut tcp = TcpStream::connect(backend_addr)
                .await
                .with_context(|| format!("connecting to backend {backend_addr} for cancel"))?;
            tcp.write_all(&buf).await?;
            tcp.shutdown().await?;
            return Ok(());
        }
        FrontendStartupMessage::GssEncRequest => {
            anyhow::bail!("GssEncRequest over WebTransport is not supported");
        }
    };

    if version != VERSION_3 {
        anyhow::bail!(
            "unsupported pgwire protocol version: {version:#010x} (expected {VERSION_3:#010x})"
        );
    }

    let user = params
        .get("user")
        .context("startup message missing 'user' parameter")?
        .clone();

    let backend_addr = match resolver {
        Resolver::Static(addr) => super::lookup(addr).await?,
        Resolver::MultiTenant(..) => {
            anyhow::bail!(
                "WebTransport does not yet support multi-tenant Frontegg resolver \
                 (user: {user}); use --static-resolver-addr for local testing"
            );
        }
    };

    let mut backend = TcpStream::connect(backend_addr)
        .await
        .with_context(|| format!("connecting to backend {backend_addr}"))?;

    let mut buf = BytesMut::new();
    FrontendStartupMessage::Startup { version, params }
        .encode(&mut buf)
        .context("encoding startup message for backend")?;
    backend
        .write_all(&buf)
        .await
        .context("sending startup message to backend")?;
    backend.flush().await?;

    let mut client = tokio::io::join(recv, send);
    tokio::io::copy_bidirectional(&mut client, &mut backend)
        .await
        .context("proxying pgwire stream")?;

    Ok(())
}
