---
name: pg-webtransport + WebTransport balancer
description: Browser-native pgwire via WebTransport — TypeScript client and Rust balancer implementation status
type: project
---

Status: **Implemented and compiling** (as of 2026-05-29)

## TypeScript client (`misc/pg-webtransport/`)

Full pgwire-over-WebTransport client for browsers:
- `src/codec.ts` — full pgwire binary protocol encode/decode (Web Crypto API, no Node.js)
- `src/auth.ts` — SCRAM-SHA-256 (crypto.subtle), cleartext password, OIDC JWT
- `src/client.ts` — `Client` class with `connect()`, `query()`, `execute()`, `close()`
- `src/react.ts` — `createMaterializeConnection`, `useQuery`, `useMutation` React hooks
- `src/index.ts` — re-exports all public API

**OIDC auth**: pass JWT token as `token` field in `ClientOptions`; sent as cleartext password when server requests it. `token ?? password` precedence.

## Rust balancer (`src/balancerd/`)

New file: `src/webtransport.rs` — `WebTransportBalancer` struct
- Uses `wtransport = "0.7"` (added to workspace Cargo.toml)
- `ServerConfig::builder().with_bind_address(addr).with_identity(identity).build()`
- Accept loop: `endpoint.accept().await` → session request at `/pgwire` path
- Per-stream: reads pgwire startup, resolves backend via `Resolver::Static`, proxies bidirectionally
- `wtransport 0.7.x` API: `accept_bi()` returns `(SendStream, RecvStream)` tuple

**Only `Resolver::Static` supported** — multi-tenant Frontegg requires interactive password challenge (TODO).

Changes to `lib.rs`:
- `mod webtransport;` added
- `BalancerConfig` has new `webtransport_listen_addr: Option<SocketAddr>` field
- `shared_resolver = Arc::new(self.cfg.resolver)` extracted before pgwire block so both can share it
- WebTransport spawn block in `serve()` after internal_http

Changes to `bin/balancerd.rs`:
- `--webtransport-listen-addr HOST:PORT` optional CLI arg
- Passed to `BalancerConfig::new()` as 4th positional arg (after https_listen_addr)

**Why:** explore full native pgwire in browser, experiment, no half-measures

**How to apply:** When asked about WebTransport/pgwire browser work, know it's implemented. Next step would be multi-tenant Frontegg support and HTTP/2 for the HTTP endpoints.
