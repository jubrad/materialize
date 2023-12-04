// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hyper::service::{make_service_fn, service_fn};
use hyper::{body, Body, Request, Response, Server as HyperServer};
use jsonwebtoken::EncodingKey;
use mz_frontegg_auth::{ApiTokenArgs, ApiTokenResponse, Claims, RefreshToken, REFRESH_SUFFIX};
use mz_ore::now::NowFn;
use mz_ore::retry::Retry;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct FronteggMockServer {
    pub url: String,
    pub refreshes: Arc<Mutex<u64>>,
    pub enable_refresh: Arc<AtomicBool>,
    pub auth_requests: Arc<Mutex<u64>>,
    pub role_updates_tx: UnboundedSender<(String, Vec<String>)>,
    pub handle: JoinHandle<Result<(), hyper::Error>>,
}

impl FronteggMockServer {
    /// Starts a [`FronteggMockServer`], must be started from within a [`tokio::runtime::Runtime`].
    ///
    /// Users is a mapping from (client, secret) -> email address.
    pub fn start(
        addr: Option<&SocketAddr>,
        encoding_key: EncodingKey,
        tenant_id: Uuid,
        users: BTreeMap<(String, String), String>,
        roles: BTreeMap<String, Vec<String>>,
        now: NowFn,
        expires_in_secs: i64,
        latency: Option<Duration>,
    ) -> Result<FronteggMockServer, anyhow::Error> {
        let (role_updates_tx, role_updates_rx) = unbounded_channel();
        let refreshes = Arc::new(Mutex::new(0u64));
        let enable_refresh = Arc::new(AtomicBool::new(true));
        let auth_requests = Arc::new(Mutex::new(0u64));
        let context = Context {
            encoding_key,
            tenant_id,
            users,
            roles,
            role_updates_rx: Arc::new(Mutex::new(role_updates_rx)),
            now,
            expires_in_secs,
            latency,
            refresh_tokens: Arc::new(Mutex::new(BTreeMap::new())),
            refreshes: Arc::clone(&refreshes),
            enable_refresh: Arc::clone(&enable_refresh),
            auth_requests: Arc::clone(&auth_requests),
        };

        let service = make_service_fn(move |_conn| {
            let mut context = context.clone();
            let service = service_fn(move |req| {
                while let Ok((email, roles)) = context.role_updates_rx.lock().unwrap().try_recv() {
                    context.roles.insert(email, roles);
                }
                Self::handle(context.clone(), req)
            });
            async move { Ok::<_, Infallible>(service) }
        });
        let addr = match addr {
            Some(addr) => Cow::Borrowed(addr),
            None => Cow::Owned(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
        };
        let server = HyperServer::bind(&addr).serve(service);
        let url = format!("http://{}/", server.local_addr());
        let handle = mz_ore::task::spawn(|| "mzcloud-mock-server", server);

        Ok(FronteggMockServer {
            url,
            refreshes,
            enable_refresh,
            auth_requests,
            role_updates_tx,
            handle,
        })
    }

    async fn handle(context: Context, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        // In some cases we want to add latency to test de-duplicating results.
        if let Some(latency) = context.latency {
            tokio::time::sleep(latency).await;
        }

        let (parts, body) = req.into_parts();
        let body = body::to_bytes(body).await.unwrap();
        let email: String = if parts.uri.path().ends_with(REFRESH_SUFFIX) {
            // Always count refresh attempts, even if enable_refresh is false.
            *context.refreshes.lock().unwrap() += 1;
            let args: RefreshToken = serde_json::from_slice(&body).unwrap();
            match (
                context
                    .refresh_tokens
                    .lock()
                    .unwrap()
                    .remove(args.refresh_token),
                context.enable_refresh.load(Ordering::Relaxed),
            ) {
                (Some(email), true) => email.to_string(),
                _ => {
                    return Ok(Response::builder()
                        .status(400)
                        .body(Body::from("unknown refresh token"))
                        .unwrap())
                }
            }
        } else {
            *context.auth_requests.lock().unwrap() += 1;
            let args: ApiTokenArgs = serde_json::from_slice(&body).unwrap();
            match context
                .users
                .get(&(args.client_id.to_string(), args.secret.to_string()))
            {
                Some(email) => email.to_string(),
                None => {
                    return Ok(Response::builder()
                        .status(400)
                        .body(Body::from("unknown user"))
                        .unwrap())
                }
            }
        };
        let roles = context.roles.get(&email).cloned().unwrap_or_default();
        let refresh_token = Uuid::new_v4().to_string();
        context
            .refresh_tokens
            .lock()
            .unwrap()
            .insert(refresh_token.clone(), email.clone());
        let access_token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &Claims {
                exp: context.now.as_secs() + context.expires_in_secs,
                email,
                sub: Uuid::new_v4(),
                user_id: None,
                tenant_id: context.tenant_id,
                roles,
                permissions: Vec::new(),
            },
            &context.encoding_key,
        )
        .unwrap();
        let resp = ApiTokenResponse {
            expires: "".to_string(),
            expires_in: context.expires_in_secs,
            access_token,
            refresh_token,
        };
        Ok(Response::new(Body::from(
            serde_json::to_vec(&resp).unwrap(),
        )))
    }

    pub fn wait_for_refresh(&self, expires_in_secs: u64) {
        let expected = *self.refreshes.lock().unwrap() + 1;
        Retry::default()
            .factor(1.0)
            .max_duration(Duration::from_secs(expires_in_secs + 20))
            .retry(|_| {
                let refreshes = *self.refreshes.lock().unwrap();
                if refreshes >= expected {
                    Ok(())
                } else {
                    Err(format!(
                        "expected refresh count {}, got {}",
                        expected, refreshes
                    ))
                }
            })
            .unwrap();
    }
}

#[derive(Clone)]
struct Context {
    encoding_key: EncodingKey,
    tenant_id: Uuid,
    users: BTreeMap<(String, String), String>,
    roles: BTreeMap<String, Vec<String>>,
    role_updates_rx: Arc<Mutex<UnboundedReceiver<(String, Vec<String>)>>>,
    now: NowFn,
    expires_in_secs: i64,
    latency: Option<Duration>,
    // Uuid -> email
    refresh_tokens: Arc<Mutex<BTreeMap<String, String>>>,
    refreshes: Arc<Mutex<u64>>,
    enable_refresh: Arc<AtomicBool>,
    auth_requests: Arc<Mutex<u64>>,
}
