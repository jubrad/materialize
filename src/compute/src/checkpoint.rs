// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Arrangement checkpoints (spike).
//!
//! A supported dataflow periodically writes the contents of each of its arrangements to persist,
//! one shard per `(arrangement site, worker)`. When the dataflow is re-created with an identical
//! checkpoint key, every arrangement and reduce starts from its state at the committed checkpoint
//! frontier `F`, and the inputs replay only the updates at `F` and later.
//!
//! The pieces:
//!
//! * [`plan_checkpoint`] runs on the worker that sequences commands, before a `CreateDataflow` is
//!   broadcast. It reads the manifest and shard uppers and records the restore frontier (or a new
//!   checkpoint generation) in the dataflow description, so every worker sees the same decision.
//! * [`RenderGuard::begin`] installs a thread-local render context for the duration of
//!   `build_compute_dataflow`. Arrange and reduce sites call [`next_site`] in rendering order, which
//!   is deterministic for a given key, so site ordinals line up across incarnations.
//! * [`attach_writer`] follows an arrangement's batch stream and appends its updates in rounds,
//!   each covering every interval boundary passed since the previous round.
//! * [`checkpoint_sink`] checkpoints an MV sink's desired collection the same way, and feeds a
//!   restored sink that collection's snapshot, so the sink self-corrects as it would from
//!   scratch.
//! * The committer holds input and output history, and the checkpoint shards' own `since`, one
//!   interval behind the committed frontier, so persist compacts superseded rounds away.
//!
//! Only the latest generation of the latest key of a dataflow is kept. A new generation
//! finalizes the previous one's shards, a new key finalizes the previous key's, and dropping all
//! of a dataflow's exports finalizes its key.
//!
//! See `doc/developer/design/20261106_arrangement_checkpoints.md`.

// TODO: Restore reads and manifest updates block the worker thread while a dataflow renders, so
// they run on `futures::executor::block_on`. Timely worker threads are not inside a tokio
// runtime, so `Handle::block_on` would need a handle passed in. The planned fix moves restore
// reads off the worker thread entirely.
#![allow(clippy::disallowed_methods)]

use std::any::{Any, TypeId};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use columnation::Columnation;
use differential_dataflow::Hashable;
use differential_dataflow::batcher::Batcher;
use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::trace::cursor::{BatchCursor, CursorList, Navigable, cursor_list};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Cursor, Description, Span, TraceReader};
use futures::StreamExt;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_types::dataflows::{CheckpointRestore, CheckpointSource, RenderDataflowDescription};
use mz_compute_types::dyncfgs::{
    ARRANGEMENT_CHECKPOINT_INTERVAL, ARRANGEMENT_CHECKPOINT_MAX_LAG,
    ENABLE_ARRANGEMENT_CHECKPOINTS, ENABLE_SYNC_MV_SINK,
};
use mz_compute_types::plan::render_plan::Expr;
use mz_compute_types::sinks::ComputeSinkConnection;
use mz_dyncfg::ConfigSet;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::{Opaque, SinceHandle};
use mz_persist_client::read::{Cursor as PersistCursor, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::codec_impls::{UnitSchema, VecU8Schema};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton};
use mz_timely_util::columnation::ColumnationStack;
use mz_timely_util::operator::BatcherNew;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Stream, StreamVec};
use timely::progress::{Antichain, Timestamp as _};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use crate::render::errors::DataflowErrorSer;
use crate::typedefs::{
    ErrBatcher, ErrBuilder, ErrSpine, RowErrBatcher, RowErrBuilder, RowErrSpine, RowRowSpine,
};
use mz_row_spine::{RowRowBatcher, RowRowBuilder, StreamSealer};

/// Bumped whenever the on-shard encoding changes.
const FORMAT_VERSION: u64 = 2;

/// An update as stored in a checkpoint shard.
type Update = ((Vec<u8>, Vec<u8>), Timestamp, i64);

type CkptWriteHandle = WriteHandle<Vec<u8>, Vec<u8>, Timestamp, i64>;
type CkptReadHandle = ReadHandle<Vec<u8>, Vec<u8>, Timestamp, i64>;
type CkptSinceHandle = SinceHandle<Vec<u8>, Vec<u8>, Timestamp, i64>;
type InputReadHandle = ReadHandle<SourceData, (), Timestamp, StorageDiff>;

/// Identity of the running binary.
///
/// `build_info!()` takes the git SHA of `HEAD`, which does not change when a local build has
/// uncommitted changes, so a stale checkpoint could match a different build. Hashing the binary
/// itself is exact.
///
/// NOTE: The binary is hundreds of MiB with debug info, so it is hashed in blocks. Reading it
/// whole raised the replica's peak memory by the binary's size.
static BUILD_IDENTITY: LazyLock<String> = LazyLock::new(|| {
    use std::io::Read;
    let exe = std::env::current_exe().expect("current executable path");
    let mut file = std::fs::File::open(&exe).expect("readable current executable");
    let mut hasher = Sha256::new();
    let mut block = vec![0; 1 << 20];
    loop {
        let n = file.read(&mut block).expect("readable current executable");
        if n == 0 {
            break;
        }
        hasher.update(&block[..n]);
    }
    hex(&hasher.finalize())
});

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Digests what a checkpoint depends on besides the dataflow: format, build, worker count, and
/// configuration. Every checkpoint key starts with it.
fn environment_digest(peers: usize, config: &ConfigSet) -> String {
    let mut hasher = Sha256::new();
    hasher.update(FORMAT_VERSION.to_le_bytes());
    hasher.update(BUILD_IDENTITY.as_bytes());
    hasher.update(peers.to_le_bytes());
    let mut entries: Vec<_> = config
        .entries()
        .map(|e| format!("{}={:?}", e.name(), e.val()))
        .collect();
    entries.sort();
    for entry in entries {
        hasher.update(entry.as_bytes());
    }
    hex(&hasher.finalize())
}

/// The key of the same dataflow on a replica with `peers` workers. Keys differ across worker
/// counts only in their environment digest, a fixed-length prefix.
fn key_for_peers(key: &str, peers: usize, config: &ConfigSet) -> String {
    let environment = environment_digest(peers, config);
    format!("{environment}{}", &key[environment.len()..])
}

/// Records the worker counts that have started a checkpoint generation of a dataflow, so a
/// replica of another size can restore from one.
fn peers_shard(key: &str, config: &ConfigSet) -> ShardId {
    derived_shard(&key_for_peers(key, 0, config), "peers")
}

/// The worker that the arrangement exchange pacts send a key with this hash to.
///
/// Timely's distributors take `hash & (peers - 1)` for a power of two and `hash % peers`
/// otherwise, which agree. Every checkpointed arrangement is partitioned this way by the hash of
/// its key: `ArrangeBy` and `mz_arrange` exchange by `key.hashed()` (`Row` hashes as its
/// `RowRef`, so the columnar pact agrees), and a reduce arranges its output by its input's key
/// without an exchange. Restore checks every key it reads against the worker that wrote it.
fn route(hash: u64, peers: usize) -> usize {
    let peers = u64::try_from(peers).expect("fits");
    usize::try_from(hash % peers).expect("less than peers")
}

/// The workers of a `source_peers` replica whose partitions can hold keys that `worker` of a
/// `peers` replica owns. A hash with `h % source_peers == j` and `h % peers == worker` exists
/// exactly when `j` and `worker` agree modulo the gcd of the two counts.
fn source_workers(worker: usize, peers: usize, source_peers: usize) -> Vec<usize> {
    let gcd = gcd(peers, source_peers);
    (0..source_peers)
        .filter(|j| j % gcd == worker % gcd)
        .collect()
}

/// Computes the checkpoint key of a dataflow, or the reason it is not eligible.
///
/// Two dataflows with the same key render identical arrangements in the same order, from the same
/// inputs, with the same code and configuration. The key is the environment digest followed by a
/// digest of the dataflow.
///
/// NOTE: Only the planning worker can compute it. `split_command` gives each worker its own part
/// of the plan, for example its slice of a constant collection, so workers verify only the
/// environment digest.
pub(crate) fn checkpoint_key(
    dataflow: &RenderDataflowDescription<CollectionMetadata>,
    peers: usize,
    config: &ConfigSet,
) -> Result<String, &'static str> {
    if dataflow.is_transient() {
        return Err("transient dataflow");
    }
    if !dataflow.index_imports.is_empty() {
        // An imported index replays its existing batches into this dataflow, which would
        // double-count against restored state.
        return Err("imports an index");
    }
    for sink in dataflow.sink_exports.values() {
        // Only the MV sink can start from its output shard in place of a snapshot.
        if !matches!(sink.connection, ComputeSinkConnection::MaterializedView(_)) {
            return Err("exports a sink other than a materialized view");
        }
        if sink.refresh_schedule.is_some() || !sink.with_snapshot || !sink.up_to.is_empty() {
            return Err("exports a sink with a refresh schedule, UP TO, or no snapshot");
        }
    }
    if !dataflow.sink_exports.is_empty() && ENABLE_SYNC_MV_SINK.get(config) {
        return Err("uses the sync MV sink");
    }
    if dataflow.index_exports.is_empty() && dataflow.sink_exports.is_empty() {
        return Err("exports nothing");
    }
    if dataflow.source_imports.is_empty() {
        return Err("imports no source");
    }
    for import in dataflow.source_imports.values() {
        if import.desc.storage_metadata.txns_shard.is_some() {
            // Replaying a txn-wal collection from `F` also needs a hold on the txns shard.
            return Err("imports a txn-wal collection");
        }
    }
    for object in &dataflow.objects_to_build {
        if object.plan.is_recursive() {
            return Err("recursive");
        }
        for stage in &object.plan.binds {
            for bind in &stage.lets {
                check_exprs(bind.value.exprs())?;
            }
        }
        check_exprs(object.plan.body.exprs())?;
    }

    let mut hasher = Sha256::new();
    // `Debug` output is deterministic for a given build, and the build is part of the key.
    hasher.update(format!("{:?}", dataflow.objects_to_build).as_bytes());
    hasher.update(format!("{:?}", dataflow.index_exports).as_bytes());
    hasher.update(format!("{:?}", dataflow.sink_exports).as_bytes());
    hasher.update(format!("{:?}", dataflow.time_dependence).as_bytes());
    for (id, import) in &dataflow.source_imports {
        // `import.upper` is a progress hint that changes on every restart.
        hasher.update(
            format!(
                "{id:?}{:?}{:?}{:?}",
                import.desc, import.monotonic, import.with_snapshot
            )
            .as_bytes(),
        );
    }
    Ok(format!(
        "{}{}",
        environment_digest(peers, config),
        hex(&hasher.finalize())
    ))
}

fn check_exprs<'a>(exprs: impl Iterator<Item = &'a Expr>) -> Result<(), &'static str> {
    for expr in exprs {
        match expr {
            Expr::TopK { .. } => return Err("top-k"),
            Expr::Threshold { .. } => return Err("threshold"),
            // `mz_join_core` skips joining the restored spans, see there. Delta joins look up
            // each input's new updates in the other traces, which include the restored spans.
            Expr::Join { .. }
            | Expr::Constant { .. }
            | Expr::Get { .. }
            | Expr::Mfp { .. }
            | Expr::FlatMap { .. }
            | Expr::Reduce { .. }
            | Expr::Negate { .. }
            | Expr::Union { .. }
            | Expr::ArrangeBy { .. } => {}
        }
    }
    Ok(())
}

/// Derives a shard ID from the checkpoint key, so every incarnation finds the same shards.
fn derived_shard(key: &str, role: &str) -> ShardId {
    let digest = Sha256::digest(format!("{key}/{role}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    format!("s{}", Uuid::from_bytes(bytes))
        .parse()
        .expect("valid shard id")
}

fn manifest_shard(key: &str) -> ShardId {
    derived_shard(key, "manifest")
}

fn site_shard(key: &str, generation: u64, site: usize, worker: usize) -> ShardId {
    derived_shard(
        key,
        &format!("gen/{generation}/site/{site}/worker/{worker}"),
    )
}

fn sink_shard(key: &str, generation: u64, sink: GlobalId, worker: usize) -> ShardId {
    derived_shard(
        key,
        &format!("gen/{generation}/sink/{sink}/worker/{worker}"),
    )
}

/// Identifies a dataflow across checkpoint keys: its exports and the replica's worker count.
///
/// Replicas of different sizes keep separate checkpoints of the same dataflow.
fn dataflow_identity(
    dataflow: &RenderDataflowDescription<CollectionMetadata>,
    peers: usize,
) -> String {
    let exports: BTreeSet<_> = dataflow.export_ids().collect();
    format!("exports/{exports:?}/peers/{peers}")
}

/// Records the latest checkpoint key of a dataflow identity, so a new key can finalize it.
fn pointer_shard(identity: &str) -> ShardId {
    derived_shard(identity, "pointer")
}

fn diagnostics(purpose: &str) -> Diagnostics {
    Diagnostics {
        shard_name: "arrangement_checkpoint".into(),
        handle_purpose: purpose.into(),
    }
}

/// The durable description of a checkpoint generation.
///
/// A generation starts when a dataflow is created without restoring. `sites` is filled in once
/// rendering finishes without encountering an unsupported arrangement. Restore requires it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Manifest {
    generation: u64,
    sites: Option<Vec<String>>,
    /// The generation's first checkpoint frontier. Every shard has a batch boundary at each
    /// multiple of the interval from here to its upper, and each is a valid restore frontier.
    first_frontier: Option<Timestamp>,
    /// The MV sinks, whose desired collections are checkpointed per worker. Filled in with
    /// `sites`.
    #[serde(default)]
    sinks: Vec<GlobalId>,
}

impl Manifest {
    /// The generation's site and sink shards, once rendering has recorded them.
    fn shards(&self, key: &str, peers: usize) -> Vec<ShardId> {
        let Some(sites) = &self.sites else {
            return Vec::new();
        };
        let generation = self.generation;
        let mut shards: Vec<_> = (0..sites.len())
            .flat_map(|site| {
                (0..peers).map(move |worker| site_shard(key, generation, site, worker))
            })
            .collect();
        shards.extend(self.sinks.iter().flat_map(|sink| {
            (0..peers).map(move |worker| sink_shard(key, generation, *sink, worker))
        }));
        shards
    }
}

async fn open_writer(client: &PersistClient, shard: ShardId, purpose: &str) -> CkptWriteHandle {
    client
        .open_writer(
            shard,
            Arc::new(VecU8Schema),
            Arc::new(VecU8Schema),
            diagnostics(purpose),
        )
        .await
        .expect("checkpoint shard codecs match")
}

async fn open_reader(client: &PersistClient, shard: ShardId, purpose: &str) -> CkptReadHandle {
    client
        .open_leased_reader(
            shard,
            Arc::new(VecU8Schema),
            Arc::new(VecU8Schema),
            diagnostics(purpose),
            false,
        )
        .await
        .expect("checkpoint shard codecs match")
}

/// Reads the full contents of a checkpoint shard at `as_of`.
async fn read_shard(
    client: &PersistClient,
    shard: ShardId,
    as_of: Timestamp,
) -> Result<Vec<Update>, String> {
    let mut read = open_reader(client, shard, "checkpoint restore").await;
    let result = read
        .snapshot_and_fetch(Antichain::from_elem(as_of))
        .await
        .map_err(|since| format!("checkpoint shard {shard} since {since:?} is beyond {as_of}"));
    read.expire().await;
    result
}

/// Reads the single record a manifest or pointer shard holds, and the shard's
/// upper, which the returned handle caches.
async fn read_record<R: DeserializeOwned>(
    client: &PersistClient,
    shard: ShardId,
) -> (Option<R>, CkptWriteHandle) {
    let mut write = open_writer(client, shard, "checkpoint record").await;
    let upper = write.fetch_recent_upper().await.clone();
    let Some(as_of) = upper.as_option().and_then(|upper| upper.step_back()) else {
        return (None, write);
    };
    let updates = read_shard(client, shard, as_of)
        .await
        .expect("record shards are never compacted past their upper");
    let record = updates
        .into_iter()
        .filter(|(_, _, diff)| *diff > 0)
        .map(|((k, _), _, _)| serde_json::from_slice(&k).expect("valid record"))
        .next();
    (record, write)
}

/// Replaces the record of a shard, closing it if `new` is `None`. Returns whether the write
/// won.
async fn write_record<R: Serialize>(
    write: &mut CkptWriteHandle,
    old: Option<&R>,
    new: Option<&R>,
) -> bool {
    let upper = write.fetch_recent_upper().await.clone();
    let Some(&lower) = upper.as_option() else {
        return false;
    };
    let mut updates = Vec::new();
    if let Some(new) = new {
        updates.push((
            (serde_json::to_vec(new).expect("serializable"), Vec::new()),
            lower,
            1_i64,
        ));
    }
    if let Some(old) = old {
        updates.push((
            (serde_json::to_vec(old).expect("serializable"), Vec::new()),
            lower,
            -1,
        ));
    }
    let new_upper = Antichain::from_elem(lower.step_forward());
    matches!(
        write
            .compare_and_append(&updates, upper, new_upper)
            .await
            .expect("valid usage"),
        Ok(())
    )
}

async fn read_manifest(client: &PersistClient, key: &str) -> (Option<Manifest>, CkptWriteHandle) {
    read_record(client, manifest_shard(key)).await
}

/// The opaque token of every checkpoint since handle. Checkpoint shards have no epoch to fence
/// on: a stale committer can only hold the since further back, never regress it.
fn since_opaque() -> Opaque {
    Opaque::encode(&0_u64)
}

/// Opens the critical since handle that holds a checkpoint shard's since.
///
/// It is not leased, so the since stays put while no committer runs, and a later restore can
/// still read at the committed frontier.
async fn open_since(client: &PersistClient, shard: ShardId, purpose: &str) -> CkptSinceHandle {
    client
        .open_critical_since(
            shard,
            PersistClient::CONTROLLER_CRITICAL_SINCE,
            since_opaque(),
            diagnostics(purpose),
        )
        .await
        .expect("checkpoint shard codecs match")
}

/// Wait between attempts to finalize a checkpoint shard.
const FINALIZE_RETRY: Duration = Duration::from_secs(60);

/// Attempts to finalize a checkpoint shard. Twenty minutes outlasts persist's default read lease
/// of fifteen.
const FINALIZE_ATTEMPTS: usize = 20;

/// Deletes checkpoint shards that no restore will read again.
fn spawn_finalize(client: PersistClient, shards: Vec<ShardId>, why: &'static str) {
    if shards.is_empty() {
        return;
    }
    info!(shards = shards.len(), %why, "finalizing arrangement checkpoint shards");
    mz_ore::task::spawn(|| "arrangement-checkpoint-finalize", async move {
        futures::stream::iter(shards)
            .for_each_concurrent(8, |shard| finalize_shard(&client, shard))
            .await;
    });
}

/// Finalizes every shard of a checkpoint key: its manifest and its current generation. Earlier
/// generations were finalized when they were superseded.
fn spawn_finalize_key(client: PersistClient, key: String, peers: usize, why: &'static str) {
    mz_ore::task::spawn(|| "arrangement-checkpoint-finalize-key", async move {
        let (manifest, write) = read_manifest(&client, &key).await;
        write.expire().await;
        let mut shards = manifest.map_or_else(Vec::new, |m| m.shards(&key, peers));
        shards.push(manifest_shard(&key));
        spawn_finalize(client, shards, why);
    });
}

/// Closes a checkpoint shard to writes and reads, then finalizes it.
///
/// A writer of a still-running incarnation fails its next append and stops. A restore that is
/// reading the shard holds its since through a leased reader, which makes finalization fail
/// until the reader expires, so this retries.
async fn finalize_shard(client: &PersistClient, shard: ShardId) {
    let mut write = open_writer(client, shard, "checkpoint finalize").await;
    write.advance_upper(&Antichain::new()).await;
    write.expire().await;
    let mut since = open_since(client, shard, "checkpoint finalize").await;
    let opaque = since_opaque();
    for attempt in 1..=FINALIZE_ATTEMPTS {
        // NOTE: Persist does not recompute a shard's since when a leased reader expires, only
        // when a reader downgrades. Downgrading again, even to the same frontier, picks up
        // expired readers.
        let _ = since
            .compare_and_downgrade_since(&opaque, (&opaque, &Antichain::new()))
            .await;
        match client
            .finalize_shard::<Vec<u8>, Vec<u8>, Timestamp, i64>(
                shard,
                diagnostics("checkpoint finalize"),
            )
            .await
        {
            Ok(()) => return,
            Err(error) if attempt == FINALIZE_ATTEMPTS => {
                warn!(%shard, %error, "giving up finalizing checkpoint shard");
                return;
            }
            Err(_) => tokio::time::sleep(FINALIZE_RETRY).await,
        }
    }
}

/// The persist location of a dataflow's inputs, where its checkpoints live too.
fn persist_location(dataflow: &RenderDataflowDescription<CollectionMetadata>) -> PersistLocation {
    dataflow
        .source_imports
        .values()
        .next()
        .expect("eligible dataflows import a source")
        .desc
        .storage_metadata
        .persist_location
        .clone()
}

/// Decides how a dataflow uses checkpoints, before it is broadcast to all workers.
///
/// Sets `dataflow.checkpoint_restore` when checkpoints are enabled and the dataflow is eligible.
/// Restores when a complete checkpoint of the current generation exists, its frontier is not
/// beyond the dataflow's `as_of`, and every input can still be read from that frontier.
/// Otherwise starts a new generation, whose shards are empty.
pub(crate) async fn plan_checkpoint(
    persist_clients: &PersistClientCache,
    config: &ConfigSet,
    peers: usize,
    dataflow: &mut RenderDataflowDescription<CollectionMetadata>,
) {
    dataflow.checkpoint_restore = None;
    if !ENABLE_ARRANGEMENT_CHECKPOINTS.get(config) || dataflow.is_transient() {
        return;
    }
    let key = match checkpoint_key(dataflow, peers, config) {
        Ok(key) => key,
        Err(reason) => {
            info!(name = %dataflow.debug_name, %reason, "dataflow not eligible for checkpoints");
            return;
        }
    };
    let client = persist_clients
        .open(persist_location(dataflow))
        .await
        .expect("persist location reachable");

    // A dataflow keeps checkpoints for one key: the one it was last created with. A different
    // key means a new build, configuration, or plan, which can never restore the old one.
    let identity = dataflow_identity(dataflow, peers);
    let (previous, mut pointer) = read_record::<String>(&client, pointer_shard(&identity)).await;
    if previous.as_ref() != Some(&key) {
        if write_record(&mut pointer, previous.as_ref(), Some(&key)).await {
            if let Some(stale) = previous {
                spawn_finalize_key(client.clone(), stale, peers, "superseded checkpoint key");
            }
        } else {
            warn!(name = %dataflow.debug_name, "lost race writing checkpoint pointer");
        }
    }
    pointer.expire().await;

    let (manifest, manifest_write) = read_manifest(&client, &key).await;

    if let Some(
        current @ Manifest {
            generation,
            sites: Some(_),
            first_frontier: Some(first_frontier),
            ..
        },
    ) = &manifest
    {
        let started = Instant::now();
        let interval = interval_ms(config);
        match restore_frontier(
            &client,
            dataflow,
            &key,
            current,
            peers,
            *first_frontier,
            interval,
        )
        .await
        {
            Ok(frontier) => {
                info!(
                    name = %dataflow.debug_name,
                    %key,
                    generation,
                    frontier = %frontier,
                    elapsed = ?started.elapsed(),
                    "restoring dataflow from arrangement checkpoint",
                );
                dataflow.checkpoint_restore = Some(CheckpointRestore {
                    key,
                    generation: *generation,
                    frontier: Some(frontier),
                    source: None,
                    joined: false,
                });
                return;
            }
            Err(reason) => {
                info!(name = %dataflow.debug_name, %key, %reason, "not restoring from checkpoint");
            }
        }
    }

    // NOTE: the source is chosen before this generation's manifest is written, so its restore
    // holds are already in place while the new generation starts.
    let source = find_other_peers(&client, config, dataflow, &key, peers).await;
    let interval = interval_ms(config);
    // The first boundary this replica's writers will write, as `attach_writer` computes it.
    let first = match &source {
        Some((_, frontier)) => *frontier,
        None => first_boundary(
            dataflow
                .as_of
                .as_ref()
                .and_then(|a| a.as_option().copied())
                .unwrap_or_else(Timestamp::minimum),
            interval,
        ),
    };
    let max_lag = ARRANGEMENT_CHECKPOINT_MAX_LAG.get(config);
    let (mut manifest, mut manifest_write) = (manifest, manifest_write);
    let mut started = None;
    for _ in 0..3 {
        if let Some(current) = &manifest {
            if in_progress(&client, &key, current, peers, first, max_lag).await {
                started = Some((current.generation, true));
                break;
            }
        }
        let generation = manifest.as_ref().map_or(0, |m| m.generation + 1);
        let new = Manifest {
            generation,
            sites: None,
            first_frontier: None,
            sinks: Vec::new(),
        };
        if write_record(&mut manifest_write, manifest.as_ref(), Some(&new)).await {
            started = Some((generation, false));
            break;
        }
        // Another replica started a generation first. Decide again against it.
        manifest_write.expire().await;
        (manifest, manifest_write) = read_manifest(&client, &key).await;
    }
    manifest_write.expire().await;
    let Some((generation, joined)) = started else {
        warn!(name = %dataflow.debug_name, %key, "lost races starting a checkpoint generation");
        return;
    };
    if joined {
        info!(name = %dataflow.debug_name, %key, generation, "joining checkpoint generation in progress");
    } else if let Some(superseded) = &manifest {
        spawn_finalize(
            client.clone(),
            superseded.shards(&key, peers),
            "superseded checkpoint generation",
        );
    }
    register_peers(&client, config, &key, peers).await;
    let (frontier, source) = match source {
        Some((source, frontier)) => {
            info!(
                name = %dataflow.debug_name,
                %key,
                generation,
                source_peers = source.peers,
                %frontier,
                "starting checkpoint generation seeded from another worker count",
            );
            (Some(frontier), Some(source))
        }
        None => {
            info!(name = %dataflow.debug_name, %key, generation, "starting checkpoint generation");
            (None, None)
        }
    };
    dataflow.checkpoint_restore = Some(CheckpointRestore {
        key,
        generation,
        frontier,
        source,
        joined,
    });
}

/// Whether another replica of the same size is building `manifest`'s generation, so this replica
/// should join it rather than supersede it and finalize the shards it is writing.
///
/// Writers stop once a round or the committed frontier stalls for the maximum lag, so a live
/// generation's latest upper is within that lag of this replica's first boundary. One that has
/// not recorded its sites, or written a round, is still starting.
async fn in_progress(
    client: &PersistClient,
    key: &str,
    manifest: &Manifest,
    peers: usize,
    first: Timestamp,
    max_lag: Duration,
) -> bool {
    if manifest.sites.is_none() {
        return true;
    }
    let mut latest = Timestamp::minimum();
    for shard in manifest.shards(key, peers) {
        let mut write = open_writer(client, shard, "checkpoint probe").await;
        let upper = write.fetch_recent_upper().await.clone();
        write.expire().await;
        match upper.as_option() {
            Some(upper) => latest = std::cmp::max(latest, *upper),
            None => return false,
        }
    }
    let lag = u64::try_from(max_lag.as_millis()).expect("fits");
    latest == Timestamp::minimum() || u64::from(latest).saturating_add(lag) >= u64::from(first)
}

fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

/// Finds a restorable checkpoint of the dataflow written by a replica with another worker count,
/// and the frontier to restore it at.
///
/// Each worker reads `1 / gcd(peers, source peers)` of the checkpoint, so the source with the
/// largest gcd is tried first.
async fn find_other_peers(
    client: &PersistClient,
    config: &ConfigSet,
    dataflow: &RenderDataflowDescription<CollectionMetadata>,
    key: &str,
    peers: usize,
) -> Option<(CheckpointSource, Timestamp)> {
    let (recorded, write) = read_record::<BTreeSet<usize>>(client, peers_shard(key, config)).await;
    write.expire().await;
    let mut candidates: Vec<_> = recorded
        .unwrap_or_default()
        .into_iter()
        .filter(|p| *p != peers)
        .collect();
    candidates.sort_by_key(|p| std::cmp::Reverse(gcd(*p, peers)));
    for source_peers in candidates {
        let source_key = key_for_peers(key, source_peers, config);
        let (manifest, write) = read_manifest(client, &source_key).await;
        write.expire().await;
        let Some(
            manifest @ Manifest {
                generation,
                sites: Some(_),
                first_frontier: Some(first_frontier),
                ..
            },
        ) = &manifest
        else {
            continue;
        };
        match restore_frontier(
            client,
            dataflow,
            &source_key,
            manifest,
            source_peers,
            *first_frontier,
            interval_ms(config),
        )
        .await
        {
            Ok(frontier) => {
                let source = CheckpointSource {
                    key: source_key,
                    generation: *generation,
                    peers: source_peers,
                };
                return Some((source, frontier));
            }
            Err(reason) => {
                info!(name = %dataflow.debug_name, source_peers, %reason, "not restoring from another worker count");
            }
        }
    }
    None
}

/// Adds `peers` to the worker counts with a checkpoint generation of `key`.
async fn register_peers(client: &PersistClient, config: &ConfigSet, key: &str, peers: usize) {
    for _ in 0..5 {
        let (recorded, mut write) =
            read_record::<BTreeSet<usize>>(client, peers_shard(key, config)).await;
        let mut all = recorded.clone().unwrap_or_default();
        let done =
            !all.insert(peers) || write_record(&mut write, recorded.as_ref(), Some(&all)).await;
        write.expire().await;
        if done {
            return;
        }
    }
    warn!(%key, peers, "lost races recording checkpoint worker count");
}

/// A dataflow that was planned with checkpoints, remembered until all its exports are dropped.
struct Planned {
    key: String,
    identity: String,
    location: PersistLocation,
}

/// Plans checkpoint use for commands on their way to the command channel.
pub(crate) struct CommandPlanner {
    persist_clients: Arc<PersistClientCache>,
    peers: usize,
    /// Mirrors the workers' `worker_config`, which only changes through `UpdateConfiguration`.
    config: ConfigSet,
    /// Export ID to the checkpoint of its dataflow.
    planned: Mutex<BTreeMap<GlobalId, Arc<Planned>>>,
}

impl CommandPlanner {
    pub(crate) fn new(persist_clients: Arc<PersistClientCache>, peers: usize) -> Self {
        Self {
            persist_clients,
            peers,
            config: mz_dyncfgs::all_dyncfgs(),
            planned: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) async fn plan(&self, cmd: ComputeCommand) -> ComputeCommand {
        match cmd {
            ComputeCommand::UpdateConfiguration(params) => {
                params.dyncfg_updates.apply(&self.config);
                ComputeCommand::UpdateConfiguration(params)
            }
            ComputeCommand::CreateDataflow(mut dataflow) => {
                plan_checkpoint(
                    &self.persist_clients,
                    &self.config,
                    self.peers,
                    &mut dataflow,
                )
                .await;
                if let Some(plan) = &dataflow.checkpoint_restore {
                    let planned = Arc::new(Planned {
                        key: plan.key.clone(),
                        identity: dataflow_identity(&dataflow, self.peers),
                        location: persist_location(&dataflow),
                    });
                    let mut map = self.planned.lock().expect("lock poisoned");
                    for id in dataflow.export_ids() {
                        map.insert(id, Arc::clone(&planned));
                    }
                }
                ComputeCommand::CreateDataflow(dataflow)
            }
            ComputeCommand::AllowCompaction { id, frontier } => {
                if frontier.is_empty() {
                    self.dropped(id).await;
                }
                ComputeCommand::AllowCompaction { id, frontier }
            }
            cmd => cmd,
        }
    }

    /// Finalizes a dataflow's checkpoints once the controller has dropped all its exports.
    ///
    /// Exports dropped while this replica was disconnected never reach it, so their checkpoints
    /// are not finalized here.
    async fn dropped(&self, id: GlobalId) {
        let finished = {
            let mut map = self.planned.lock().expect("lock poisoned");
            map.remove(&id)
                .filter(|planned| !map.values().any(|other| Arc::ptr_eq(other, planned)))
        };
        let Some(planned) = finished else {
            return;
        };
        let client = self
            .persist_clients
            .open(planned.location.clone())
            .await
            .expect("persist location reachable");
        spawn_finalize_key(
            client.clone(),
            planned.key.clone(),
            self.peers,
            "dataflow dropped",
        );
        spawn_finalize(
            client,
            vec![pointer_shard(&planned.identity)],
            "dataflow dropped",
        );
    }
}

/// The committed frontier of a complete checkpoint, if the dataflow can restore from it.
///
/// Opens leased readers that hold every shard the restore reads, so nothing compacts past the
/// frontier before the dataflow's own readers have registered.
async fn restore_frontier(
    client: &PersistClient,
    dataflow: &RenderDataflowDescription<CollectionMetadata>,
    key: &str,
    manifest: &Manifest,
    peers: usize,
    first_frontier: Timestamp,
    interval_ms: u64,
) -> Result<Timestamp, String> {
    let shards = manifest.shards(key, peers);
    let mut frontier = Timestamp::MAX;
    for &shard in &shards {
        let mut write = open_writer(client, shard, "probe").await;
        let upper = write.fetch_recent_upper().await;
        let upper = *upper.as_option().ok_or("checkpoint shard closed")?;
        frontier = std::cmp::min(frontier, upper);
    }
    let dataflow_as_of = dataflow
        .as_of
        .as_ref()
        .and_then(|a| a.as_option().copied())
        .ok_or("dataflow has no as_of")?;
    // Restored state reflects the collection only at `frontier - 1`, so restoring past the
    // `as_of` would answer reads in between wrongly. Every interval boundary from the first
    // checkpoint to the committed one is a valid frontier, so take the latest that is not.
    let latest_allowed =
        Timestamp::new((u64::from(dataflow_as_of) + 1) / interval_ms * interval_ms);
    let frontier = std::cmp::min(frontier, latest_allowed);
    if frontier < first_frontier {
        return Err(format!(
            "no checkpoint frontier in [{first_frontier}, {latest_allowed}], committed {frontier}"
        ));
    }
    let as_of = frontier.step_back().ok_or("no checkpoint written yet")?;

    let mut site_holds = Vec::new();
    for shard in shards {
        let read = open_reader(client, shard, "checkpoint restore probe").await;
        if !read.since().less_equal(&as_of) {
            return Err(format!(
                "checkpoint shard {shard} since {:?} is beyond {as_of}",
                read.since()
            ));
        }
        site_holds.push(read);
    }
    let mut holds = Vec::new();
    for import in dataflow.source_imports.values() {
        let meta = &import.desc.storage_metadata;
        let read: InputReadHandle = client
            .open_leased_reader(
                meta.data_shard,
                Arc::new(meta.relation_desc.clone()),
                Arc::new(UnitSchema),
                diagnostics("checkpoint restore probe"),
                false,
            )
            .await
            .map_err(|e| format!("open input: {e}"))?;
        if !read.since().less_equal(&as_of) {
            return Err(format!(
                "input {} since {:?} is beyond checkpoint as_of {as_of}",
                meta.data_shard,
                read.since()
            ));
        }
        holds.push(read);
    }
    mz_ore::task::spawn(|| "checkpoint-restore-holds", async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        for read in holds {
            read.expire().await;
        }
        for read in site_holds {
            read.expire().await;
        }
    });
    Ok(frontier)
}

fn interval_ms(config: &ConfigSet) -> u64 {
    u64::try_from(ARRANGEMENT_CHECKPOINT_INTERVAL.get(config).as_millis())
        .expect("interval fits")
        .max(1)
}

/// The first checkpoint frontier after `as_of`: the smallest multiple of the interval beyond it.
fn first_boundary(as_of: Timestamp, interval_ms: u64) -> Timestamp {
    Timestamp::new((u64::from(as_of) / interval_ms + 1) * interval_ms)
}

/// Converts arrangement keys and values to and from their checkpoint encoding.
///
/// The encodings are in-memory representations with no stability guarantee. That is sound only
/// because the checkpoint key includes the build identity.
///
/// Encoded bytes should sort like `Ord`. Persist returns a snapshot sorted by encoded bytes, so
/// restore can then build batches without sorting. [`restore_span`] checks the order and falls
/// back to a batcher if it does not hold.
trait CheckpointBytes: Sized {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Self;
}

impl CheckpointBytes for Row {
    fn to_bytes(&self) -> Vec<u8> {
        // `RowRef` orders by length, then bytes. A big-endian length prefix makes the encoded
        // bytes sort the same way.
        let data = self.data();
        let len = u32::try_from(data.len()).expect("row length fits in u32");
        let mut bytes = Vec::with_capacity(4 + data.len());
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(data);
        bytes
    }
    fn from_bytes(bytes: Vec<u8>) -> Self {
        // SAFETY: The bytes were produced by `to_bytes` in a binary with the same build identity.
        unsafe { Row::from_bytes_unchecked(&bytes[4..]) }
    }
}

impl CheckpointBytes for () {
    fn to_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
    fn from_bytes(_: Vec<u8>) -> Self {}
}

impl CheckpointBytes for DataflowErrorSer {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
    fn from_bytes(bytes: Vec<u8>) -> Self {
        DataflowErrorSer::from_bytes(bytes)
    }
}

/// Type-erased encode and restore functions for one trace type.
#[derive(Clone, Copy)]
struct SiteCodec {
    /// Starts encoding a round. See [`Encoder`].
    encoder: fn(Box<dyn Any>, Timestamp, Timestamp, u64) -> Box<dyn RoundEncoder>,
    /// Reads a worker's partition at `frontier - 1` into a `Span<Timestamp, Tr::Batch>` spanning
    /// `[minimum, frontier)`. See [`restore_span`].
    restore:
        fn(&PersistClient, &Partition, Timestamp) -> Result<(Box<dyn Any>, RestoreCounts), String>,
}

/// The source shards of one worker's restored partition of an arrangement.
struct Partition {
    /// `(shard, source worker)` pairs.
    shards: Vec<(ShardId, usize)>,
    source_peers: usize,
    worker: usize,
    peers: usize,
}

#[derive(Debug, Default)]
struct RestoreCounts {
    read: usize,
    kept: usize,
    /// Keys whose hash does not route to the worker that wrote them. Restoring onto a different
    /// worker count is correct only while this is zero.
    misrouted: usize,
}

/// The codec for trace type `Tr`, if checkpoints support it.
fn codec_for<Tr: 'static>() -> Option<SiteCodec> {
    let id = TypeId::of::<Tr>();
    if id == TypeId::of::<RowRowSpine<Timestamp, Diff>>() {
        Some(SiteCodec {
            encoder: Encoder::<RowRowSpine<Timestamp, Diff>, Row, Row>::start,
            restore: restore_span::<
                RowRowSpine<Timestamp, Diff>,
                RowRowBatcher<Timestamp, Diff>,
                RowRowBuilder<Timestamp, Diff>,
                Row,
                Row,
            >,
        })
    } else if id == TypeId::of::<ErrSpine<Timestamp, Diff>>() {
        Some(SiteCodec {
            encoder: Encoder::<ErrSpine<Timestamp, Diff>, DataflowErrorSer, ()>::start,
            restore: restore_span::<
                ErrSpine<Timestamp, Diff>,
                ErrBatcher<Timestamp, Diff>,
                ErrBuilder<Timestamp, Diff>,
                DataflowErrorSer,
                (),
            >,
        })
    } else if id == TypeId::of::<RowErrSpine<Timestamp, Diff>>() {
        Some(SiteCodec {
            encoder: Encoder::<RowErrSpine<Timestamp, Diff>, Row, DataflowErrorSer>::start,
            restore: restore_span::<
                RowErrSpine<Timestamp, Diff>,
                RowErrBatcher<Timestamp, Diff>,
                RowErrBuilder<Timestamp, Diff>,
                Row,
                DataflowErrorSer,
            >,
        })
    } else {
        None
    }
}

/// Encodes one round of a checkpoint writer incrementally.
trait RoundEncoder {
    /// Appends encoded updates to `out` until it reaches `max_bytes` or `fuel` runs out.
    /// Returns whether every update of the round has been encoded.
    fn fill(&mut self, out: &mut Chunk, max_bytes: usize, fuel: &mut isize) -> bool;
}

/// Encoded updates on their way to a checkpoint shard.
#[derive(Default)]
struct Chunk {
    updates: Vec<Update>,
    bytes: usize,
}

/// Encodes the updates of a round `[lower, upper)` from the arrangement's own batches.
///
/// The batches are shared with the trace, so encoding copies no more than one chunk at a time.
/// A merged cursor visits each `(key, val)` once across all batches, so updates that cancel
/// within an interval are never written.
///
/// Each time is rounded down to a multiple of the interval. Every interval boundary inside a
/// round is then still a valid restore frontier, so one round may cover many intervals.
struct Encoder<Tr: TraceReader, K, V>
where
    Tr::Batch: Navigable,
{
    cursor: CursorList<BatchCursor<Tr>>,
    storage: Vec<Tr::Batch>,
    lower: Timestamp,
    upper: Timestamp,
    interval_ms: u64,
    times: Vec<(Timestamp, i64)>,
    _types: std::marker::PhantomData<(K, V)>,
}

impl<Tr, K, V> Encoder<Tr, K, V>
where
    Tr: TraceReader<Time = Timestamp> + 'static,
    Tr::Batch: Navigable + 'static,
    BatchCursor<Tr>: Cursor<Time = Timestamp, Diff = Diff, ValOwn = V, Storage = Tr::Batch>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: CheckpointBytes + 'static,
    V: CheckpointBytes + 'static,
{
    /// `batches` is a `Vec<Tr::Batch>`.
    fn start(
        batches: Box<dyn Any>,
        lower: Timestamp,
        upper: Timestamp,
        interval_ms: u64,
    ) -> Box<dyn RoundEncoder> {
        let batches = *batches
            .downcast::<Vec<Tr::Batch>>()
            .expect("checkpoint codec matches the trace type");
        let (cursor, storage) = cursor_list(batches);
        Box::new(Self {
            cursor,
            storage,
            lower,
            upper,
            interval_ms,
            times: Vec::new(),
            _types: std::marker::PhantomData,
        })
    }
}

impl<Tr, K, V> RoundEncoder for Encoder<Tr, K, V>
where
    Tr: TraceReader<Time = Timestamp> + 'static,
    Tr::Batch: Navigable + 'static,
    BatchCursor<Tr>: Cursor<Time = Timestamp, Diff = Diff, ValOwn = V, Storage = Tr::Batch>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    K: CheckpointBytes + 'static,
    V: CheckpointBytes + 'static,
{
    fn fill(&mut self, out: &mut Chunk, max_bytes: usize, fuel: &mut isize) -> bool {
        type C<Tr> = CursorList<BatchCursor<Tr>>;
        let Self {
            cursor,
            storage,
            lower,
            upper,
            interval_ms,
            times,
            _types,
        } = self;
        while cursor.key_valid(storage) {
            let key = <<C<Tr> as Cursor>::KeyContainer as BatchContainer>::into_owned(
                cursor.key(storage),
            )
            .to_bytes();
            while cursor.val_valid(storage) {
                times.clear();
                cursor.map_times(storage, |time, diff| {
                    let time = <C<Tr> as Cursor>::owned_time(time);
                    if *lower <= time && time < *upper {
                        let rounded = Timestamp::new(u64::from(time) / *interval_ms * *interval_ms);
                        times.push((
                            std::cmp::max(rounded, *lower),
                            <C<Tr> as Cursor>::owned_diff(diff).into_inner(),
                        ));
                    }
                });
                *fuel -= isize::try_from(times.len()).expect("fits") + 1;
                consolidate(times);
                if !times.is_empty() {
                    let val = <C<Tr> as Cursor>::owned_val(cursor.val(storage)).to_bytes();
                    for (time, diff) in times.drain(..) {
                        out.bytes += key.len() + val.len() + 16;
                        out.updates.push(((key.clone(), val.clone()), time, diff));
                    }
                }
                cursor.step_val(storage);
                if out.bytes >= max_bytes || *fuel <= 0 {
                    return false;
                }
            }
            cursor.step_key(storage);
        }
        true
    }
}

/// Reads a worker's partition of a checkpoint at `frontier - 1` into a span covering
/// `[minimum, frontier)`.
///
/// With a different worker count than the checkpoint's, the partition reads every source shard
/// that can hold this worker's keys and keeps the keys that route here. Each source shard holds
/// one source worker's keys, so this needs no exchange, at the cost of reading up to
/// `source_peers / gcd` shards per worker.
///
/// The encoding sorts like the trace (see [`CheckpointBytes`]), so updates arrive in the order the
/// trace's builder takes them, and the batch is built directly in two passes over the partition
/// (see [`StreamSealer`]). Peak memory is the finished batch plus one persist part per source
/// shard. If the first pass finds the updates out of order, this falls back to a batcher, which
/// holds a sorted copy of the partition beside the batch it builds.
fn restore_span<Tr, Ba, Bu, K, V>(
    client: &PersistClient,
    partition: &Partition,
    frontier: Timestamp,
) -> Result<(Box<dyn Any>, RestoreCounts), String>
where
    Tr: TraceReader<Time = Timestamp> + 'static,
    Ba: Batcher<Vec<((K, V), Timestamp, Diff)>, Time = Timestamp> + BatcherNew,
    Ba::Output: Into<Tr::Batch>,
    Bu: StreamSealer<Input = ColumnationStack<((K, V), Timestamp, Diff)>, Time = Timestamp>,
    Bu::Output: Into<Tr::Batch>,
    K: CheckpointBytes + std::hash::Hash + Ord + Columnation,
    V: CheckpointBytes + Ord + Columnation,
{
    let as_of = frontier.step_back().ok_or("restore frontier is positive")?;
    let built = futures::executor::block_on(async {
        let mut stats = Bu::Stats::default();
        let mut chunk = ColumnationStack::default();
        let mut prev: Option<(K, V)> = None;
        let mut sorted = true;
        let mut first = RestoreCounts::default();
        for_each_update(client, partition, as_of, &mut first, |kv: (K, V), diff| {
            if prev.as_ref().is_some_and(|prev| *prev >= kv) {
                sorted = false;
                return false;
            }
            let update = (kv, as_of, diff);
            chunk.copy(&update);
            prev = Some(update.0);
            if chunk.len() == RESTORE_CHUNK {
                Bu::observe(&mut stats, &chunk);
                chunk.clear();
            }
            true
        })
        .await?;
        if !sorted {
            return Ok::<_, String>(None);
        }
        if !chunk.is_empty() {
            Bu::observe(&mut stats, &chunk);
            chunk.clear();
        }

        let mut builder = Bu::with_stats(stats);
        let mut counts = RestoreCounts::default();
        for_each_update(client, partition, as_of, &mut counts, |kv, diff| {
            chunk.copy(&(kv, as_of, diff));
            if chunk.len() == RESTORE_CHUNK {
                builder.push(&mut chunk);
                chunk.clear();
            }
            true
        })
        .await?;
        if !chunk.is_empty() {
            builder.push(&mut chunk);
        }
        // Both passes read the same snapshot, so they see the same updates in the same order.
        assert_eq!(first.kept, counts.kept, "checkpoint restore passes agree");
        Ok(Some((builder.done().map(Into::into), counts)))
    })?;
    let (batch, counts) = match built {
        Some(built) => built,
        None => {
            warn!(
                worker = partition.worker,
                "checkpoint updates are not in trace order, restoring through a batcher"
            );
            restore_batched::<Tr, Ba, K, V>(client, partition, as_of)?
        }
    };
    let desc = Description::new(
        Antichain::from_elem(Timestamp::minimum()),
        Antichain::from_elem(frontier),
        Antichain::from_elem(Timestamp::minimum()),
    );
    Ok((
        Box::new(Span::<Timestamp, Tr::Batch>::new(desc, batch)),
        counts,
    ))
}

/// Updates per chunk handed to a batcher or builder during restore.
const RESTORE_CHUNK: usize = 8192;

/// Prefetch budget of each source shard's cursor during restore. With persist's default, the
/// 1 GiB compaction memory bound, a cursor fetches a whole checkpoint shard ahead of the builder.
/// Restoring a 393 MiB arrangement peaked at about 1,200 MiB that way, and at about 800 MiB with
/// this budget.
const RESTORE_PREFETCH_BYTES: usize = 16 << 20;

/// Builds a worker's partition through a batcher, which sorts and consolidates in any input order.
fn restore_batched<Tr, Ba, K, V>(
    client: &PersistClient,
    partition: &Partition,
    as_of: Timestamp,
) -> Result<(Option<Tr::Batch>, RestoreCounts), String>
where
    Tr: TraceReader<Time = Timestamp> + 'static,
    Ba: Batcher<Vec<((K, V), Timestamp, Diff)>, Time = Timestamp> + BatcherNew,
    Ba::Output: Into<Tr::Batch>,
    K: CheckpointBytes + std::hash::Hash + Ord,
    V: CheckpointBytes + Ord,
{
    let mut batcher = Ba::new_batcher(None, 0);
    let mut counts = RestoreCounts::default();
    let mut typed = Vec::with_capacity(RESTORE_CHUNK);
    futures::executor::block_on(for_each_update(
        client,
        partition,
        as_of,
        &mut counts,
        |kv, diff| {
            typed.push((kv, as_of, diff));
            if typed.len() == RESTORE_CHUNK {
                batcher.insert(&mut typed);
                typed.clear();
            }
            true
        },
    ))?;
    batcher.insert(&mut typed);
    let upper = Antichain::from_elem(as_of.step_forward());
    let (batch, _) = batcher.extract(upper.borrow());
    Ok((batch.map(Into::into), counts))
}

/// One source shard of a partition being read in order.
struct RestoreSource<K, V> {
    read: CkptReadHandle,
    cursor: PersistCursor<Vec<u8>, Vec<u8>, Timestamp, i64>,
    source_worker: usize,
    buffer: VecDeque<((K, V), Diff)>,
    done: bool,
}

impl<K: CheckpointBytes + std::hash::Hash, V: CheckpointBytes> RestoreSource<K, V> {
    /// Refills the buffer from the next consolidated batch, keeping the keys that route to this
    /// worker.
    async fn fill(&mut self, partition: &Partition, counts: &mut RestoreCounts) {
        let same_peers = partition.source_peers == partition.peers;
        while self.buffer.is_empty() && !self.done {
            let Some(updates) = self.cursor.next().await else {
                self.done = true;
                break;
            };
            for ((k, v), _, d) in updates {
                if d == 0 {
                    continue;
                }
                counts.read += 1;
                let key = K::from_bytes(k);
                let hash = key.hashed();
                if route(hash, partition.source_peers) != self.source_worker {
                    counts.misrouted += 1;
                }
                if !same_peers && route(hash, partition.peers) != partition.worker {
                    continue;
                }
                counts.kept += 1;
                self.buffer
                    .push_back(((key, V::from_bytes(v)), Diff::from(d)));
            }
        }
    }
}

/// Feeds the updates of `partition` at `as_of` to `f` until it returns false.
///
/// Persist returns each shard consolidated and sorted by encoded bytes. Different source shards
/// hold disjoint keys, so merging their heads by `(K, V)` yields one consolidated sequence, sorted
/// by `(K, V)` whenever the encoding sorts like `(K, V)`.
async fn for_each_update<K, V>(
    client: &PersistClient,
    partition: &Partition,
    as_of: Timestamp,
    counts: &mut RestoreCounts,
    mut f: impl FnMut((K, V), Diff) -> bool,
) -> Result<(), String>
where
    K: CheckpointBytes + std::hash::Hash + Ord,
    V: CheckpointBytes + Ord,
{
    let mut sources = Vec::new();
    for &(shard, source_worker) in &partition.shards {
        let mut read = open_reader(client, shard, "checkpoint restore").await;
        let cursor = read
            .snapshot_cursor_with_budget(
                Antichain::from_elem(as_of),
                |_| true,
                RESTORE_PREFETCH_BYTES,
            )
            .await
            .map_err(|since| {
                format!("checkpoint shard {shard} since {since:?} is beyond {as_of}")
            })?;
        let mut source = RestoreSource {
            read,
            cursor,
            source_worker,
            buffer: VecDeque::new(),
            done: false,
        };
        source.fill(partition, counts).await;
        sources.push(source);
    }
    loop {
        let next = sources
            .iter()
            .enumerate()
            .filter_map(|(i, source)| source.buffer.front().map(|head| (i, &head.0)))
            .min_by(|(_, a), (_, b)| a.cmp(b))
            .map(|(i, _)| i);
        let Some(i) = next else { break };
        let (kv, diff) = sources[i].buffer.pop_front().expect("head exists");
        if sources[i].buffer.is_empty() {
            sources[i].fill(partition, counts).await;
        }
        if !f(kv, diff) {
            break;
        }
    }
    for RestoreSource { read, cursor, .. } in sources {
        drop(cursor);
        read.expire().await;
    }
    Ok(())
}

/// Shared between the render context, the writers it creates, and [`RenderGuard::finish`].
struct Shared {
    /// `None` until rendering finishes, then whether writers may write.
    enabled: Cell<Option<bool>>,
    /// The committer task on worker 0, aborted when the dataflow's operators are dropped.
    committer: RefCell<Option<AbortOnDropHandle<()>>>,
}

/// The render-time state of one dataflow on one worker.
struct RenderContext {
    key: String,
    generation: u64,
    worker: usize,
    peers: usize,
    client: PersistClient,
    restore: Option<Timestamp>,
    /// The generation restored from: this one, or one of a replica with another worker count.
    source: CheckpointSource,
    /// Whether this generation is new and seeded with state restored from another worker count.
    /// Its writers then write the restored contents as their first round.
    seeded: bool,
    /// Whether this generation may already have rounds, so writers start from the shard uppers.
    joined: bool,
    interval_ms: u64,
    max_lag: Duration,
    dataflow_as_of: Timestamp,
    /// Inputs, whose history the committer holds.
    held_shards: Vec<(ShardId, mz_repr::RelationDesc)>,
    site_types: Vec<&'static str>,
    sinks: Vec<GlobalId>,
    poisoned: Option<&'static str>,
    shared: Rc<Shared>,
    /// The manifest this generation must complete, read back by worker 0 at the end of rendering.
    restore_types: Option<Vec<String>>,
}

thread_local! {
    static CONTEXT: RefCell<Option<RenderContext>> = const { RefCell::new(None) };
}

/// Installs the render context for one `build_compute_dataflow` call.
pub(crate) struct RenderGuard {
    restoring: Option<Timestamp>,
}

impl RenderGuard {
    /// Starts rendering `dataflow` with checkpoints, if the sequencing worker chose to.
    ///
    /// Returns the frontier to restore at, if restoring. The caller must then read inputs from
    /// `frontier - 1` with their snapshots excluded.
    pub(crate) fn begin(
        dataflow: &RenderDataflowDescription<CollectionMetadata>,
        worker: usize,
        peers: usize,
        config: &ConfigSet,
        persist_clients: &PersistClientCache,
    ) -> Self {
        let none = Self { restoring: None };
        let Some(plan) = &dataflow.checkpoint_restore else {
            return none;
        };
        // Configuration is applied in command order on every worker, and the plan's checkpoint
        // decision is broadcast, so every worker reaches the same conclusion here. A conclusion
        // that differed between workers would render different operators on each.
        let environment = environment_digest(peers, config);
        let source = plan.source.clone().unwrap_or_else(|| CheckpointSource {
            key: plan.key.clone(),
            generation: plan.generation,
            peers,
        });
        if !plan.key.starts_with(&environment)
            || !source
                .key
                .starts_with(&environment_digest(source.peers, config))
        {
            warn!(name = %dataflow.debug_name, "checkpoint environment mismatch, not checkpointing");
            return none;
        }
        let meta = &dataflow
            .source_imports
            .values()
            .next()
            .expect("eligible")
            .desc
            .storage_metadata;
        let client =
            futures::executor::block_on(persist_clients.open(meta.persist_location.clone()))
                .expect("persist location reachable");
        let restore_types = match plan.frontier {
            Some(_) => {
                let (manifest, _) =
                    futures::executor::block_on(read_manifest(&client, &source.key));
                manifest.and_then(|m| m.sites)
            }
            None => None,
        };
        let context = RenderContext {
            key: plan.key.clone(),
            generation: plan.generation,
            worker,
            peers,
            client,
            restore: plan.frontier,
            seeded: plan.frontier.is_some() && plan.source.is_some(),
            joined: plan.joined,
            source,
            interval_ms: interval_ms(config),
            max_lag: ARRANGEMENT_CHECKPOINT_MAX_LAG.get(config),
            // Writers may only checkpoint times at which the arrangements reflect the
            // collection. When restoring, that starts at the restored frontier.
            dataflow_as_of: match plan.frontier {
                Some(frontier) => frontier.step_back().expect("restore frontier is positive"),
                None => dataflow
                    .as_of
                    .as_ref()
                    .and_then(|a| a.as_option().copied())
                    .unwrap_or_else(Timestamp::minimum),
            },
            held_shards: dataflow
                .source_imports
                .values()
                .map(|i| &i.desc.storage_metadata)
                .map(|meta| (meta.data_shard, meta.relation_desc.clone()))
                .collect(),
            site_types: Vec::new(),
            sinks: Vec::new(),
            poisoned: None,
            shared: Rc::new(Shared {
                enabled: Cell::new(None),
                committer: RefCell::new(None),
            }),
            restore_types,
        };
        CONTEXT.with(|c| *c.borrow_mut() = Some(context));
        Self {
            restoring: plan.frontier,
        }
    }

    /// The frontier being restored at, if any.
    pub(crate) fn restoring(&self) -> Option<Timestamp> {
        self.restoring
    }

    /// Ends rendering. Enables the writers, and records the site list for a new generation.
    pub(crate) fn finish(self, name: &str) {
        let Some(context) = CONTEXT.with(|c| c.borrow_mut().take()) else {
            return;
        };
        if let Some(reason) = context.poisoned {
            assert!(
                context.restore.is_none(),
                "restored dataflow rendered an unsupported arrangement: {reason}"
            );
            info!(%name, %reason, "dataflow has an unsupported arrangement, not checkpointing");
            context.shared.enabled.set(Some(false));
            return;
        }
        let sites = context.site_types.len();
        if let Some(types) = &context.restore_types {
            assert_eq!(
                types.len(),
                sites,
                "restored dataflow rendered a different number of arrangements"
            );
        }
        context.shared.enabled.set(Some(true));
        let manifest = Manifest {
            generation: context.generation,
            sites: Some(context.site_types.iter().map(|s| s.to_string()).collect()),
            first_frontier: Some(first_boundary(context.dataflow_as_of, context.interval_ms)),
            sinks: context.sinks.clone(),
        };
        if context.worker != 0 {
            return;
        }
        let committer = mz_ore::task::spawn(
            || "arrangement-checkpoint-committer",
            run_committer(
                context.client.clone(),
                CommitSpec {
                    shards: manifest.shards(&context.key, context.peers),
                    held: context.held_shards.clone(),
                    interval_ms: context.interval_ms,
                    max_lag: context.max_lag,
                },
            ),
        );
        *context.shared.committer.borrow_mut() = Some(committer.abort_on_drop());
        if context.restore.is_none() || context.seeded {
            let key = context.key.clone();
            let client = context.client.clone();
            let recorded = futures::executor::block_on(record_manifest(&client, &key, manifest));
            info!(%name, sites, recorded, "recorded checkpoint manifest");
        }
    }
}

/// Records the sites of a generation this replica started or joined. Returns whether the manifest
/// now describes them.
///
/// Every replica of a generation records the same sites. Their first frontiers can differ, because
/// each starts from its own `as_of`, and the recorded one must be the latest: a boundary before a
/// writer's first frontier may never have been written with complete contents.
async fn record_manifest(client: &PersistClient, key: &str, mine: Manifest) -> bool {
    for _ in 0..5 {
        let (current, mut write) = read_manifest(client, key).await;
        let merged = match &current {
            Some(current) if current.generation == mine.generation => match &current.sites {
                None => mine.clone(),
                Some(sites) => {
                    if Some(sites) != mine.sites.as_ref() || current.sinks != mine.sinks {
                        warn!(%key, "checkpoint generation recorded different sites");
                        write.expire().await;
                        return false;
                    }
                    if current.first_frontier >= mine.first_frontier {
                        write.expire().await;
                        return true;
                    }
                    Manifest {
                        first_frontier: mine.first_frontier,
                        ..current.clone()
                    }
                }
            },
            // Superseded by a newer generation, or the key was finalized.
            _ => {
                write.expire().await;
                return false;
            }
        };
        let won = write_record(&mut write, current.as_ref(), Some(&merged)).await;
        write.expire().await;
        if won {
            return true;
        }
    }
    false
}

/// Whether the dataflow being rendered on this worker restores from a checkpoint.
pub(crate) fn restoring() -> bool {
    CONTEXT.with(|c| c.borrow().as_ref().is_some_and(|c| c.restore.is_some()))
}

/// A checkpointed arrangement site, handed to the operator that builds the arrangement.
pub(crate) struct Site {
    /// The restored contents, a `Span<Timestamp, Tr::Batch>` spanning `[minimum, F)`.
    pub(crate) restored: Option<Box<dyn Any>>,
    writer: WriterSpec,
}

struct WriterSpec {
    site: usize,
    codec: SiteCodec,
    client: PersistClient,
    shard: ShardId,
    /// The shard's upper when the dataflow was rendered. Updates before it are already durable.
    initial_upper: Timestamp,
    dataflow_as_of: Timestamp,
    interval_ms: u64,
    max_lag: Duration,
    shared: Rc<Shared>,
}

/// What the committer of a dataflow tracks and holds. It runs on worker 0 only.
struct CommitSpec {
    /// Every site and sink shard. The committed frontier is their minimum upper.
    shards: Vec<ShardId>,
    held: Vec<(ShardId, mz_repr::RelationDesc)>,
    interval_ms: u64,
    max_lag: Duration,
}

/// Registers the next arrangement site of the dataflow being rendered.
///
/// Returns `None` when the dataflow is not checkpointed. A trace type without a codec poisons the
/// dataflow: it renders normally and never writes a complete checkpoint.
pub(crate) fn next_site<Tr: 'static>() -> Option<Site> {
    CONTEXT.with(|c| {
        let mut borrow = c.borrow_mut();
        let context = borrow.as_mut()?;
        let site = context.site_types.len();
        let type_name = std::any::type_name::<Tr>();
        context.site_types.push(type_name);
        if context.poisoned.is_some() {
            return None;
        }
        let Some(codec) = codec_for::<Tr>() else {
            context.poisoned = Some(type_name);
            return None;
        };
        if let Some(types) = &context.restore_types {
            assert_eq!(
                types.get(site).map(String::as_str),
                Some(type_name),
                "restored dataflow rendered a different arrangement at site {site}"
            );
        }
        let shard = site_shard(&context.key, context.generation, site, context.worker);
        let client = context.client.clone();
        let (restored, initial_upper) = match context.restore {
            Some(frontier) => {
                let started = Instant::now();
                let upper = initial_upper(context, &client, shard);
                let source = &context.source;
                let partition = Partition {
                    shards: source_workers(context.worker, context.peers, source.peers)
                        .into_iter()
                        .map(|j| (site_shard(&source.key, source.generation, site, j), j))
                        .collect(),
                    source_peers: source.peers,
                    worker: context.worker,
                    peers: context.peers,
                };
                let (span, counts) = (codec.restore)(&client, &partition, frontier)
                    .expect("checkpoint shard readable at the committed frontier");
                info!(
                    site,
                    worker = context.worker,
                    source_peers = source.peers,
                    source_shards = partition.shards.len(),
                    read = counts.read,
                    updates = counts.kept,
                    elapsed = ?started.elapsed(),
                    "restored arrangement from checkpoint"
                );
                if counts.misrouted > 0 {
                    warn!(
                        site,
                        worker = context.worker,
                        misrouted = counts.misrouted,
                        "checkpoint keys do not route to the worker that wrote them"
                    );
                }
                (Some(span), upper)
            }
            None => (None, initial_upper(context, &client, shard)),
        };
        Some(Site {
            restored,
            writer: WriterSpec {
                site,
                codec,
                client,
                shard,
                initial_upper,
                dataflow_as_of: context.dataflow_as_of,
                interval_ms: context.interval_ms,
                max_lag: context.max_lag,
                shared: Rc::clone(&context.shared),
            },
        })
    })
}

/// The upper of a writer's shard when the dataflow renders. Updates before it are durable.
///
/// A new generation's shards are empty, unless another replica of the same size started it.
fn initial_upper(context: &RenderContext, client: &PersistClient, shard: ShardId) -> Timestamp {
    let fresh = context.restore.is_none() || context.seeded;
    if fresh && !context.joined {
        return Timestamp::minimum();
    }
    futures::executor::block_on(async {
        let mut write = open_writer(client, shard, "checkpoint writer start").await;
        let upper = write.fetch_recent_upper().await.clone();
        write.expire().await;
        // A finalized shard ends the writer at its first append.
        upper.as_option().copied().unwrap_or(Timestamp::MAX)
    })
}

/// A checkpointed MV sink, handed to the sink when it renders.
pub(crate) struct SinkSite {
    restore: Option<Timestamp>,
    /// The source shards this worker restores the snapshot from.
    restore_shards: Vec<ShardId>,
    /// See [`RenderContext::seeded`].
    seeded: bool,
    client: PersistClient,
    shard: ShardId,
    /// The shard's upper when the dataflow was rendered. Updates before it are already durable.
    initial_upper: Timestamp,
    dataflow_as_of: Timestamp,
    interval_ms: u64,
    max_lag: Duration,
    shared: Rc<Shared>,
}

/// Registers an MV sink of the dataflow being rendered.
pub(crate) fn next_sink_site(sink: GlobalId) -> Option<SinkSite> {
    CONTEXT.with(|c| {
        let mut borrow = c.borrow_mut();
        let context = borrow.as_mut()?;
        context.sinks.push(sink);
        let shard = sink_shard(&context.key, context.generation, sink, context.worker);
        let client = context.client.clone();
        let initial_upper = initial_upper(context, &client, shard);
        // The sink exchanges its desired collection by row, so a snapshot split across workers
        // in any way is correct.
        let source = &context.source;
        let restore_shards = (0..source.peers)
            .filter(|j| j % context.peers == context.worker)
            .map(|j| sink_shard(&source.key, source.generation, sink, j))
            .collect();
        Some(SinkSite {
            restore: context.restore,
            restore_shards,
            seeded: context.seeded,
            client,
            shard,
            initial_upper,
            dataflow_as_of: context.dataflow_as_of,
            interval_ms: context.interval_ms,
            max_lag: context.max_lag,
            shared: Rc::clone(&context.shared),
        })
    })
}

/// The desired collection of an MV sink, as the sink consumes it.
pub(crate) type SinkStreams<'s> = (
    StreamVec<'s, Timestamp, (Row, Timestamp, Diff)>,
    StreamVec<'s, Timestamp, (DataflowErrorSer, Timestamp, Diff)>,
);

/// Checkpoints an MV sink's desired collection, and when restoring, prepends its restored
/// snapshot.
///
/// A restored dataflow emits only the changes from the restore frontier `F` on. The sink compares
/// its desired collection against the full contents of its output shard, so it needs the
/// desired snapshot at `F - 1` too, or it would retract everything. That snapshot comes from the
/// generation's own shards, so the sink trusts nothing another release or replica wrote to the
/// output shard, and self-corrects exactly as when hydrating from scratch.
///
/// Returns the streams the sink must consume, and a token that must live as long as the sink.
pub(crate) fn checkpoint_sink<'s>(
    (ok, err): SinkStreams<'s>,
    site: SinkSite,
) -> (SinkStreams<'s>, Option<Rc<dyn Any>>) {
    let Some(frontier) = site.restore else {
        attach_sink_writer(ok.clone(), err.clone(), &site);
        return ((ok, err), None);
    };
    let (restored_ok, restored_err, button) = restored_sink_snapshot(ok.scope(), &site, frontier);
    let (all_ok, all_err) = (
        ok.clone().concat(restored_ok),
        err.clone().concat(restored_err),
    );
    // A seeded generation's shards are empty, so its writer must also write the snapshot.
    if site.seeded {
        attach_sink_writer(all_ok.clone(), all_err.clone(), &site);
    } else {
        attach_sink_writer(ok, err, &site);
    }
    ((all_ok, all_err), Some(Rc::new(button)))
}

/// Tags that keep ok and error updates apart in a sink shard.
const SINK_OK: u8 = 0;
const SINK_ERR: u8 = 1;

/// Emits a sink shard's contents at `frontier - 1`, at that time.
fn restored_sink_snapshot<'s>(
    scope: timely::dataflow::Scope<'s, Timestamp>,
    site: &SinkSite,
    frontier: Timestamp,
) -> (
    StreamVec<'s, Timestamp, (Row, Timestamp, Diff)>,
    StreamVec<'s, Timestamp, (DataflowErrorSer, Timestamp, Diff)>,
    PressOnDropButton,
) {
    let as_of = frontier.step_back().expect("restore frontier is positive");
    let client = site.client.clone();
    let shards = site.restore_shards.clone();
    let mut op = AsyncOperatorBuilder::new(format!("CheckpointSinkRestore({})", site.shard), scope);
    let (ok_output, ok) = op.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let (err_output, err) = op.new_output::<CapacityContainerBuilder<Vec<_>>>();
    let button = op.build(move |capabilities| async move {
        let mut capabilities = capabilities.into_iter();
        let mut ok_cap = capabilities.next().expect("one per output");
        let mut err_cap = capabilities.next().expect("one per output");
        ok_cap.downgrade(&as_of);
        err_cap.downgrade(&as_of);
        for shard in shards {
            let started = Instant::now();
            let mut read = open_reader(&client, shard, "checkpoint restore").await;
            let stream = read
                .snapshot_and_stream(Antichain::from_elem(as_of))
                .await
                .expect("the checkpoint planner holds sink shards at the restore frontier");
            let mut stream = std::pin::pin!(stream);
            let mut count = 0_usize;
            while let Some(((key, _), _, diff)) = stream.next().await {
                count += 1;
                let diff = Diff::from(diff);
                match key.split_first() {
                    Some((&SINK_OK, row)) => {
                        ok_output.give(&ok_cap, (Row::from_bytes(row.to_vec()), as_of, diff));
                    }
                    Some((&SINK_ERR, err)) => {
                        err_output.give(
                            &err_cap,
                            (DataflowErrorSer::from_bytes(err.to_vec()), as_of, diff),
                        );
                    }
                    _ => panic!("malformed sink checkpoint update"),
                }
            }
            read.expire().await;
            info!(%shard, updates = count, elapsed = ?started.elapsed(), "restored sink from checkpoint");
        }
    });
    (ok, err, button.press_on_drop())
}

/// Appends the updates an MV sink's desired collection receives on this worker to the site's
/// shard, in rounds that end on interval boundaries.
///
/// A round's upper is decided only once the input frontier passes a boundary. Updates that must
/// fall in the next round, because they are before the next boundary, are staged as they arrive,
/// so hydrating from scratch never holds the whole snapshot. Later updates wait in memory, which
/// bounds that buffer by one interval of changes. Times are rounded down to the interval, so the
/// shard has a batch boundary at every interval multiple, like a site shard.
fn attach_sink_writer<'s>(
    ok: StreamVec<'s, Timestamp, (Row, Timestamp, Diff)>,
    err: StreamVec<'s, Timestamp, (DataflowErrorSer, Timestamp, Diff)>,
    site: &SinkSite,
) {
    let SinkSite {
        client,
        shard,
        initial_upper,
        dataflow_as_of,
        interval_ms,
        max_lag,
        shared,
        ..
    } = site;
    let (shard, interval_ms, max_lag) = (*shard, *interval_ms, *max_lag);
    let shared = Rc::clone(shared);
    let scope = ok.scope();
    let mut builder = OperatorBuilder::new(format!("CheckpointSinkWriter({shard})"), scope.clone());
    let info = builder.operator_info();
    let mut ok_input = builder.new_input(ok, Pipeline);
    let mut err_input = builder.new_input(err, Pipeline);

    let (msg_tx, msg_rx) = mpsc::channel(1);
    let (ack_tx, ack_rx) = std::sync::mpsc::channel();
    let sync_activator = scope.worker().sync_activator_for(info.address.to_vec());
    let task = mz_ore::task::spawn(
        || format!("arrangement-checkpoint-sink-writer-{shard}"),
        write_task(client.clone(), shard, msg_rx, ack_tx, sync_activator),
    );
    let first = first_boundary(*dataflow_as_of, interval_ms);
    let initial_upper = *initial_upper;

    builder.build(move |_capabilities| {
        let task = task.abort_on_drop();
        // The smallest boundary beyond `lower` at which a round may end.
        let bound =
            move |lower: Timestamp| std::cmp::max(first, first_boundary(lower, interval_ms));
        let round_time = move |t: Timestamp, lower: Timestamp| {
            std::cmp::max(
                Timestamp::new(u64::from(t) / interval_ms * interval_ms),
                lower,
            )
        };
        // The lower of the round being filled: the shard's upper once every sealed round lands.
        let mut lower = initial_upper;
        let mut begun = false;
        let mut sealed: Option<(Timestamp, Instant)> = None;
        // The latest boundary at which this writer probed the shard while waiting.
        let mut probed = Timestamp::minimum();
        // Updates before `bound(lower)`, already rounded.
        let mut early = Chunk::default();
        // Updates at or after `bound(lower)`, at their own times.
        let mut late: Vec<Update> = Vec::new();
        let mut queue: std::collections::VecDeque<WriteMsg> = Default::default();
        let mut msg_tx = Some(msg_tx);

        move |frontiers| {
            let _ = &task;
            let mut accept = |key: Vec<u8>, t: Timestamp, d: Diff| {
                // While waiting, rounds resume at or after `first`. See `waiting`.
                if t < lower || (waiting(lower, first) && t < first) {
                    return;
                }
                let bytes = key.len() + 16;
                if t < bound(lower) {
                    early
                        .updates
                        .push(((key, Vec::new()), round_time(t, lower), d.into_inner()));
                    early.bytes += bytes;
                } else {
                    late.push(((key, Vec::new()), t, d.into_inner()));
                }
            };
            let enabled = msg_tx.is_some() && shared.enabled.get() != Some(false);
            ok_input.for_each(|_time, data| {
                if !enabled {
                    return;
                }
                for (row, t, d) in data.drain(..) {
                    let mut key = vec![SINK_OK];
                    key.extend(row.to_bytes());
                    accept(key, t, d);
                }
            });
            err_input.for_each(|_time, data| {
                if !enabled {
                    return;
                }
                for (err, t, d) in data.drain(..) {
                    let mut key = vec![SINK_ERR];
                    key.extend(err.to_bytes());
                    accept(key, t, d);
                }
            });
            if msg_tx.is_none() || shared.enabled.get() != Some(true) {
                return;
            }
            let stop = |why: &str| warn!(%shard, %why, "stopping sink checkpoint writer");

            loop {
                match ack_rx.try_recv() {
                    Ok(upper) => {
                        sealed = None;
                        if upper > lower {
                            // Another replica's round landed beyond ours, with the same contents.
                            lower = upper;
                            begun = false;
                            early = Chunk::default();
                            let floor = if waiting(lower, first) { first } else { upper };
                            late.retain(|(_, t, _)| *t >= floor);
                        }
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        stop("write task stopped");
                        msg_tx = None;
                        return;
                    }
                }
            }
            if sealed.is_some_and(|(_, at)| at.elapsed() > max_lag) {
                stop("checkpoint round exceeds maximum lag");
                msg_tx = None;
                return;
            }

            // An input that has finished does not hold the frontier back.
            let frontier = frontiers
                .iter()
                .filter_map(|f| f.frontier().first().copied())
                .min();
            let boundary =
                frontier.map(|f| Timestamp::new(u64::from(f) / interval_ms * interval_ms));

            let flush = |updates: &mut Vec<Update>,
                         upper: Timestamp,
                         queue: &mut std::collections::VecDeque<WriteMsg>| {
                if updates.is_empty() {
                    return;
                }
                consolidate_updates(updates);
                let updates = std::mem::take(updates);
                if !updates.is_empty() {
                    queue.push_back(WriteMsg::Chunk { updates, upper });
                }
            };
            if early.bytes >= CHUNK_BYTES || (!early.updates.is_empty() && !begun) {
                if !begun {
                    queue.push_back(WriteMsg::Begin { lower });
                    begun = true;
                }
                if early.bytes >= CHUNK_BYTES {
                    flush(&mut early.updates, bound(lower), &mut queue);
                    early.bytes = 0;
                }
            }
            if waiting(lower, first) {
                if let Some(boundary) = boundary.filter(|b| sealed.is_none() && *b > probed) {
                    if queue.is_empty() {
                        probed = boundary;
                        queue.push_back(WriteMsg::Probe);
                        sealed = Some((lower, Instant::now()));
                    }
                }
            } else if let Some(boundary) = boundary {
                if sealed.is_none() && boundary >= first && boundary > lower {
                    if !begun {
                        queue.push_back(WriteMsg::Begin { lower });
                    }
                    flush(&mut early.updates, bound(lower), &mut queue);
                    early.bytes = 0;
                    let mut round: Vec<Update> = Vec::new();
                    late.retain(|(kv, t, d)| {
                        if *t < boundary {
                            round.push((kv.clone(), round_time(*t, lower), *d));
                            false
                        } else {
                            true
                        }
                    });
                    for chunk in round.chunks(CHUNK_BYTES / 64) {
                        flush(&mut chunk.to_vec(), boundary, &mut queue);
                    }
                    queue.push_back(WriteMsg::Seal { upper: boundary });
                    sealed = Some((boundary, Instant::now()));
                    lower = boundary;
                    begun = false;
                    // Late updates now before the next bound belong to the next round's early
                    // set.
                    let next = bound(lower);
                    late.retain(|(kv, t, d)| {
                        if *t < next {
                            early.bytes += kv.0.len() + 16;
                            early.updates.push((kv.clone(), round_time(*t, lower), *d));
                            false
                        } else {
                            true
                        }
                    });
                }
            }

            while let Some(msg) = queue.pop_front() {
                let tx = msg_tx.as_ref().expect("writer running");
                match tx.try_send(msg) {
                    Ok(()) => {}
                    // The write task activates this operator when it takes a message.
                    Err(mpsc::error::TrySendError::Full(msg)) => {
                        queue.push_front(msg);
                        return;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        stop("write task stopped");
                        msg_tx = None;
                        queue.clear();
                        return;
                    }
                }
            }
        }
    });
}

/// Whether a writer whose shard is at `durable` must wait for another replica to advance it to at
/// least `first`, the writer's first checkpoint boundary.
///
/// NOTE: a writer that joined a generation in progress can adopt an upper below `first`. Its
/// arrangements hold every update before its `as_of` at one compacted time, so a round starting
/// there would write that history again. Only a round from the minimum, which writes the whole
/// arrangement, or from `first` on is correct.
fn waiting(durable: Timestamp, first: Timestamp) -> bool {
    Timestamp::minimum() < durable && durable < first
}

/// Encoded bytes per persist batch a writer appends. A writer holds at most three chunks: one
/// being filled, one queued for its write task, and one being uploaded.
const CHUNK_BYTES: usize = 4 << 20;

/// Updates a writer examines per activation before yielding the worker.
const FUEL: isize = 100_000;

/// A message from a writer operator to its write task.
enum WriteMsg {
    /// Starts a round at `lower`.
    Begin { lower: Timestamp },
    /// Stages a persist batch of updates for the current round, all at times before `upper`.
    ///
    /// A round's upper can be decided after some of its chunks were staged. Sealing raises each
    /// staged batch's upper to the round's, which persist does in metadata.
    Chunk {
        updates: Vec<Update>,
        upper: Timestamp,
    },
    /// Appends the round's batches, advancing the shard's upper to `upper`.
    Seal { upper: Timestamp },
    /// Acknowledges the shard's current upper, for a writer waiting on another replica.
    Probe,
}

/// A round of a writer: one append covering every interval since the previous one.
struct Round {
    /// `None` once every update has been queued and the round sealed.
    encoder: Option<Box<dyn RoundEncoder>>,
    chunk: Chunk,
    upper: Timestamp,
    started: Instant,
}

/// Converts a [`Timestamp`] to the time type of a checkpointed trace.
fn to_time<T: 'static>(time: Timestamp) -> T {
    let time: Box<dyn Any> = Box::new(time);
    *time
        .downcast::<T>()
        .expect("checkpointed traces use `Timestamp`")
}

/// Converts a frontier element of a checkpointed trace to [`Timestamp`].
fn timestamp<T: 'static>(time: &T) -> Timestamp {
    let time: &dyn Any = time;
    *time
        .downcast_ref::<Timestamp>()
        .expect("checkpointed traces use `Timestamp`")
}

/// Follows an arrangement's batch stream and appends its updates to the site's shard.
///
/// The writer holds the arrangement's batches, which the trace shares, until they are durable.
/// When no append is in flight it starts a round up to the latest interval boundary the frontier
/// has passed, so a slow append makes the next round cover more intervals rather than queueing
/// one append per interval. If a round takes longer than the maximum lag, the writer stops and
/// releases its batches. See [`Committer::advance`] for what that means for the input holds.
///
/// Generic over the time type because arrange sites are. [`next_site`] only returns a site for
/// traces over [`Timestamp`], so the downcasts cannot fail.
///
/// `restored` is the span the trace was seeded with, which the batch stream never carries. The
/// writer writes it only when it is not yet durable, which is for a seeded generation.
pub(crate) fn attach_writer<'scope, T, B, Tr>(
    stream: Stream<'scope, T, Vec<Span<T, B>>>,
    trace: Tr,
    site: Site,
    restored: Option<Span<T, B>>,
) where
    T: timely::progress::Timestamp,
    B: Clone + 'static,
    Tr: TraceReader<Time = T> + 'static,
{
    let WriterSpec {
        site,
        codec,
        client,
        shard,
        initial_upper,
        dataflow_as_of,
        interval_ms,
        max_lag,
        shared,
    } = site.writer;

    let scope = stream.scope();
    let mut builder = OperatorBuilder::new(format!("CheckpointWriter({site})"), scope.clone());
    let info = builder.operator_info();
    let mut input = builder.new_input(stream, Pipeline);

    let (msg_tx, msg_rx) = mpsc::channel(1);
    let (ack_tx, ack_rx) = std::sync::mpsc::channel();
    let sync_activator = scope.worker().sync_activator_for(info.address.to_vec());
    let task = mz_ore::task::spawn(
        || format!("arrangement-checkpoint-writer-{shard}"),
        write_task(client, shard, msg_rx, ack_tx, sync_activator),
    );

    // NOTE: The spine merges a batch only once its upper is at or before the physical compaction
    // frontier. Holding that frontier at the durable upper keeps every batch this writer still
    // needs unmerged, so holding them costs no memory beyond the trace's own. Without the hold,
    // a merge during a round leaves both its inputs and its output alive. Logical compaction only
    // affects merged batches, which the writer never reads, so it holds none.
    let mut trace = Some(trace);
    if let Some(trace) = &mut trace {
        trace.set_logical_compaction(Antichain::new().borrow());
        let first = first_boundary(dataflow_as_of, interval_ms);
        let needed = if waiting(initial_upper, first) {
            first
        } else {
            initial_upper
        };
        trace.set_physical_compaction(Antichain::from_elem(to_time::<T>(needed)).borrow());
    }

    builder.build(move |_capabilities| {
        let activator = scope.activator_for(Rc::clone(&info.address));
        let first = first_boundary(dataflow_as_of, interval_ms);
        // `(lower, upper, batch)` for every batch with updates that are not yet durable.
        let mut held: Vec<(Timestamp, Timestamp, B)> = Vec::new();
        if let Some(span) = restored {
            let upper = span.upper().first().map_or(Timestamp::MAX, timestamp);
            let lower = span.lower().first().map_or(Timestamp::MAX, timestamp);
            if let (Some(batch), true) = (span.inner, upper > initial_upper) {
                held.push((lower, upper, batch));
            }
        }
        // The shard's upper, as of the last acknowledged append.
        let mut durable = initial_upper;
        // The latest boundary at which this writer probed the shard while waiting.
        let mut probed = Timestamp::minimum();
        let mut round: Option<Round> = None;
        let mut queue: std::collections::VecDeque<WriteMsg> = Default::default();
        let mut msg_tx = Some(msg_tx);
        let task = task.abort_on_drop();

        move |frontiers| {
            // Aborts the write task when the operator is dropped.
            let _ = &task;
            if shared.enabled.get() == Some(false) {
                trace = None;
            }
            input.for_each(|_time, data| {
                if msg_tx.is_none() || shared.enabled.get() == Some(false) {
                    return;
                }
                for span in data.drain(..) {
                    let upper = span.upper().first().map_or(Timestamp::MAX, timestamp);
                    if upper <= durable {
                        continue;
                    }
                    let lower = span.lower().first().map_or(Timestamp::MAX, timestamp);
                    if let Some(batch) = span.inner {
                        held.push((lower, upper, batch));
                    }
                }
            });
            if msg_tx.is_none() || shared.enabled.get() != Some(true) {
                return;
            }
            let stop = |why: &str, durable: Timestamp| {
                warn!(%shard, %durable, %why, "stopping arrangement checkpoint writer");
            };

            loop {
                match ack_rx.try_recv() {
                    Ok(upper) => {
                        durable = upper;
                        round = None;
                        // While waiting, nothing before `first` will be written by this writer.
                        let needed = if waiting(durable, first) {
                            first
                        } else {
                            durable
                        };
                        held.retain(|(_, upper, _)| *upper > needed);
                        if let Some(trace) = &mut trace {
                            trace.set_physical_compaction(
                                Antichain::from_elem(to_time::<T>(needed)).borrow(),
                            );
                        }
                    }
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        stop("write task stopped", durable);
                        held.clear();
                        msg_tx = None;
                        trace = None;
                        return;
                    }
                }
            }

            let Some(frontier) = frontiers[0].frontier().first().map(timestamp) else {
                return;
            };
            let boundary = Timestamp::new(u64::from(frontier) / interval_ms * interval_ms);
            // Lag is wall-clock time, because a frontier can jump far ahead in one step, for
            // example when a dataflow starts from an old `as_of`. A round that finishes promptly
            // then covers the jump, so only a slow round means the writer is not keeping up.
            if round
                .as_ref()
                .is_some_and(|r| r.started.elapsed() > max_lag)
            {
                stop("checkpoint round exceeds maximum lag", durable);
                held.clear();
                msg_tx = None;
                trace = None;
                round = None;
                queue.clear();
                return;
            }

            if round.is_none() && queue.is_empty() && waiting(durable, first) && boundary > probed {
                probed = boundary;
                queue.push_back(WriteMsg::Probe);
            }
            if round.is_none()
                && !waiting(durable, first)
                && boundary >= first
                && boundary > durable
            {
                let batches: Vec<B> = held
                    .iter()
                    .filter(|(lower, _, _)| *lower < boundary)
                    .map(|(_, _, batch)| batch.clone())
                    .collect();
                round = Some(Round {
                    encoder: Some((codec.encoder)(
                        Box::new(batches),
                        durable,
                        boundary,
                        interval_ms,
                    )),
                    chunk: Chunk::default(),
                    upper: boundary,
                    started: Instant::now(),
                });
                queue.push_back(WriteMsg::Begin { lower: durable });
            }

            let mut fuel = FUEL;
            loop {
                while let Some(msg) = queue.pop_front() {
                    let tx = msg_tx.as_ref().expect("writer running");
                    match tx.try_send(msg) {
                        Ok(()) => {}
                        // The write task activates this operator when it takes a message.
                        Err(mpsc::error::TrySendError::Full(msg)) => {
                            queue.push_front(msg);
                            return;
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            stop("write task stopped", durable);
                            held.clear();
                            msg_tx = None;
                            trace = None;
                            round = None;
                            queue.clear();
                            return;
                        }
                    }
                }
                let Some(Round {
                    encoder: Some(encoder),
                    chunk,
                    upper,
                    ..
                }) = round.as_mut()
                else {
                    return;
                };
                if fuel <= 0 {
                    activator.activate();
                    return;
                }
                let upper = *upper;
                let done = encoder.fill(chunk, CHUNK_BYTES, &mut fuel);
                if done || chunk.bytes >= CHUNK_BYTES {
                    let chunk = std::mem::take(chunk);
                    if !chunk.updates.is_empty() {
                        queue.push_back(WriteMsg::Chunk {
                            updates: chunk.updates,
                            upper,
                        });
                    }
                }
                if done {
                    round.as_mut().expect("in a round").encoder = None;
                    queue.push_back(WriteMsg::Seal { upper });
                }
            }
        }
    });
}

type CkptBatch = mz_persist_client::batch::Batch<Vec<u8>, Vec<u8>, Timestamp, i64>;

/// Stages and appends a writer's rounds, and acknowledges each with the shard's new upper.
///
/// Every replica of the same size runs the same dataflow with the same key and writes the same
/// shards, and a round's contents are a function of the key and the round's bounds. So when
/// another replica's append wins, its contents are what this writer would have written. The task
/// then acknowledges the winner's upper, which the writer adopts, and skips any round it had
/// already started below it.
async fn write_task(
    client: PersistClient,
    shard: ShardId,
    mut rx: mpsc::Receiver<WriteMsg>,
    ack: std::sync::mpsc::Sender<Timestamp>,
    activator: timely::scheduling::SyncActivator,
) {
    // Opened at the first round, so a dataflow that never checkpoints creates no shard.
    let mut write = None;
    // The round's lower, or `None` if the round is below an adopted upper and skipped.
    let mut round: Option<Antichain<Timestamp>> = None;
    let mut adopted = Timestamp::minimum();
    let mut batches: Vec<CkptBatch> = Vec::new();
    let mut started = Instant::now();
    let mut updates = 0;
    while let Some(msg) = rx.recv().await {
        let _ = activator.activate();
        match msg {
            WriteMsg::Begin { lower } => {
                if write.is_none() {
                    write = Some(open_writer(&client, shard, "checkpoint writer").await);
                }
                round = (lower >= adopted).then(|| Antichain::from_elem(lower));
                started = Instant::now();
                updates = 0;
            }
            WriteMsg::Chunk {
                updates: chunk,
                upper,
            } => {
                let Some(lower) = round.clone() else {
                    continue;
                };
                let write = write.as_mut().expect("opened at the round's start");
                updates += chunk.len();
                let batch = write
                    .batch(&chunk, lower, Antichain::from_elem(upper))
                    .await
                    .expect("updates within the round");
                batches.push(batch);
            }
            WriteMsg::Seal { upper } => {
                let Some(lower) = round.take() else {
                    if ack.send(adopted).is_err() {
                        break;
                    }
                    let _ = activator.activate();
                    continue;
                };
                let write = write.as_mut().expect("opened at the round's start");
                let upper = Antichain::from_elem(upper);
                for batch in &mut batches {
                    if batch.upper() != &upper {
                        batch
                            .rewrite_ts(&lower, upper.clone())
                            .expect("a round's upper bounds its chunks");
                    }
                }
                if batches.is_empty() {
                    let empty: [Update; 0] = [];
                    let batch = write
                        .batch(&empty, lower.clone(), upper.clone())
                        .await
                        .expect("valid usage");
                    batches.push(batch);
                }
                let parts = batches.len();
                let mut refs: Vec<&mut CkptBatch> = batches.iter_mut().collect();
                let result = write
                    .compare_and_append_batch(&mut refs, lower.clone(), upper.clone(), true)
                    .await
                    .expect("valid usage");
                let new_upper = match result {
                    Ok(()) => {
                        batches.clear();
                        let upper = *upper.as_option().expect("finite upper");
                        info!(
                            %shard,
                            lower = ?lower.as_option(),
                            %upper,
                            updates,
                            batches = parts,
                            elapsed = ?started.elapsed(),
                            "wrote arrangement checkpoint"
                        );
                        upper
                    }
                    Err(mismatch) => {
                        for batch in batches.drain(..) {
                            batch.delete().await;
                        }
                        let Some(&current) = mismatch.current.as_option() else {
                            // Finalized: a newer generation or key superseded this one.
                            info!(%shard, "checkpoint shard closed, stopping writer");
                            break;
                        };
                        info!(%shard, %current, "another replica wrote this checkpoint round");
                        adopted = current;
                        current
                    }
                };
                if ack.send(new_upper).is_err() {
                    break;
                }
                let _ = activator.activate();
            }
            WriteMsg::Probe => {
                if write.is_none() {
                    write = Some(open_writer(&client, shard, "checkpoint writer").await);
                }
                let upper = write
                    .as_mut()
                    .expect("opened")
                    .fetch_recent_upper()
                    .await
                    .clone();
                let Some(&current) = upper.as_option() else {
                    info!(%shard, "checkpoint shard closed, stopping writer");
                    break;
                };
                adopted = std::cmp::max(adopted, current);
                if ack.send(adopted).is_err() {
                    break;
                }
                let _ = activator.activate();
            }
        }
    }
    // Batches written but never appended would otherwise leak their blobs.
    for batch in batches {
        batch.delete().await;
    }
}

/// Advances the committer once per interval until it stops.
async fn run_committer(client: PersistClient, spec: CommitSpec) {
    let period = Duration::from_millis(spec.interval_ms);
    let mut committer = Committer::new(&client, spec).await;
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if !committer.advance().await {
            return;
        }
    }
}

/// Tracks the committed frontier, and holds history one interval behind it.
struct Committer {
    interval_ms: u64,
    max_lag: Duration,
    /// Writers of every site and sink shard, used to read their uppers.
    shards: Vec<CkptWriteHandle>,
    /// Since handles of the same shards. Holding them back lets persist consolidate superseded
    /// rounds, so a shard's size follows the arrangement's, not the history of its updates.
    sinces: Vec<CkptSinceHandle>,
    /// The committed frontier, and when it last advanced.
    progress: Option<(Timestamp, Instant)>,
    /// Leased readers on the inputs.
    held: Vec<InputReadHandle>,
}

impl Committer {
    /// Opens leased readers on every input. They register at the shard's current
    /// since, which the controller holds at or before the dataflow's `as_of` while the dataflow
    /// is created.
    async fn new(client: &PersistClient, spec: CommitSpec) -> Self {
        let mut held = Vec::new();
        for (shard, desc) in spec.held {
            let read = client
                .open_leased_reader(
                    shard,
                    Arc::new(desc),
                    Arc::new(UnitSchema),
                    diagnostics("checkpoint input hold"),
                    false,
                )
                .await
                .expect("input shard codecs match");
            info!(%shard, since = ?read.since(), "holding history for arrangement checkpoints");
            held.push(read);
        }
        let mut shards = Vec::new();
        let mut sinces = Vec::new();
        for shard in spec.shards {
            shards.push(open_writer(client, shard, "checkpoint commit").await);
            sinces.push(open_since(client, shard, "checkpoint commit").await);
        }
        Self {
            interval_ms: spec.interval_ms,
            max_lag: spec.max_lag,
            shards,
            sinces,
            progress: None,
            held,
        }
    }

    /// Advances the holds to the committed frontier. Returns whether to keep running.
    ///
    /// The committed frontier is the minimum upper over all shards. If it stalls for longer than
    /// the maximum lag, the input holds are released for good. A later restore then
    /// finds them compacted past the checkpoint and starts from scratch.
    async fn advance(&mut self) -> bool {
        let mut committed = Timestamp::MAX;
        let mut started = false;
        for write in &mut self.shards {
            let upper = write.fetch_recent_upper().await;
            let Some(&upper) = upper.as_option() else {
                // Finalized: a newer generation or key superseded this one.
                return false;
            };
            committed = std::cmp::min(committed, upper);
            started |= upper > Timestamp::minimum();
        }
        // A dataflow hydrating from scratch writes its first round only once it has caught up,
        // which can take longer than the maximum lag.
        if !started {
            return true;
        }
        match &mut self.progress {
            Some((last, since)) if *last == committed => {
                if since.elapsed() > self.max_lag {
                    warn!(
                        %committed,
                        "committed checkpoint exceeds maximum lag, releasing history holds"
                    );
                    // Dropping a leased reader expires it.
                    self.held.clear();
                    return false;
                }
                return true;
            }
            progress => *progress = Some((committed, Instant::now())),
        }
        // Restore picks the latest boundary at or before the dataflow's `as_of + 1`, which trails
        // the committed frontier by the index's compaction window. Holding one interval further
        // back covers windows shorter than the interval.
        let Some(hold) = u64::from(committed)
            .checked_sub(self.interval_ms + 1)
            .map(Timestamp::new)
        else {
            return true;
        };
        let hold = Antichain::from_elem(hold);
        for read in &mut self.held {
            if PartialOrder::less_than(read.since(), &hold) {
                read.downgrade_since(&hold).await;
            }
        }
        let opaque = since_opaque();
        for since in &mut self.sinces {
            if PartialOrder::less_than(since.since(), &hold) {
                // A finalizer may have advanced it to the empty frontier, which is fine.
                let _ = since
                    .compare_and_downgrade_since(&opaque, (&opaque, &hold))
                    .await;
            }
        }
        info!(%committed, "committed arrangement checkpoint");
        true
    }
}
