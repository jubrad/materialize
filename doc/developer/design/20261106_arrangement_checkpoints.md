# Arrangement checkpoints

- Associated: spike branch `justin/arrangement-checkpoints-spike`

## The Problem

When a replica restarts, every dataflow on it rehydrates from scratch. Each
dataflow reads a full snapshot of its inputs from persist, rebuilds every
arrangement, and recomputes every stateful operator (reduces, joins, top-k)
before it can serve again.

The restarts users feel are the ones within a single release:

* a replica is OOM-killed or crashes,
* a node fails, or a pod is evicted or rescheduled,
* any replica restart on a cluster with a single replica, which then serves
  nothing until hydration finishes.

0dt upgrades and graceful cluster resizes already hide hydration, because the
new replicas hydrate while the old ones keep serving.

For a dataflow whose state is expensive to recompute (large reduces, many-way
joins), hydration can take far longer than reading the state it produces.

## Success Criteria

* A replica restart within a release resumes a supported dataflow from a recent
  checkpoint of its operator state, then replays only the input updates after
  that checkpoint.
* Restored state is exactly the state a full recompute would produce. Any doubt
  about whether a checkpoint matches the current plan, build, configuration, or
  inputs means recompute from scratch.
* Restore takes measurably less time than full hydration for dataflows
  dominated by recompute cost.
* Checkpointing does not hold back input compaction by more than one
  checkpoint interval.

## Out of Scope

* Reusing checkpoints across releases. The checkpoint key includes the build,
  so every release starts from scratch. See [Versioning](#versioning).
* Reducing the memory a hydrated dataflow needs. A restored arrangement occupies
  the same memory as a recomputed one. A replica that OOMs while hydrating will
  OOM again while restoring.
* Persisted indexes as a serving structure, where lookups read from persist
  instead of memory. That is a separate tradeoff, with slower lookups in
  exchange for shared storage, and it does not reduce hydration work.

## Solution Proposal

Periodically write the contents of a dataflow's arrangements to persist shards,
at a frontier `F` that every arrangement in the dataflow has passed. On restart,
if a complete checkpoint with an exactly matching key exists, start every
arrangement and stateful operator from its state at `F`, and read the
dataflow's inputs from `F` onward without a snapshot.

### Why this works

Differential dataflow gives consistent cuts without a barrier protocol. An
arrangement whose upper is `F` contains exactly the updates at times before
`F`. If every arrangement in a dataflow is restored at the same `F`, and the
inputs replay exactly the updates at or after `F`, every operator sees what it
would have seen had the dataflow never stopped.

Operators process a new input batch against their existing traces. A reduce
reads its input trace and output trace up to the batch's lower bound, and a join
reads the other side's trace. The traces only have to contain the right updates
and report the right frontiers. How they got those updates does not matter.

### Restore

For each arrangement, restore reads its checkpoint shard at `as_of = F - 1`,
builds a single trace batch spanning `[minimum, F)`, and inserts it into the
trace without sending it downstream. Operators that maintain their own output
trace, such as reduce, do the same for that trace and start with their internal
frontiers at `F` instead of the minimum time.

Every input is read with `as_of = F - 1` and its snapshot excluded, so the
dataflow sees updates at times `F` and later. The persist source already
supports this mode (`SnapshotMode::Exclude`).

All workers must restore at the same `F`. Updates are exchanged between
workers, so a worker replaying from an earlier frontier would send updates
another worker's restored state already contains. Worker 0 chooses `F` and
records it in the dataflow description before broadcasting the
`CreateDataflow` command, so every worker in every process sees the same
value.

### Writing checkpoints

Each arrangement has a writer operator attached to its batch stream. The writer
appends the arrangement's updates to its shard in rounds:

* Checkpoint frontiers are multiples of the interval in timestamp space, so
  every writer in a dataflow picks the same frontiers without coordinating.
* When no append is in flight, the writer starts a round `[durable, B)`, where
  `B` is the latest interval boundary the input frontier has passed. A slow
  append makes the next round cover several intervals. Nothing queues up one
  append per interval.
* Each update's time is rounded down to a multiple of the interval, so every
  interval boundary inside a round stays a valid restore frontier. Restore reads
  at `F - 1`, and an update is included exactly when its original time is
  before `F`.
* The writer reads the round with a merged cursor over the arrangement's own
  batches. Each `(key, val)` is visited once across all batches, so updates
  that cancel within an interval never reach object storage, and upload volume
  is bounded by net change, not update count.
* Encoding is fueled, so a round yields the worker thread between activations.
  It produces chunks of at most 4 MiB, each written as one persist batch, and all
  of a round's batches are appended together with one compare-and-append. A
  writer holds at most three chunks: one being filled, one queued, and one being
  uploaded.
* Each `(arrangement, worker)` pair has its own shard and a single writer, so
  appends never conflict.

The writer holds a trace handle whose physical compaction frontier is the
durable upper. The spine only merges a batch once its upper is at or before
that frontier, so the batches the writer still needs stay unmerged and holding
them costs no memory beyond the trace's own. Without that hold, a merge during
a round keeps both its inputs and its output alive.

A checkpoint at `F` is complete once every shard of the dataflow has an upper of
at least `F`. The committed frontier is the minimum upper across the shards.

### Bounded lag

`arrangement_checkpoint_max_lag` bounds two things, both in wall-clock time:

* A round that takes longer than the maximum lag stops the writer. It drops its
  batches and its trace handle, so the arrangement merges normally again.
* The committer releases the input holds if the committed frontier stays put for
  longer than the maximum lag while its own writer keeps committing. That covers
  a writer on another worker that stopped or fell behind.

Lag is measured in wall-clock time because a dataflow's frontier can jump far
ahead in one step, for example when it starts from an old `as_of`. Measured in
timestamps, that jump looked like lag even though the next round covered it in
milliseconds.

Stopping is fail-closed. A stopped checkpoint is still correct at its committed
frontier. Once the input holds are released, a later restore finds the inputs
compacted past it and starts from scratch.

### Input history

Replaying from `F` requires each input's since to be at or before `F - 1`, and
a restored MV sink reads its output shard from `F - 1` too. A committer task on
worker 0 holds a leased reader on every input and MV output, and advances it to
one interval behind the committed frontier once per interval. Input history is
therefore retained for about two checkpoint intervals.

The committer also holds a critical since handle on every checkpoint shard at
the same frontier. Without it the shards' since would stay at the minimum, and
persist could never consolidate superseded rounds, so a shard would grow with
the history of its arrangement rather than its size. The handle is not leased,
so the since stays put while no replica runs the dataflow.

### Cleanup

Only the latest generation of the latest key of a dataflow is kept.

* A new generation finalizes the shards of the one it replaces.
* A pointer shard per dataflow identity, the export IDs plus the worker count,
  records the key the dataflow was last created with. A different key, from a
  new build, configuration, or plan, finalizes the old key's manifest and
  shards.
* When the controller drops all of a dataflow's exports, the replica
  finalizes its key and pointer.

Finalizing closes the shard's upper, which stops any writer of a
still-running incarnation at its next append. It then downgrades the critical
since to the empty frontier and finalizes. A restore that is reading the shard
holds a leased reader, which blocks finalization until it expires, so
finalization retries for longer than the read lease.

An alternative that avoids the hold: if the checkpoint contains the arrangements
built directly on the raw inputs, restore can compute the missing updates as
`snapshot(input, now) - checkpointed_input_at_F`. That only works where an
arrangement holds the complete input, before any MFP drops columns or rows.

### Materialized views

An MV's dataflow ends in a sink that self-corrects: it compares the dataflow's
output, its desired collection, with the contents of its output shard and
appends the difference. A restored dataflow emits only the changes from `F` on,
so the sink would see an empty desired snapshot and retract everything.

The desired collection is usually not arranged anywhere, because an MFP
follows the last arrangement. So each sink gets one more site: a writer per
worker appends the desired updates that worker receives to the generation's own
shard, in rounds on the same interval boundaries. On restore, each worker reads
its shard at `F - 1` and feeds that snapshot into the sink ahead of the replayed
changes. The sink reads its output shard back in full, as when hydrating from
scratch, and self-corrects normally.

The sink therefore trusts nothing outside its generation's shards. That
matters during a zero-downtime upgrade, when the previous release keeps
appending to the output shard and may compute different results. Trusting the
output shard at `F - 1` instead would skip the correction for anything the
previous release wrote differently.

The writer cannot read finished batches as an arrangement writer does, so it
buffers updates. A round's upper is known only once the frontier passes a
boundary, but every update before the next boundary must fall in the next
round. Those are staged to persist as they arrive, with a provisional upper
that `Batch::rewrite_ts` raises when the round seals. Hydrating from scratch
therefore never holds the whole snapshot, and only updates beyond the next
boundary, at most one interval of changes, wait in memory.

The cost is a second copy of the MV's contents in object storage, and a restore
that reads both copies. Both are small next to recomputing.

### Multiple replicas

Replicas of the same size compute the same key and write the same shards.
Rounds end on interval boundaries, and each round is a compare-and-append from
the shard's upper, so exactly one replica's round lands for each range. A round's
contents depend only on the key and its bounds, so the loser adopts the
winner's upper, discards its own round, and continues from there.

### Versioning

A checkpoint is reusable only if everything that determines its contents is
identical. The checkpoint key is a hash of:

1. the dataflow's LIR plan, excluding `as_of` and `until`,
2. the build identity (version and commit),
3. the values of all dyncfgs that rendering reads,
4. the inputs' shard IDs and schemas (part of the plan's storage metadata),
5. the worker count, until restore can repartition (see below),
6. the checkpoint format version.

Restore fails closed. A missing manifest, a missing shard, a key mismatch, or
a decode error means the dataflow recomputes from scratch. A dataflow is never
restored partly from checkpoints and partly from scratch.

Including the build identity means every release invalidates every checkpoint.
A finer-grained key, such as per-function semantic versions bumped by hand, is
not exact, because a bug fix can change results without changing the plan.
Hashing the source of every crate that compute depends on is exact, but that
set changes nearly every release. Releases already hydrate in the background
with 0dt, so the exact key costs little.

Because the build is part of the key, checkpoints can use in-memory encodings
such as raw `Row` bytes without a stable serialization format.

### Checkpoint interval

| | Short interval | Long interval |
|---|---|---|
| Restore replay | Little | Up to one interval of input updates |
| Input history retained | Little | Up to one interval |
| Upload volume | Close to raw update volume | Approaches net change |
| Object store PUTs | More, smaller parts | Fewer, larger parts |
| Unmerged batches held by the writer | Up to one interval | Up to one interval |

A fixed interval is a reasonable start. A better policy checkpoints when the
estimated replay cost since the last checkpoint crosses a threshold.

### What to checkpoint

An operator whose state is not checkpointed can rebuild it by replaying its
restored upstream arrangement as a stream. A cost model can therefore choose a
cut set: checkpoint state that is expensive to recompute relative to its size
(reduce and join outputs), and skip arrangements that are only a sort of an
input. The first version checkpoints every arrangement in a supported dataflow.

## Minimal Viable Prototype

The spike on `justin/arrangement-checkpoints-spike` implements a vertical
slice, behind the `enable_arrangement_checkpoints`,
`arrangement_checkpoint_interval`, and `arrangement_checkpoint_max_lag`
dyncfgs.

### What the spike implements

* `src/compute/src/checkpoint.rs`: the key, eligibility check, derived shard
  IDs, manifest, codecs, restore read, writer operator, and the commit task that
  holds input sinces.
* `src/compute/src/extensions/restorable.rs`: copies of differential's
  `arrange_core` and `reduce_with_tactic` that start from a restored span. Every
  change from upstream is marked `CHECKPOINT:`.
* Hooks in `mz_arrange_core` and `mz_reduce_abelian`, so no call site changes.
  Sites are numbered in rendering order through a thread-local render context.
* `CommandPlanner`, on the channel adapter of the worker that sequences
  commands, sets `DataflowDescription::checkpoint_restore` before broadcast.

Scope limits, each enforced so the dataflow falls back to recompute:

* Index and MV dataflows. No other sinks, no MVs with a refresh schedule or
  `UP TO`, no sync MV sink, no index imports, no recursion, no txn-wal inputs.
* No `Join`, `TopK`, or `Threshold` nodes. `mz_join_core` tracks the other
  side's trace by the batches it has received, so it would not see restored
  state.
* Only `RowRowSpine`, `ErrSpine`, and `RowErrSpine` traces. Any other trace
  type (accumulable and monotonic reduces) poisons the dataflow at render time
  and it never writes a complete manifest.
* The worker count is part of the key.

### Spike results

Setup: a local environment, cluster `c` with 2 workers, a 5 second interval.
Inputs were a 2M row materialized view and an `AUCTION` load generator.
Indexes:

* `agg`: `string_agg` over 2,000 groups of 1,000 values.
* `bids_agg`: `string_agg` over live bids.
* `heavy`: a sorted `string_agg` hashed with `md5`, over 20 groups of 100,000
  values.

The replica was restarted with `REPLICATION FACTOR 0` then `1`. "Ready" is wall
clock time from the replica's creation until the index answered a query.

| Restart | Indexes | Restored | From scratch |
|---|---|---|---|
| Two runs each | `agg`, `bids_agg` | 0.8 to 2.0 s | 1.2 to 1.8 s |
| Two runs each | `agg`, `bids_agg`, `heavy` | 1.2 to 1.6 s | 2.4 to 4.0 s |
| Two runs each | `slow`, `bids_agg` | 1.0 to 1.7 s | 39 to 77 s |

`slow` is a `string_agg` over 3,000 groups of a 3M row input, where each value
first passes through a chain of 100 nested `md5` calls. The chain runs in the
reduce's input MFP, before the input arrangement, so a restore skips it
entirely. This is the case checkpoints target: hydration dominated by
recompute, with state no larger than the input. The from-scratch runs varied
because the replicas shared laptop cores with other clusters. Restore stayed at
about 1 s regardless.

* After every restore, each index matched an independently computed
  materialized view at the same timestamp, with zero differing rows. This
  includes a restore that replayed about 2.5 minutes of input updates, and a
  restore from a checkpoint written by an already restored incarnation.
* Restoring an arrangement of 1M rows per worker took about 350 ms. Writing
  it took 150 to 250 ms per shard.
* The win scales with recompute cost relative to state size. Restore still
  reads every arrangement, including the one over the raw input, so a cheap
  reduce over a large input gains little.
* `mz_compute_hydration_times` reports about 0.1 ms for a restored index. The
  restore happens while the dataflow is rendered, before that clock starts, so
  the metric needs to change before it can measure this.

#### Materialized views

Two MVs on a 2-worker replica, each checked against an independent copy on
another cluster: `slow_mv`, the expensive reduce above, and `bids_mv`, a reduce
over the live auction source.

| Run | Hydrated after |
|---|---|
| From scratch | 113.4 s |
| Restart, restored | 6.0 s |
| Second replica added while the first runs, restored | 7.8 s |
| Restart after both replicas wrote interleaved rounds | 2.5 s |

* Zero diffs after every run, rechecked 30 seconds later while the live source
  advanced.
* With two replicas, 650 of about 6,500 rounds in 90 seconds lost the race and
  adopted the winner's upper. No writer stopped, and the committed frontier
  kept advancing.
* An earlier version, which trusted the output shard instead of checkpointing
  the desired collection, restored in 3.8 s. Dropping an MV finalized its
  manifest and shards, and a new build superseded 79 keys through their
  pointers.

#### Memory

Peak resident memory of the replica process, sampled every 100 ms during a
restart, for an index over a static view with a live, empty branch. Arrangement
size is from `mz_dataflow_arrangement_sizes`.

| Arrangement | Checkpoints off | On, from scratch | Restored, batcher | Restored, direct |
|---|---|---|---|---|
| 4M rows, 393 MiB | 1,745 to 1,936 MiB, mean 1,814 | 1,892 to 2,096 MiB, mean 2,011 | 1,379 to 1,397 MiB | 789 to 812 MiB |
| 8M rows, 786 MiB | 3,063 and 3,216 MiB | 2,984 and 3,376 MiB | 2,399 and 2,415 MiB | not measured |

* From scratch, the hydration peak is the same with checkpoints on or off. The
  first full write adds a transient of about 180 MiB after hydration, while the
  writer's chunks and persist's part buffers are live. It does not grow with
  arrangement size, and at 786 MiB it is within run-to-run noise.
* A restore through a batcher peaks about 24% lower than a from-scratch
  hydration. It skips the input snapshot, but the batcher holds a sorted copy
  of the arrangement beside the batch it builds.
* A direct restore peaks about 57% lower than a from-scratch hydration, within
  about 200 MiB of where the replica settles. The encoding sorts like the
  trace, so restore builds the batch from persist's sorted snapshot in two
  passes: the first gathers the update counts and dictionary statistics that
  `seal` would read from a chain, the second pushes into a builder sized and
  configured from them. Each cursor prefetches at most 16 MiB. With persist's
  default 1 GiB prefetch budget the same restore peaked at about 1,200 MiB.
* The direct restore took 0.7 to 1.1 s per worker for 2M updates, against 1.0
  to 1.35 s through the batcher, despite reading the shard twice.
* After the first write, memory settles about 50 MiB above checkpoints off.
* Writing the 4M row checkpoint took 1.4 to 1.8 s per worker, as 57 batches of
  about 4 MiB.

#### Lag and coalescing

* With a 100 ms interval, every append covered 10 intervals, because the
  frontier advances once per second.
* A restore with a 10 s retain history landed 2.0 s into a 3.0 s append, at an
  interval boundary inside the round, on an index over live bids. The index
  matched an independent materialized view with zero differing rows.
* With the maximum lag set to 1 ms, every writer stopped before its first
  append. The next restart found no checkpoint frontier, started a new
  generation from scratch, and matched the independent view.

### Findings that changed the design

* **The controller's `as_of` trails the committed checkpoint.** An index's
  since trails its upper by its compaction window, so on a new replica the
  `as_of` was earlier than the latest committed frontier. Restoring there would
  answer reads between the two wrongly. The fix: every interval boundary is a
  batch boundary in every shard, restore picks the latest boundary at or
  before `as_of + 1`, and the input hold trails the committed frontier by one
  extra interval. That covers compaction windows shorter than the interval. A
  real implementation should hold inputs at the dataflow's read frontier.
* **Leased readers are good enough for input holds.** A leased reader left by
  a crashed replica keeps holding the input since until its lease expires, so a
  restart within the lease window can always replay, and a dropped dataflow
  never leaks a permanent hold. In one run, the restore replayed from a
  checkpoint 2.5 minutes old because an earlier incarnation's leases were
  still live.
* **Mixing aggregate kinds introduces a join.** A `GROUP BY` with both a basic
  aggregate and a hierarchical one lowers to separate reduces joined together.
  Supporting joins is required before typical aggregate views qualify.
* **The build SHA is not an exact build identity.** `build_info!()` uses the
  SHA of `HEAD`, which does not change with uncommitted changes. The spike
  hashes the running binary instead.
* **A constant input closes its frontier.** Writers must not checkpoint past
  an empty frontier, and restore must treat a closed shard as unusable.
* **Copying updates doubled hydration memory.** The first version copied every
  update of every batch into an owned buffer on arrival. The initial snapshot
  arrives at hydration, so the first checkpoint held a full second copy of
  every arrangement at peak memory. Reading the arrangement's own batches
  removes the copy, and the physical compaction hold keeps the spine from
  merging them into a second copy while the round runs.
* **Hashing the binary in one read added its size to peak memory.** The build
  identity read the whole 356 MiB binary into memory on the first dataflow
  creation. It is now hashed in 1 MiB blocks.
* **Timestamp lag is the wrong signal.** A writer measuring lag in timestamps
  stopped on every introspection index at startup, because their frontiers
  jumped from an old `as_of` to the present. Wall-clock round duration measures
  whether the writer keeps up.

### Known gaps in the spike

* Reconciliation after an environmentd restart re-plans the dataflow, which
  can start a new generation that the still-running dataflow does not write.
  This costs a restore opportunity, never correctness.
* Exports dropped while a replica was disconnected never reach it, so their
  checkpoints are not finalized. A sweep at the controller would need to know
  every live dataflow identity.
* The pointer keeps one key per identity. During a zero-downtime upgrade, old
  and new replicas of the same size have different keys and finalize each
  other's checkpoints when either restarts.
* A sink writer's queue to its write task is unbounded. If uploads fall behind
  a hydration from scratch, staged chunks accumulate in memory.
* Only the planning worker can compute the key. `split_command` gives each
  worker its own part of the plan, such as its slice of a constant collection,
  so workers verify only the environment digest that starts the key. An
  earlier version re-hashed the plan on every worker, reached different
  conclusions on different workers, rendered different operators, and
  panicked timely's channel allocation.
* Restore reads shards synchronously on the worker thread during rendering.
* Hashing the binary costs a few hundred milliseconds of CPU on the first
  dataflow creation in each process. It could start at process startup.
* A stopped writer stays stopped until the dataflow is re-created. It could
  start a new generation from the trace instead.
* Every shard does one consensus compare-and-append per round, even when
  nothing changed, so consensus load scales with arrangements times workers.
  One shard per dataflow with a single appender would remove both factors.
* `restore_frontier` keeps its probe readers alive for a fixed 60 seconds to
  cover the window until the dataflow's sources register their own.
* No automated tests. Validation was manual, against an independent
  materialized view.

## Alternatives

* **Persisted indexes for serving.** Answer lookups from a key-ordered persist
  shard with a local cache instead of an in-memory arrangement. This shares
  storage across replicas and survives restarts, but lookups go through object
  storage, and rehydration of the operator state upstream of the index is
  unchanged.
* **Persisting only input arrangements.** Loading inputs pre-sorted saves the
  sort but not the recompute, which dominates for expensive dataflows.
* **Spilling arrangements to local disk.** Already exists (lgalloc and swap).
  It reduces memory pressure, but the state does not survive a restart and is
  not shared between replicas.

## Open questions

* How much of hydration time is spent fetching and decoding, arranging, and
  recomputing, for representative dataflows? This bounds the benefit.
* How often do user-visible rehydrations happen, by cause (crash, OOM,
  eviction, single-replica restart)?
* Repartitioning on restore: sorting checkpoint shards by `(hash(key), key)`
  would let any worker count fetch only the hash range it owns, and remove the
  worker count from the key.
* A verification mode that restores and also recomputes, then compares the
  arrangements, to catch bugs in the write and restore paths.
