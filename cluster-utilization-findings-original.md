# Cluster utilization overview: maintained views vs a single 1-min base index

Exploration of how to back the Console's cluster-detail utilization graphs.
All numbers from a local `environmentd` on a dedicated `scale=1,workers=1`
cluster (matching `mz_catalog_server`), synthetic fleet, 14 days of metrics at
a realistic 1/min scrape. Memory = sum of maintained arrangement bytes
(`mz_internal.mz_object_arrangement_sizes`). Test scripts in this directory:
`measure_cost.py`, `measure_base.py`, `diag_rebin.py`, `bench_driver.py`.

## The two designs

- **A (current / shipped):** three maintained indexed views — `_3h` (1-min),
  `_24h` (5-min), `_overview` (1-hour/14d). Each pre-computes five per-bucket
  arg-max top-1s. The Console reads the finest covering view as an indexed
  point lookup on `cluster_id`.
- **B (proposed):** one maintained 1-min/14d **base** view + index. At a 1/min
  scrape each 1-min bucket holds exactly one sample, so the five top-1s collapse
  to identity and vanish from the maintained dataflow. Coarser timeframes
  (24h/14d) re-bin at query time (unmaterialized view or frontend).

## Scaling: A (14d/1h view alone) vs B (1-min base)

| replicas | design | hydrate | arrangement memory | single-user query p50 |
|---|---|---|---|---|
| 50  | A | 33.9 s  | 1100 MB | 124 ms |
| 50  | B | 20.4 s  | 1220 MB | 332 ms (re-bin) |
| 100 | A | 67.5 s  | 2690 MB | 155 ms |
| 100 | B | 40.6 s  | 2250 MB | 330 ms |
| 200 | A | 139.5 s | 6670 MB | 192 ms |
| 200 | B | 84.5 s  | 4760 MB | 320 ms |

Note A is *only the 14d view*. B is one index that **subsumes all three**
current views, so the fair memory comparison at 200 replicas is B (4760 MB) vs
the current 3-view total (~7300 MB = 6670 + ~520 + ~80).

## Findings

1. **Memory crosses over in B's favor as the fleet grows.** A's five top-1s +
   6-way join grow super-linearly (~2.45x per doubling); B grows ~linearly. B
   is +11% at 50 replicas but -29% (vs the 14d view alone) / ~-35% (vs all
   three views) at 200.
2. **B hydrates ~40% faster** at every scale (no top-1 hierarchies to build).
3. **The re-bin must push `cluster_id` into the index lookup.** First cut put
   the cluster filter *after* the top-1, so the re-bin ran over the whole fleet
   and took **7.1 s** (`EXPLAIN`: `base_1min_idx (*** full scan ***)`). Making
   `cluster_id` the **leading `DISTINCT ON` key** turns it into
   `base_1min_idx (lookup)` -> **~330 ms, and flat across fleet size** (it only
   touches one cluster's ~40K rows). This is the same non-pushdown trap as the
   original Console query.
4. **B's single-user query is ~2x A's** (330 ms vs 124-192 ms) but stays flat
   while A's grows slightly with fleet size. The raw full-resolution lookup
   (for frontend-side re-binning) returns ~40K rows/query (~500 ms) vs 672 for
   A; the server-side re-bin view returns 674 rows.

## Open question: behavior under concurrency (esp. SUBSCRIBE)

Single-user, B looks like a win (less memory at scale, faster hydration, one
index, sub-second queries). The deciding question is concurrency, and the
intended use is **SUBSCRIBE**.

Key constraint: **a SUBSCRIBE cannot share a dataflow/arrangement unless it is
maintained by a catalog object (index or materialized view).**

- **A (maintained views):** N concurrent SUBSCRIBEs all read the *one* shared
  maintained arrangement. Per-subscriber cost is a trivial filter; memory and
  hydration should stay ~flat as N grows.
- **B (unmaterialized re-bin):** each SUBSCRIBE installs its *own* re-bin
  dataflow (the base index is shared, but the per-cluster top-1 re-bin on top
  is not). N viewers -> N independent re-bin dataflows maintained continuously.
  Expected to grow with N.

So B trades steady-state memory/hydration for per-subscriber compute that does
not share. Whether that is acceptable depends on how many concurrent
subscribers a replica must serve.

## Concurrency result (SUBSCRIBE, 100 replicas / 50 clusters)

N concurrent SUBSCRIBEs, distinct clusters, initial-snapshot latency +
cluster-wide arrangement-memory growth (`subscribe_concurrency.py`):

| N | A maintained: snap p50 / mem Δ | B base+re-bin: snap p50 / mem Δ |
|---|---|---|
| 1  | 27 ms / flat  | 611 ms / flat |
| 4  | 90 ms / flat  | 2.3 s / flat |
| 16 | 168 ms / flat | 9.8 s / +258 MB |
| 32 | 230 ms / flat | 18.9 s / +628 MB |

(A's mem deltas are small negatives = noise/compaction around a flat baseline.)

**A stays cheap and flat; B collapses.** N SUBSCRIBEs to the maintained view
share the one arrangement (sub-300 ms snapshots, flat memory at N=32). N
SUBSCRIBEs to the unmaterialized re-bin each build their own dataflow, serialize
on the worker, and grow to 18.9 s snapshots (82x worse) and +628 MB at N=32.

## Variant: 1-min base + a 1-hour re-bin built ON the base (both maintained)

Idea: keep the cheaper 1-min base, and for the >=24h range maintain one 1-hour
view derived from it (so the console subscribes to minute data for <24h and
hour data for >=24h, both shared arrangements, re-binning client-side for
display). `measure_chain.py`:

Memory, MEASURED (not estimated) -- the three shipped views include the name
lateral + offline agg; the chain numbers are lean (no name/offline, which a
1-min base would not bake in anyway -- it fetches those separately):

| design | mem @100 | mem @200 |
|---|---|---|
| current 3 views, measured (3h+24h+14d) | 3430 MB | 7230 MB |
|   - 14d view alone (full rollup) | 3290 | 6730 |
| chain: base + 1h-from-base (lean) | 3460 MB | 7080 MB |
|   - base_1min alone | 2290 | 4690 |
|   - hour-from-base increment | 1170 | 2390 |

(An earlier draft compared against an under-counted ~2840 MB estimate for the
current design and wrongly concluded the chain was +22%. Measured, it is a wash:
even at 100 replicas, slightly cheaper at 200.)

- The 1h view **does reuse the base** (1170 MB increment vs a 2690 MB lean
  standalone 1h at 100 replicas) -- confirmed by the memory delta.
- Measured against the real three-view total, **the chain is a wash**: even at
  100 replicas (3460 vs 3430), ~2% cheaper at 200 (7080 vs 7230). The current
  14d view grows ~2.45x/doubling vs the chain's ~2.05x, so the chain gets
  relatively cheaper as the fleet grows. The chain's lean numbers also omit
  name/offline, which it would fetch separately (cheap) rather than bake into a
  1-min base, whereas the current design pays name/offline ~3x. Both designs are
  SUBSCRIBE-safe (all arrangements maintained).

So memory/hydration don't pick a winner. The chain's only real upside is
**1-minute resolution for <24h** (vs the current 5-min 24h view) and 2 objects
instead of 3, at the cost of client-side re-binning and ~5x snapshot payload
for <24h ranges.

## Conclusion

**Keep the shipped design: three maintained indexed views.** The 1-min base is a
better single-user storage shape (less memory at scale, ~40% faster hydration,
one index), but the coarse rollups must stay materialized: to serve many
concurrent SUBSCRIBEs cheaply the top-1 re-bin has to be maintained once in a
shared object (index/MV). Deferring it pays the cost per subscriber with no
sharing. This exploration strengthens design A rather than replacing it.
