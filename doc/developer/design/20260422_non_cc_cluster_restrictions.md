# Restrict Compute Objects to CC-Sized Clusters

- Associated: (TBD — link to tracking issue)

## The Problem

Materialize supports two families of cluster replica sizes:

- **Modern "cc" sizes** (e.g., `25cc`, `50cc`, `100cc`): billed in compute
  credits, with modern runtime defaults (disk always enabled, tuned memory
  management, persist configuration optimized for cc workloads).
- **Legacy T-shirt sizes** (e.g., `xsmall`, `small`, `medium`): an older
  billing model with different runtime characteristics.

Today, any cluster can host any type of object — sources, materialized views,
indexes, sinks, and continual tasks — regardless of its replica size family.
This creates problems:

1. **Operational risk**: Legacy-sized clusters may not have the runtime
   configuration (disk, memory limits, persist tuning) expected by compute
   objects. Customers on legacy sizes who create materialized views or indexes
   may experience degraded performance or unexpected behavior that is difficult
   to diagnose.

2. **Billing confusion**: The cc sizing model is designed to make compute costs
   predictable. Allowing compute objects on legacy clusters undermines this by
   mixing billing models within a single deployment.

3. **Migration friction**: To deprecate legacy sizes, we need a mechanism to
   prevent new compute workloads from being installed on them, while still
   allowing existing source-only workloads to continue operating.

The goal is to enforce that materialized views, indexes, and sinks can only be
created on clusters whose replicas all use cc sizes.

## Success Criteria

1. When the feature is enabled, `CREATE MATERIALIZED VIEW`, `CREATE INDEX`, and
   `CREATE SINK` fail with a clear error if the target cluster has any non-cc
   replica or a non-cc managed size.

2. When the feature is enabled, adding a non-cc replica to a cluster that
   already contains materialized views, indexes, or sinks fails with a clear
   error. Similarly, `ALTER CLUSTER ... SET (SIZE ...)` to a non-cc size fails
   if the cluster has compute objects.

3. Sources continue to work on non-cc clusters without restriction.

4. The feature is gated behind a dynamic configuration flag
   (`enable_cc_cluster_check`) so it can be rolled out incrementally across
   environments without requiring a deployment.

5. `EXPLAIN CREATE MATERIALIZED VIEW` and `EXPLAIN CREATE INDEX` are not
   blocked (they are read-only operations).

6. Existing clusters with compute objects on non-cc sizes continue to function
   — the restriction only applies to new DDL operations.

## Out of Scope

- **Migrating existing compute objects off legacy clusters.** This design only
  prevents new installations. A separate migration plan would handle existing
  deployments.
- **Restricting `ContinualTask`.** While continual tasks have a `cluster_id`,
  they are excluded from this restriction for now.
- **Removing legacy sizes entirely.** This is a prerequisite step toward
  deprecation, not the deprecation itself.
- **Validating at the SQL planning layer.** The enforcement is in the
  coordinator's sequencer, not in the SQL planner. This is a deliberate
  choice — see Alternatives.

## Solution Proposal

### Overview

Add a bidirectional enforcement in the coordinator's sequencer layer, gated
behind a `ENABLE_CC_CLUSTER_CHECK` dynamic configuration flag (default: off).

**Direction 1 — Block compute objects on non-cc clusters:**

Before creating a materialized view, index, or sink, check whether the target
cluster is cc-compatible. A cluster is non-cc if:
- It is a managed cluster whose declared size maps to `is_cc: false`, OR
- Any of its replicas has `is_cc: false` in its allocation, OR
- Any of its replicas is unmanaged (which has no cc designation).

**Direction 2 — Block non-cc replicas on clusters with compute objects:**

Before adding a replica or changing a managed cluster's size, check whether the
cluster has any bound materialized views, indexes, or sinks. If it does, reject
the operation if the new replica/size is non-cc.

### Enforcement points

| Operation | File | Function |
|---|---|---|
| `CREATE MATERIALIZED VIEW` | `inner/create_materialized_view.rs` | `create_materialized_view_validate()` |
| `CREATE INDEX` | `inner/create_index.rs` | `create_index_validate()` |
| `CREATE SINK` | `inner.rs` | `sequence_create_sink()` |
| `CREATE CLUSTER REPLICA` | `inner/cluster.rs` | `sequence_create_cluster_replica()` |
| `ALTER CLUSTER ... SET (SIZE)` | `inner/cluster.rs` | `sequence_alter_cluster_managed_to_managed()` |

### Helper methods

Two helper methods on `Coordinator` centralize the logic:

- **`check_cluster_non_cc(cluster_id) -> Option<String>`**: Returns the cluster
  name if the cluster is non-cc (checking both the managed size and all
  replicas). Returns `None` if the cluster is cc-compatible or the feature flag
  is off.

- **`cluster_has_compute_objects(cluster_id) -> bool`**: Returns true if any
  of the cluster's `bound_objects` is a `MaterializedView`, `Index`, or `Sink`.

### Feature flag

The check is gated behind `ENABLE_CC_CLUSTER_CHECK`, a boolean dyncfg
(default: `false`). It is settable at runtime via:

```sql
ALTER SYSTEM SET enable_cc_cluster_check = true;
```

This allows incremental rollout via LaunchDarkly or per-environment
configuration without redeployment.

### Error messages

Errors follow the Materialize convention of lowercase, factual, no trailing
punctuation:

- `cannot create materialized view on cluster '{name}' as it is not using a cc
  cluster size`
- `cannot create index on cluster '{name}' as it is not using a cc cluster
  size`
- `cannot create sink on cluster '{name}' as it is not using a cc cluster size`
- `cannot add a non-cc replica to cluster '{name}' as it contains materialized
  views, indexes, or sinks`
- `cannot change cluster '{name}' to a non-cc size as it contains materialized
  views, indexes, or sinks`

Errors should use a structured `AdapterError` variant (not `Unstructured`) with
`SqlState::FEATURE_NOT_SUPPORTED`.

### Test size

A new `"1cc"` size with `is_cc: true` is added to `ClusterReplicaSizeMap::for_tests()`
so that SLT tests can create cc clusters.

### Test plan

A new SLT file (`test/sqllogictest/non_cc_cluster_restrictions.slt`) covers:

1. Enable the flag via `ALTER SYSTEM SET`.
2. `CREATE MATERIALIZED VIEW` on a non-cc cluster → error.
3. `CREATE INDEX` on a non-cc cluster → error.
4. `CREATE MATERIALIZED VIEW` on a cc cluster → success.
5. `CREATE INDEX` on a cc cluster → success.
6. `ALTER CLUSTER` a cc cluster with compute objects to a non-cc size → error.
7. `ALTER CLUSTER` a non-cc cluster without compute objects to another non-cc
   size → success.
8. Verify that objects on the cc cluster return correct data.

Additional test coverage needed:
- `CREATE CLUSTER REPLICA` with a non-cc size on a cluster with compute objects
  → error.
- `EXPLAIN CREATE MATERIALIZED VIEW` on a non-cc cluster → success (not
  blocked).

## Alternatives

### Enforce in the SQL planner (`ddl.rs`) instead of the sequencer

The SQL planner already has access to `is_cluster_size_cc()` via the
`SessionCatalog` trait, and uses it for the `DISK` option validation. We could
add the check there.

**Pros:** Catches errors earlier with cleaner `sql_bail!()` errors. Consistent
with the `DISK` validation pattern.

**Cons:** The planner operates on the plan, not the live catalog state. For
managed clusters with 0 replicas, the planner knows the size but not the
replica state. For unmanaged clusters, the planner doesn't have direct access
to replica allocations — it would need a new `SessionCatalog` method. Also,
dyncfg access is less natural in the planner.

**Decision:** Sequencer enforcement was chosen because it has direct access to
both the live catalog (cluster replicas, bound objects) and dyncfgs, and
because the bidirectional check (Direction 2) must live in the sequencer
anyway.

### Use a session variable instead of a dyncfg

A session variable (like `unsafe_enable_...`) could gate the check per-session.

**Pros:** More granular control; individual sessions could opt in.

**Cons:** The goal is an environment-wide policy, not per-session opt-in. A
dyncfg is the right mechanism for a policy that should apply uniformly.

### Block at `CREATE CLUSTER` time instead of object creation time

Prevent creating clusters with non-cc sizes entirely when the flag is on.

**Pros:** Simpler — one enforcement point.

**Cons:** Breaks existing non-cc clusters used for sources. The goal is to
restrict what can *run* on non-cc clusters, not to eliminate them.

### Include `ContinualTask` in the restriction

Continual tasks have a `cluster_id` and are compute-like.

**Decision:** Excluded for now to limit scope. Continual tasks are a newer
feature with a smaller footprint and can be added to the restriction later if
needed.

## Open Questions

1. **Should `EXPLAIN CREATE ...` be blocked?** The current prototype blocks it
   because the validate functions are shared with the explain path. The
   recommendation is to skip the check when `explain_ctx` is not
   `ExplainContext::None`, since `EXPLAIN` is read-only.

2. **What about the `quickstart` cluster in production?** If `quickstart` uses
   a cc size in production, this is a non-issue. If it uses a legacy size, enabling
   the flag would prevent users from creating MVs on the default cluster.
   The rollout plan should ensure `quickstart` is cc-sized before enabling the
   flag.

3. **Should system clusters be exempt?** System clusters (`mz_system`,
   `mz_catalog_server`) use non-cc sizes in test environments and have internal
   indexes. The current implementation applies the check to all clusters, but
   since system indexes are created during bootstrap (not through the
   sequencer), the check only fires for user-initiated DDL on system clusters.
   If users need to create indexes on system clusters with non-cc sizes, an
   exemption for `cluster_id.is_system()` should be added.

4. **Should the error use a structured `AdapterError` variant?** The prototype
   uses `AdapterError::Unstructured(anyhow!(...))`, which maps to
   `SqlState::INTERNAL_ERROR`. A dedicated variant would give
   `FEATURE_NOT_SUPPORTED` and cleaner error structure. This should be addressed
   before merging.
