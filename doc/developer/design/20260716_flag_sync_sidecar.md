# Feature flag delivery via a per-environment sync sidecar

- Associated:
  - Self-managed file-based system parameter sync (#32317)
  - Scoped feature flags design (#36947), resolution (#36959), create-time evaluation (#37158)
  - Segments and rules in the config sync file (#38208)
  - LaunchDarkly initialization retry logic (#32030)

## The Problem

Materialize evaluates feature flags in two places, both inside the database
process and both talking to LaunchDarkly directly:

- `environmentd` runs `SystemParameterFrontend`
  (`src/adapter/src/config/frontend.rs`), which pulls system parameter values
  from the LaunchDarkly SDK and pushes changes through `ALTER SYSTEM`. The same
  frontend evaluates the `cluster` and `replica` contexts for scoped feature
  flags.
- `balancerd` runs `mz_dyncfg_launchdarkly::sync_launchdarkly_to_configset`
  (`src/dyncfg-launchdarkly`) to keep its own `ConfigSet` in sync.

`clusterd` never talks to LaunchDarkly. It receives dyncfg values from the
`environmentd` controller over the compute and storage protocols.

Self-managed deployments use a different path. `orchestratord` mounts an
operator-supplied ConfigMap at `/system-params/system-params.json` and starts
`environmentd` with `--config-sync-file-path` and a one second
`--config-sync-loop-interval`
(`src/orchestratord/src/controller/materialize/generation.rs`). `environmentd`
selects `SystemParameterFrontendClient::File`, `balancerd` selects
`mz_dyncfg_file`. With #38208 the file also carries `segments` and `rules`, so
scoped feature flags resolve from the file with the same first-match semantics
LaunchDarkly uses, including at create time.

The split causes concrete problems:

1. **Outage coupling.** A LaunchDarkly outage or a silently dead streaming
   connection (the failure class behind the `read_timeout` handling in
   `frontend.rs`) degrades the database. `environmentd` keeps its last catalog
   values, but a `balancerd` restart during an outage comes up on compiled-in
   defaults, and any SDK bug or reconnect regression ships inside the database
   binary.
2. **Two code paths, one tested.** Cloud runs the LaunchDarkly path. CI,
   sqllogictest, and self-managed run the file path. The production path has
   the least coverage.
3. **Vendor coupling inside the database.** The SDK, its transport, its
   context model, and its billing quirks are compiled into `environmentd` and
   `balancerd`. Leaving LaunchDarkly, or upgrading the SDK, is a database
   change.

## Success Criteria

- `environmentd` and `balancerd` contain no LaunchDarkly SDK code. The only
  component that speaks to LaunchDarkly is replaceable without a database
  release.
- Cloud and self-managed use the same in-database flag delivery path (the
  file frontend), so the path exercised in CI is the path running in
  production.
- A LaunchDarkly outage of any duration changes nothing about a running
  environment and nothing about an environment that restarts during the
  outage. The last synced values survive.
- Flag propagation latency (LaunchDarkly change to value applied in
  `environmentd`) stays within a few seconds, at or below today's.
- Scoped feature flags keep the semantics of #36947 and #38208, including
  synchronous create-time resolution for render-frozen flags.
- Migration is reversible at every step and can be canaried per environment.

## Out of Scope

- Changing what a feature flag is, how flags are declared (`Config`,
  `ParameterScope`), the file format from #38208, or how `clusterd` receives
  dyncfg values.
- Replacing LaunchDarkly as the flag authoring UI. This design makes such a
  replacement cheap, it does not perform it.
- GitOps or review workflows for flag changes. The ConfigMap makes them
  possible, they are a follow-up.
- Percentage rollouts or experiments targeted at `cluster` or `replica`
  contexts. #38208 supports attribute clauses only and this design inherits
  that boundary (see "Translation constraints").

## Solution Proposal

### Summary

Introduce a small sync binary (working name `flagsyncd`) that runs as a native
sidecar container in the `environmentd` pod and in `balancerd` pods. It is the
only component holding a LaunchDarkly client. It evaluates environment-wide
values for its environment, translates the cluster and replica targeting of
scoped flags into the `segments` and `rules` of #38208, and writes the result
to the JSON file the main container already reads. The database loses its
LaunchDarkly dependency and gains nothing new. Everything the database needs,
the file frontend from #32317 and #38208 already does.

```
          LaunchDarkly (streaming SDK + flag definition stream)
                    |
   +----------------|------------------ environmentd pod ----------------+
   |                v                                                    |
   |   +------------------------+   write    +------------------------+  |
   |   |  flagsyncd (sidecar)   | ---------> | emptyDir: params.json  |  |
   |   |  - env/org/build ctx   |            +-----------+------------+  |
   |   |  - env-wide values     |                        | poll 1s       |
   |   |  - LD rules -> file    |                        v               |
   |   |    segments + rules    |                 +--------------+       |
   |   +-----------+------------+                 | environmentd |       |
   |               | mirror                       | file frontend|       |
   +---------------|------------------------------+--------------+-------+
                   v
        ConfigMap (per generation, durable snapshot, seeds emptyDir on boot)
```

The sidecar knows nothing about the catalog. It never needs the live set of
clusters or replicas, because the file carries rules rather than values, and
`environmentd` evaluates those rules against the catalog exactly as it does
for an operator-written file today.

### The sync sidecar

`flagsyncd` is a new binary in this repository, built from the same source
tree and version as `environmentd` and deployed in the same pod spec. It runs
as a native sidecar (an init container with `restartPolicy: Always`), which
gives it a lifecycle independent of the main container's crashes and a defined
startup order: the sidecar starts and seeds the file, then `environmentd`
starts.

Responsibilities:

1. Hold one LaunchDarkly client for the environment, with the SDK key mounted
   from the per-environment secret `environmentd` uses today.
2. Build the same `environment`, `organization`, and `build` contexts that
   `frontend.rs::ld_ctx` builds today. The `build` context is the sidecar's
   own `BuildInfo`, which equals `environmentd`'s because they ship in one pod
   spec.
3. For every synced parameter, evaluate the environment-wide value through
   the SDK with that context, exactly as `SystemParameterFrontend::pull` does
   now. This is the flat top-level section of the file.
4. For every flag whose LaunchDarkly rules reference the `cluster` or
   `replica` context kinds, translate those rules into `segments` and `rules`
   (see below).
5. On every SDK stream event and on a slow periodic tick, write the file.
   Mirror it to a ConfigMap named per deploy generation.
6. Apply `launchdarkly_key_map` (parameter name to flag key). File keys are
   parameter names, the database never sees flag keys again.

The sidecar sends evaluation events to LaunchDarkly as the in-process SDK
does today, so flag status and context views in the LaunchDarkly UI keep
working for the environment context. Cluster and replica contexts stop
appearing there, since their evaluation moves into `environmentd` where it
never touches LaunchDarkly.

### Two files, one format: emptyDir for speed, ConfigMap for durability

The sidecar writes the file to an `emptyDir` volume shared with the main
container. `environmentd` polls its config file every second, so a flag change
lands within roughly one second of the SDK event, with no kubelet ConfigMap
propagation delay and no pod annotation tricks.

The same content is mirrored to a ConfigMap. The ConfigMap is a durable
snapshot, not the read path. On sidecar start, before the LaunchDarkly client
initializes, the sidecar copies the ConfigMap content into the `emptyDir`.
This is what makes a restart during a LaunchDarkly outage safe: the main
container starts on the last synced values, not on compiled-in defaults.

The ConfigMap is named per deploy generation. Two generations run
concurrently during a rollout and each has its own sidecar, so per-generation
naming avoids two writers on one object, and it makes `build`-context
targeting correct by construction, since each generation's sidecar evaluates
with its own build. When a new generation's ConfigMap does not exist yet the
sidecar seeds it from the previous generation's, so a rollout during an outage
still starts on known values rather than defaults.

RBAC for the sidecar's service account is `get`, `create`, `update` on
ConfigMaps in its own namespace. Nothing cluster-scoped, no pod patching.

### Translating LaunchDarkly targeting into the file

#38208 chose LaunchDarkly's clause shape (`{attribute, op, values, negate}`,
ANDed clauses, ORed values, `contextKind` on the segment) for exactly this
reason: a cloud-side translator is a near copy at the clause level. The
sidecar is that translator.

A LaunchDarkly flag evaluates as: individual targets, then rules in array
order, then fallthrough. For a flag whose rules mention `cluster` or `replica`
kinds, the sidecar walks that structure once per flag change:

- **Clauses on `environment`, `organization`, or `build`** are constants for
  this sidecar. It evaluates them against its own context. A rule with a
  false constant clause is dropped. A rule with only true constant clauses and
  no scoped clause is environment-wide and is already accounted for by the
  SDK evaluation in step 3.
- **Clauses on `cluster` or `replica`** become the clauses of a file segment.
  Attribute names and the five string operators (`in`, `startsWith`,
  `endsWith`, `contains`, `matches`) carry over unchanged. Each surviving rule
  becomes one file rule naming that segment and carrying the rule's variation
  value for the parameter.
- **Individual targets** (`contextTargets`) on `cluster` or `replica` kinds
  become a segment with an `in` clause on `cluster_id` or `replica_id`,
  emitted before the rules so they keep LaunchDarkly's precedence.
- **Fallthrough** is the environment-wide value, which is what the SDK
  evaluation in step 3 yields when no scoped kind is present in the context.
  Nothing to emit.
- **Rule order is preserved.** Both LaunchDarkly and #38208 are
  first-match-wins per parameter, so array order in the file is array order
  in the flag.

The sidecar needs the flag definitions, not just evaluations, and the SDK
does not expose them: every field of `Flag`, `Rule`, and `Clause` in
`launchdarkly-server-sdk-evaluation` is `pub(crate)`. The sidecar reads the
same streaming endpoint the SDK reads, with the same SDK key, and parses the
documented wire format (the `put` and `patch` events carrying flag and segment
JSON) into its own small `serde` structs covering just the fields above. This
is the mechanism the vendor's own Relay Proxy uses, and the format is stable
across SDK versions because every SDK depends on it. The SDK client stays for
step 3 so environment-wide evaluation is exact, including percentage rollouts
keyed on the environment.

### Translation constraints

Some LaunchDarkly targeting cannot be expressed in the file, and the sidecar
must refuse it loudly rather than approximate:

- Percentage rollouts or experiments whose bucketing context is `cluster` or
  `replica`. #38208 is attribute-only.
- Prerequisite flags that are themselves cluster- or replica-scoped.
- LaunchDarkly segments (`segmentMatch`) referencing `cluster` or `replica`
  kinds are inlined when they contain only attribute clauses and key lists.
  Segments with rollout weights or nested segment references are refused.
- Operators other than the five string ones. Every scoped attribute is a
  string, so numeric, date, and semver operators can only evaluate false.
  #38208 refuses them at parse time, the sidecar refuses them at translation
  time so the author sees it in the sidecar's metrics rather than in
  `environmentd`'s logs.

Refusal means: the offending rule is omitted, the rest of the flag translates,
a per-flag gauge `mz_flagsync_untranslatable_rules` is exported, and an alert
fires. This is the same fail-safe posture as #38208, where a segment that
cannot be evaluated matches nothing. The constraint is documented in the flag
authoring guidance as "scoped targeting is attribute clauses only", which is
already how the shipped scoped flags are written.

### Correctness check for the translator

The translator's contract is: for every live cluster and replica, the file
frontend's resolution equals what the in-process LaunchDarkly evaluation
would have produced. This is testable without a live LaunchDarkly:

- Unit tests feed hand-written flag definitions in wire format through the
  translator and through the SDK's file data source, evaluate a corpus of
  cluster and replica contexts both ways, and assert equality.
- Shadow mode (see prototype) does the same comparison in production with real
  flags and real catalogs, every tick, as a diff metric.

### Create-time resolution

The scoped design requires synchronous create-time resolution: render-frozen
flags (optimizer features baked into dataflows, replica flags read only at
render) make the next-tick window a correctness bug, not a delay. #38208
provides this for the file frontend through a parsed-file cache that the
create path evaluates against the new object's context without touching disk
or the coordinator loop. Because the file carries rules and not values, a
cluster or replica that does not exist yet still resolves correctly at
create. No sidecar involvement, no RPC, no new failure mode on the DDL path.

### `balancerd`

`balancerd` has no scoped parameters. The same sidecar binary runs next to it
in a mode that writes only the flat section, and `balancerd` reads it with
`mz_dyncfg_file` exactly as self-managed does today. `mz_dyncfg_launchdarkly`
is deleted once cloud has migrated.

### Fail-static hardening of the file readers

Two behaviors in the current file path are acceptable for an operator-managed
ConfigMap and wrong for a machine-written one:

- `sync_file_to_configset` returns `Ok(())` when the file does not exist
  (`src/dyncfg-file/src/lib.rs`). The process starts on defaults with a single
  warning.
- `SystemParameterFrontend::pull` treats an unreadable file as "no
  information" and continues on defaults.

Both readers gain a `--config-sync-required` mode used in cloud: a missing or
unparseable file at startup blocks readiness until the sidecar produces one
(native sidecar ordering makes this the normal case), and a file that stops
changing is visible through a `mz_config_sync_file_age_seconds` gauge that
replaces `last_sse_time_seconds` as the staleness alert. The sidecar exports
its own gauges for last SDK event, last successful write, last ConfigMap
mirror, and untranslatable rules.

### What happens to the kill switch and the key map

`enable_launchdarkly` (`params.rs`) currently stops the sync loop from
pulling. Its semantics become "accept externally synced values" and it keeps
working unchanged from an operator's view. Renaming it is a separate decision,
it is a persisted system parameter.

`launchdarkly_key_map` moves to the sidecar's configuration. The
`environmentd` and `balancerd` flags are removed at the end of the migration.

### Self-managed

Nothing changes for self-managed deployments. The operator writes a
ConfigMap in the #38208 format, `orchestratord` mounts it. The sidecar is not
shipped in the Helm chart by default. Offering it later as an opt-in
LaunchDarkly integration for self-managed is a chart change with no database
change, which is the point.

### Failure modes

| Situation | Behavior |
|---|---|
| LaunchDarkly unreachable | Sidecar keeps serving the last file. Nothing changes in the database. Staleness gauge rises, alert fires. |
| `environmentd` restarts during outage | Sidecar seeds `emptyDir` from ConfigMap, `environmentd` starts on last synced values. |
| New generation rolls out during outage | New sidecar seeds its ConfigMap from the previous generation's, then serves it. |
| Sidecar crashes | Native sidecar restarts it. File in `emptyDir` persists across container restarts within the pod. |
| Flag uses untranslatable scoped targeting | Rule omitted, rest of flag translated, gauge and alert. Affected objects resolve to the environment-wide value. |
| `CREATE CLUSTER`/`REPLICA` at any time | Resolves from the parsed-file cache in-process. Sidecar state irrelevant. |
| File missing or malformed at startup (cloud mode) | Readiness blocked, alert. |
| ConfigMap write fails (RBAC, API outage) | File still written to `emptyDir`, database unaffected, mirror gauge stale, alert. |

## Minimal Viable Prototype

In order, each step independently useful:

1. **Land #38208.** It is the file format and the in-database half of this
   design. Nothing here depends on anything else in `environmentd`.
2. **`flagsyncd` in shadow mode.** The binary, deployed as a sidecar, writing
   the `emptyDir` file and ConfigMap while `environmentd` still runs the
   LaunchDarkly frontend. `environmentd` additionally parses the file and
   compares, every tick, the file-resolved environment-wide and scoped values
   against its own LaunchDarkly evaluation, exported as a diff counter. Zero
   diff across a soak period on a set of environments is the exit criterion.
   Measure end-to-end latency from flag change to file write.
3. **Flip one environment.** `--config-sync-file-path` pointing at the
   `emptyDir` file and `--config-sync-required`. Reversible by flipping the
   flags back. Canary, then roll out per environment.
4. **Delete.** Remove `mz_dyncfg_launchdarkly`, the `LaunchDarkly` variant of
   `SystemParameterFrontendClient`, the `launchdarkly_*` CLI flags, and the
   SDK and transport dependencies from `environmentd` and `balancerd`.

## Alternatives

### A shared fleet controller

One controller per Kubernetes cluster that discovers environments, evaluates
flags for each, writes each environment's ConfigMap, and patches pod
annotations to force kubelet remounts. Rejected because it needs cluster-wide
RBAC including pod patching, needs an environment discovery mechanism, turns
every flag change into a fan-out write storm from one client, and puts a
single credential and a single bug in front of every environment. It also
inherits kubelet ConfigMap propagation delay, which the sidecar avoids
entirely.

### A standalone per-environment service

One Deployment per environment instead of a sidecar. Most of the sidecar's
benefits, but two `environmentd` generations share one ConfigMap during a
rollout, `build`-context targeting across generations needs explicit
handling, and the `emptyDir` fast path is unavailable. The sidecar makes all
three disappear by construction.

### Pre-materialized per-object values

Have the sidecar fetch the live clusters and replicas from `environmentd`,
evaluate each through the SDK, and write values keyed by id. This keeps
LaunchDarkly as the only rule engine and needs no translator, but a file of
values cannot contain an object that does not exist yet, so create-time
resolution for render-frozen flags would need a synchronous localhost RPC
from the coordinator to the sidecar on the DDL path, with a new failure mode
when the sidecar is down at that moment. #38208 makes this moot: rules in
the file resolve new objects in-process. The translator is a bounded amount
of code, the RPC is a permanent runtime dependency.

### LaunchDarkly Relay Proxy

Run the vendor's relay in the cluster and keep the SDK in the database.
Improves outage resilience for connection establishment, nothing else. The
SDK, its bugs, and the vendor coupling stay inside the database, self-managed
stays on a different path, and leaving the vendor remains a database change.

### Sidecar writing only the `emptyDir`

Drop the ConfigMap mirror. Simpler, but a pod restart during an outage would
start on compiled-in defaults, the exact failure this design removes.

### Keep the SDK in-process, add a file cache

`environmentd` keeps evaluating LaunchDarkly and additionally writes a
ConfigMap as a fallback. Solves the restart-during-outage case only. Every
other success criterion fails.

## Open questions

- **Flag definition source.** The design reads the streaming wire format
  directly. The alternative is the REST API with an API token, which is
  polling, rate-limited across a fleet of sidecars, and a second credential.
  A third is a fork of `launchdarkly-server-sdk-evaluation` exposing the
  fields, which this repository has done before and moved away from. Confirm
  the wire format approach against the SDK contract tests before building.
- **Scoped targeting audit.** Enumerate live flags with `cluster` or
  `replica` rules and confirm every one is attribute clauses only. Any that
  is not either gets rewritten or blocks the flip for its environment.
- **Build-context targeting audit.** Do any live flags target `build` sha or
  semver? Per-generation ConfigMaps make evaluation correct regardless, but
  the answer decides whether the seed-from-previous-generation step may serve
  values wrong for the new build until the SDK initializes.
- **Cloud pod spec ownership.** The `environmentd` pod spec in cloud is
  rendered outside this repository. The sidecar, `emptyDir`, ConfigMap name,
  and the new flags need a coordinated change there. Self-managed rendering
  lives in `orchestratord` and is a normal change here.
- **Kubernetes version floor.** Native sidecars need 1.29 for GA. Confirm the
  fleet floor. A regular container with a readiness gate is the fallback and
  loses only startup ordering.
- **`enable_launchdarkly` rename.** Keep the name for compatibility or
  rename with a migration. Not blocking.
