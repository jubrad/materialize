// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Service orchestration via Kubernetes CRDs.
//!
//! This crate provides an implementation of the [`Orchestrator`] trait that
//! creates and manages services through custom resource definitions (CRDs)
//! rather than directly managing StatefulSets. The actual Kubernetes resource
//! management is delegated to orchestratord, which watches the CRDs and
//! creates/updates the underlying StatefulSets and Services.
//!
//! This architecture has several benefits:
//!
//! - **Reduced RBAC scope**: environmentd only needs permissions to manage
//!   the CRDs, not the underlying StatefulSets/Services/Pods
//! - **Centralized resource management**: orchestratord handles all the
//!   complex Kubernetes API interactions
//! - **Declarative interface**: The CRDs represent the full desired state,
//!   making debugging and recovery easier

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZero;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use kube::api::{DeleteParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::runtime::watcher::{self, Event};
use kube::{Api, Client, Resource, ResourceExt};
use tracing::{debug, trace, warn};

use mz_cloud_resources::crd::materialize_cluster_replica::v1alpha1::{
    ClusterLabelSelector, ClusterLabelSelectorOperator, ClusterPort, MaterializeClusterReplica,
    MaterializeClusterReplicaSpec, ReplicaPhase,
};
use mz_orchestrator::{
    LabelSelectionLogic, LabelSelector, NamespacedOrchestrator, Orchestrator, Service,
    ServiceAssignments, ServiceConfig, ServiceEvent, ServiceProcessMetrics, ServiceStatus,
    scheduling_config::ServiceSchedulingConfig,
};

/// Field manager name for server-side apply.
const FIELD_MANAGER: &str = "environmentd.materialize.cloud";

/// Configuration for the CRD-based Kubernetes orchestrator.
#[derive(Clone, Debug)]
pub struct KubernetesCrdOrchestratorConfig {
    /// The parent cluster reference to use for all replicas.
    /// This should be the name of the MaterializeCluster CRD.
    pub cluster_ref: String,

    /// The environmentd generation, used for tracking which generation
    /// created the replicas.
    pub generation: u64,
}

/// A Kubernetes orchestrator that manages services through CRDs.
///
/// This orchestrator creates [`MaterializeClusterReplica`] CRDs instead of
/// directly creating StatefulSets. The orchestratord controller watches these
/// CRDs and creates the underlying Kubernetes resources.
#[derive(Debug)]
pub struct KubernetesCrdOrchestrator {
    client: Client,
    config: KubernetesCrdOrchestratorConfig,
}

impl KubernetesCrdOrchestrator {
    /// Creates a new CRD-based Kubernetes orchestrator.
    pub fn new(client: Client, config: KubernetesCrdOrchestratorConfig) -> Self {
        Self { client, config }
    }
}

impl Orchestrator for KubernetesCrdOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        Arc::new(NamespacedKubernetesCrdOrchestrator {
            client: self.client.clone(),
            namespace: namespace.to_string(),
            config: self.config.clone(),
            scheduling_config: Default::default(),
        })
    }
}

/// A namespaced CRD-based Kubernetes orchestrator.
struct NamespacedKubernetesCrdOrchestrator {
    client: Client,
    namespace: String,
    config: KubernetesCrdOrchestratorConfig,
    scheduling_config: std::sync::RwLock<ServiceSchedulingConfig>,
}

impl fmt::Debug for NamespacedKubernetesCrdOrchestrator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespacedKubernetesCrdOrchestrator")
            .field("namespace", &self.namespace)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl NamespacedKubernetesCrdOrchestrator {
    /// Generates the CRD name for a service.
    fn replica_name(&self, id: &str) -> String {
        // Include generation to support zero-downtime upgrades
        format!("{}-gen-{}", id, self.config.generation)
    }

    /// Converts a LabelSelector to a ClusterLabelSelector.
    fn convert_label_selector(&self, selector: &LabelSelector) -> ClusterLabelSelector {
        let (operator, values) = match &selector.logic {
            LabelSelectionLogic::Eq { value } => {
                (ClusterLabelSelectorOperator::In, vec![value.clone()])
            }
            LabelSelectionLogic::NotEq { value } => {
                (ClusterLabelSelectorOperator::NotIn, vec![value.clone()])
            }
            LabelSelectionLogic::Exists => (ClusterLabelSelectorOperator::Exists, vec![]),
            LabelSelectionLogic::NotExists => (ClusterLabelSelectorOperator::DoesNotExist, vec![]),
            LabelSelectionLogic::InSet { values } => {
                (ClusterLabelSelectorOperator::In, values.clone())
            }
            LabelSelectionLogic::NotInSet { values } => {
                (ClusterLabelSelectorOperator::NotIn, values.clone())
            }
        };

        ClusterLabelSelector {
            key: selector.label_name.clone(),
            operator,
            values,
        }
    }

    /// Builds a MaterializeClusterReplicaSpec from a ServiceConfig.
    fn build_replica_spec(
        &self,
        id: &str,
        config: &ServiceConfig,
    ) -> MaterializeClusterReplicaSpec {
        // Compute arguments using deterministic addresses
        let scale = config.scale.get();
        let replica_name = self.replica_name(id);

        // Build listen addresses (0.0.0.0:port)
        let listen_addrs: BTreeMap<String, String> = config
            .ports
            .iter()
            .map(|p| (p.name.clone(), format!("0.0.0.0:{}", p.port_hint)))
            .collect();

        // Build peer addresses
        // Note: We can't know the actual service name until the CRD is created
        // and status is populated, but we can compute deterministic addresses
        // based on the expected naming pattern.
        let peer_addrs: Vec<BTreeMap<String, String>> = (0..scale)
            .map(|i| {
                config
                    .ports
                    .iter()
                    .map(|p| {
                        // This address pattern matches what the controller will create
                        let host = format!(
                            "{replica_name}-{i}.{replica_name}.{}.svc.cluster.local",
                            self.namespace
                        );
                        (p.name.clone(), format!("{host}:{}", p.port_hint))
                    })
                    .collect()
            })
            .collect();

        // Generate arguments
        let args = (config.args)(ServiceAssignments {
            listen_addrs: &listen_addrs,
            peer_addrs: &peer_addrs,
        });

        // Convert ports
        let ports: Vec<ClusterPort> = config
            .ports
            .iter()
            .map(|p| ClusterPort {
                name: p.name.clone(),
                port: p.port_hint,
            })
            .collect();

        // Convert label selectors
        let other_replicas_selector: Vec<ClusterLabelSelector> = config
            .other_replicas_selector
            .iter()
            .map(|s| self.convert_label_selector(s))
            .collect();

        let replicas_selector: Vec<ClusterLabelSelector> = config
            .replicas_selector
            .iter()
            .map(|s| self.convert_label_selector(s))
            .collect();

        MaterializeClusterReplicaSpec {
            cluster_ref: self.config.cluster_ref.clone(),
            replica_id: id.to_string(),
            replica_name: id.to_string(),
            image: config.image.clone(),
            init_container_image: config.init_container_image.clone(),
            args,
            ports,
            cpu_limit: config.cpu_limit.map(|c| format!("{}m", c.as_millicpus())),
            cpu_request: config.cpu_request.map(|c| format!("{}m", c.as_millicpus())),
            memory_limit: config.memory_limit.map(|m| m.0.to_string()),
            memory_request: config.memory_request.map(|m| m.0.to_string()),
            disk_limit: config.disk_limit.map(|d| d.0.to_string()),
            scale,
            availability_zones: config.availability_zones.clone(),
            node_selector: config.node_selector.clone(),
            labels: config.labels.clone(),
            annotations: config.annotations.clone(),
            other_replicas_selector,
            replicas_selector,
            disk: config.disk_limit.is_some(),
        }
    }

    /// Computes addresses for a service based on its spec.
    fn compute_addresses(
        &self,
        spec: &MaterializeClusterReplicaSpec,
        replica_name: &str,
    ) -> BTreeMap<String, Vec<String>> {
        let scale = spec.scale.max(1);
        let hosts: Vec<String> = (0..scale)
            .map(|i| {
                format!(
                    "{replica_name}-{i}.{replica_name}.{}.svc.cluster.local",
                    self.namespace
                )
            })
            .collect();

        let mut addresses = BTreeMap::new();
        for port in &spec.ports {
            let port_addrs: Vec<String> =
                hosts.iter().map(|h| format!("{h}:{}", port.port)).collect();
            addresses.insert(port.name.clone(), port_addrs);
        }
        addresses
    }
}

/// A service backed by a MaterializeClusterReplica CRD.
#[derive(Debug)]
struct CrdService {
    addresses: BTreeMap<String, Vec<String>>,
}

impl Service for CrdService {
    fn addresses(&self, port: &str) -> Vec<String> {
        self.addresses
            .get(port)
            .cloned()
            .unwrap_or_else(|| panic!("unknown port: {port}"))
    }
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedKubernetesCrdOrchestrator {
    fn ensure_service(
        &self,
        id: &str,
        config: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let replica_name = self.replica_name(id);
        let spec = self.build_replica_spec(id, &config);

        // Compute addresses deterministically from spec
        let addresses = self.compute_addresses(&spec, &replica_name);

        // Build the CRD
        let replica = MaterializeClusterReplica {
            metadata: ObjectMeta {
                name: Some(replica_name.clone()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            spec,
            status: None,
        };

        // Spawn a task to create/update the CRD
        // We use server-side apply to handle concurrent updates gracefully
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        mz_ore::task::spawn(|| "ensure_service_crd", async move {
            let api: Api<MaterializeClusterReplica> = Api::namespaced(client, &namespace);
            match api
                .patch(
                    &replica_name,
                    &PatchParams::apply(FIELD_MANAGER).force(),
                    &Patch::Apply(&replica),
                )
                .await
            {
                Ok(_) => {
                    trace!(
                        replica_name,
                        "created/updated MaterializeClusterReplica CRD"
                    );
                }
                Err(e) => {
                    warn!(
                        replica_name,
                        error = %e,
                        "failed to create/update MaterializeClusterReplica CRD"
                    );
                }
            }
        });

        Ok(Box::new(CrdService { addresses }))
    }

    fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        let replica_name = self.replica_name(id);
        let client = self.client.clone();
        let namespace = self.namespace.clone();

        mz_ore::task::spawn(|| "drop_service_crd", async move {
            let api: Api<MaterializeClusterReplica> = Api::namespaced(client, &namespace);
            match api.delete(&replica_name, &DeleteParams::default()).await {
                Ok(_) => {
                    debug!(replica_name, "deleted MaterializeClusterReplica CRD");
                }
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    // Already deleted, that's fine
                    trace!(
                        replica_name,
                        "MaterializeClusterReplica CRD already deleted"
                    );
                }
                Err(e) => {
                    warn!(
                        replica_name,
                        error = %e,
                        "failed to delete MaterializeClusterReplica CRD"
                    );
                }
            }
        });

        Ok(())
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let api: Api<MaterializeClusterReplica> =
            Api::namespaced(self.client.clone(), &self.namespace);

        // List all replicas for this cluster and generation
        let label_selector = format!(
            "materialize.cloud/cluster-ref={},materialize.cloud/replica-id",
            self.config.cluster_ref
        );

        let replicas = api
            .list(&kube::api::ListParams::default().labels(&label_selector))
            .await?;

        // Filter by generation (included in the name)
        let gen_suffix = format!("-gen-{}", self.config.generation);
        let services: Vec<String> = replicas
            .items
            .into_iter()
            .filter_map(|r| {
                let name = r.name_unchecked();
                if name.ends_with(&gen_suffix) {
                    // Extract the service ID by removing the generation suffix
                    Some(name[..name.len() - gen_suffix.len()].to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(services)
    }

    fn watch_services(&self) -> BoxStream<'static, Result<ServiceEvent, anyhow::Error>> {
        let api: Api<MaterializeClusterReplica> =
            Api::namespaced(self.client.clone(), &self.namespace);
        let cluster_ref = self.config.cluster_ref.clone();
        let generation = self.config.generation;
        let gen_suffix = format!("-gen-{}", generation);

        // Watch all replicas for this cluster
        let label_selector = format!("materialize.cloud/cluster-ref={}", cluster_ref);
        let watcher_config = watcher::Config::default().labels(&label_selector);

        watcher::watcher(api, watcher_config)
            .filter_map(move |event| {
                let gen_suffix = gen_suffix.clone();
                async move {
                    match event {
                        Ok(Event::Applied(replica)) => {
                            let name = replica.name_unchecked();
                            // Only process replicas from this generation
                            if !name.ends_with(&gen_suffix) {
                                return None;
                            }

                            let service_id = name[..name.len() - gen_suffix.len()].to_string();

                            // Determine status from replica phase
                            let status = replica
                                .status
                                .as_ref()
                                .map(|s| match s.phase {
                                    ReplicaPhase::Running => ServiceStatus::Online,
                                    ReplicaPhase::Failed => ServiceStatus::Offline(Some(
                                        mz_orchestrator::OfflineReason::OomKilled,
                                    )),
                                    _ => ServiceStatus::Offline(Some(
                                        mz_orchestrator::OfflineReason::Initializing,
                                    )),
                                })
                                .unwrap_or(ServiceStatus::Offline(Some(
                                    mz_orchestrator::OfflineReason::Initializing,
                                )));

                            // For now, we emit events for process 0 only
                            // TODO: Handle multi-process replicas
                            Some(Ok(ServiceEvent {
                                service_id,
                                process_id: 0,
                                status,
                                time: Utc::now(),
                            }))
                        }
                        Ok(Event::Deleted(replica)) => {
                            let name = replica.name_unchecked();
                            if !name.ends_with(&gen_suffix) {
                                return None;
                            }

                            let service_id = name[..name.len() - gen_suffix.len()].to_string();
                            Some(Ok(ServiceEvent {
                                service_id,
                                process_id: 0,
                                status: ServiceStatus::Offline(None),
                                time: Utc::now(),
                            }))
                        }
                        Ok(Event::Restarted(_)) => None,
                        Err(e) => Some(Err(anyhow::anyhow!("watch error: {}", e))),
                    }
                }
            })
            .boxed()
    }

    async fn fetch_service_metrics(
        &self,
        _id: &str,
    ) -> Result<Vec<ServiceProcessMetrics>, anyhow::Error> {
        // TODO: Implement metrics collection
        // For now, return empty metrics
        // The actual implementation would query the pods directly or
        // read from CRD status if we decide to populate metrics there
        Ok(vec![ServiceProcessMetrics::default()])
    }

    fn update_scheduling_config(&self, config: ServiceSchedulingConfig) {
        *self.scheduling_config.write().unwrap() = config;
    }
}
