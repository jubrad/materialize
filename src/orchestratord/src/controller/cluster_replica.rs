// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Controller for MaterializeClusterReplica resources.
//!
//! This controller reconciles MaterializeClusterReplica CRDs into Kubernetes
//! StatefulSets and Services. It handles:
//!
//! - Creating headless Services for DNS-based discovery
//! - Creating StatefulSets with proper scheduling configuration
//! - Updating CRD status based on pod states
//!
//! The controller is designed to be a drop-in replacement for the direct
//! StatefulSet management in orchestrator-kubernetes, with the key difference
//! being that the CRD spec contains all the information needed to generate
//! the Kubernetes resources.

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, ContainerPort, PodSecurityContext, PodSpec, PodTemplateSpec,
    ResourceRequirements, SeccompProfile, SecurityContext, Service, ServicePort, ServiceSpec,
    Toleration,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector, Time};
use k8s_openapi::jiff::Timestamp;
use kube::api::{ObjectMeta, PostParams};
use kube::runtime::controller::Action;
use kube::runtime::reflector::{ObjectRef, Store};
use kube::{Api, Client, Resource, ResourceExt};
use maplit::btreemap;
use sha2::{Digest, Sha256};
use tracing::{instrument, trace};

use crate::Error;
use crate::k8s::{apply_resource, make_reflector};
use mz_cloud_resources::crd::ManagedResource;
use mz_cloud_resources::crd::materialize_cluster_replica::v1alpha1::{
    ClusterLabelSelector, ClusterLabelSelectorOperator, MaterializeClusterReplica,
    MaterializeClusterReplicaStatus, ReplicaPhase,
};
use mz_orchestrator_kubernetes::KubernetesImagePullPolicy;

/// Annotation key for the pod template hash.
/// Used to detect when pods need to be recreated.
const POD_TEMPLATE_HASH_ANNOTATION: &str = "environmentd.materialize.cloud/pod-template-hash";

/// Toleration seconds for node failure conditions.
const NODE_FAILURE_THRESHOLD_SECONDS: i64 = 30;

/// Configuration for the MaterializeClusterReplica controller.
pub struct Config {
    /// Whether to enable security context for pods.
    pub enable_security_context: bool,

    /// Whether to enable Prometheus scrape annotations.
    pub enable_prometheus_scrape_annotations: bool,

    /// Image pull policy for containers.
    pub image_pull_policy: KubernetesImagePullPolicy,

    /// Optional scheduler name to use.
    pub scheduler_name: Option<String>,

    /// Optional ephemeral volume storage class for scratch disks.
    pub ephemeral_volume_storage_class: Option<String>,

    /// Optional fs_group for pod security context.
    pub service_fs_group: Option<i64>,

    /// Optional service account to use.
    pub service_account: Option<String>,
}

/// Controller context for MaterializeClusterReplica resources.
pub struct Context {
    config: Config,
    statefulsets: Store<StatefulSet>,
}

impl Context {
    /// Creates a new controller context.
    pub async fn new(config: Config, client: Client) -> Self {
        Self {
            config,
            statefulsets: make_reflector(client).await,
        }
    }

    /// Builds a headless Service for the replica.
    fn build_service(&self, replica: &MaterializeClusterReplica) -> Service {
        let service_name = replica.service_name();
        let match_labels = btreemap! {
            "materialize.cloud/name".to_string() => service_name.clone(),
        };

        let ports: Vec<ServicePort> = replica
            .spec
            .ports
            .iter()
            .map(|port| ServicePort {
                name: Some(port.name.clone()),
                port: port.port.into(),
                ..Default::default()
            })
            .collect();

        Service {
            metadata: replica.managed_resource_meta(service_name),
            spec: Some(ServiceSpec {
                ports: Some(ports),
                cluster_ip: Some("None".to_string()), // Headless service
                selector: Some(match_labels),
                ..Default::default()
            }),
            status: None,
        }
    }

    /// Builds a StatefulSet for the replica.
    fn build_statefulset(&self, replica: &MaterializeClusterReplica) -> StatefulSet {
        let statefulset_name = replica.statefulset_name();
        let service_name = replica.service_name();

        let match_labels = btreemap! {
            "materialize.cloud/name".to_string() => statefulset_name.clone(),
        };

        // Build pod labels
        let mut pod_labels = replica.default_labels();
        pod_labels.insert(
            "materialize.cloud/name".to_string(),
            statefulset_name.clone(),
        );
        pod_labels.insert("app".to_string(), "clusterd".to_string());

        // Add custom labels from spec
        for (key, value) in &replica.spec.labels {
            pod_labels.insert(key.clone(), value.clone());
        }

        // Build pod annotations
        let mut pod_annotations = btreemap! {
            // Prevent cluster-autoscaler from evicting these pods
            "cluster-autoscaler.kubernetes.io/safe-to-evict".to_string() => "false".to_string(),
            "karpenter.sh/do-not-evict".to_string() => "true".to_string(),
            "karpenter.sh/do-not-disrupt".to_string() => "true".to_string(),
        };

        // Add prometheus scrape annotations if enabled
        if self.config.enable_prometheus_scrape_annotations {
            if let Some(internal_http_port) = replica
                .spec
                .ports
                .iter()
                .find(|p| p.name == "internal-http")
            {
                pod_annotations.insert("prometheus.io/scrape".to_string(), "true".to_string());
                pod_annotations.insert(
                    "prometheus.io/port".to_string(),
                    internal_http_port.port.to_string(),
                );
                pod_annotations.insert("prometheus.io/path".to_string(), "/metrics".to_string());
                pod_annotations.insert("prometheus.io/scheme".to_string(), "http".to_string());
            }
        }

        // Add custom annotations from spec
        for (key, value) in &replica.spec.annotations {
            pod_annotations.insert(key.clone(), value.clone());
        }

        // Build resource requirements
        let (limits, requests) = self.build_resource_requirements(replica);

        // Build container security context
        let container_security_context = if self.config.enable_security_context {
            Some(SecurityContext {
                privileged: Some(false),
                run_as_non_root: Some(true),
                allow_privilege_escalation: Some(false),
                seccomp_profile: Some(SeccompProfile {
                    type_: "RuntimeDefault".to_string(),
                    ..Default::default()
                }),
                capabilities: Some(Capabilities {
                    drop: Some(vec!["ALL".to_string()]),
                    ..Default::default()
                }),
                ..Default::default()
            })
        } else {
            None
        };

        // Build container ports
        let container_ports: Vec<ContainerPort> = replica
            .spec
            .ports
            .iter()
            .map(|port| ContainerPort {
                container_port: port.port.into(),
                name: Some(port.name.clone()),
                ..Default::default()
            })
            .collect();

        // Extract container name from image
        let container_name = replica
            .spec
            .image
            .rsplit_once('/')
            .and_then(|(_, name_version)| name_version.rsplit_once(':'))
            .map(|(name, _)| name.to_string())
            .unwrap_or_else(|| "clusterd".to_string());

        // Build tolerations
        let tolerations = Some(vec![
            Toleration {
                effect: Some("NoExecute".into()),
                key: Some("node.kubernetes.io/not-ready".into()),
                operator: Some("Exists".into()),
                toleration_seconds: Some(NODE_FAILURE_THRESHOLD_SECONDS),
                value: None,
            },
            Toleration {
                effect: Some("NoExecute".into()),
                key: Some("node.kubernetes.io/unreachable".into()),
                operator: Some("Exists".into()),
                toleration_seconds: Some(NODE_FAILURE_THRESHOLD_SECONDS),
                value: None,
            },
        ]);

        // Build node selector
        let node_selector: BTreeMap<String, String> = if replica.spec.disk {
            [("materialize.cloud/disk".to_string(), "true".to_string())]
                .into_iter()
                .chain(replica.spec.node_selector.clone())
                .collect()
        } else {
            replica.spec.node_selector.clone()
        };

        // Build pod security context
        let pod_security_context =
            self.config
                .service_fs_group
                .map(|fs_group| PodSecurityContext {
                    fs_group: Some(fs_group),
                    run_as_user: Some(fs_group),
                    run_as_group: Some(fs_group),
                    ..Default::default()
                });

        // Build affinity rules
        let affinity = self.build_affinity(replica);

        // Build container
        let container = Container {
            name: container_name,
            image: Some(replica.spec.image.clone()),
            args: Some(replica.spec.args.clone()),
            image_pull_policy: Some(self.config.image_pull_policy.to_string()),
            ports: Some(container_ports),
            security_context: container_security_context,
            resources: Some(ResourceRequirements {
                claims: None,
                limits: Some(limits),
                requests: Some(requests),
            }),
            ..Default::default()
        };

        // Build pod template without annotations (compute hash first)
        let mut pod_template_spec = PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(pod_labels),
                ..Default::default()
            }),
            spec: Some(PodSpec {
                containers: vec![container],
                security_context: pod_security_context,
                node_selector: if node_selector.is_empty() {
                    None
                } else {
                    Some(node_selector)
                },
                scheduler_name: self.config.scheduler_name.clone(),
                service_account: self.config.service_account.clone(),
                affinity,
                tolerations,
                // Setting 0s termination grace period enables faster recovery
                // from node failures by allowing a new pod to start immediately
                // when the previous pod is terminating.
                termination_grace_period_seconds: Some(0),
                ..Default::default()
            }),
        };

        // Compute pod template hash and add to annotations
        let pod_template_json = serde_json::to_string(&pod_template_spec).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(pod_template_json);
        let pod_template_hash = format!("{:x}", hasher.finalize());
        pod_annotations.insert(POD_TEMPLATE_HASH_ANNOTATION.to_string(), pod_template_hash);
        pod_template_spec.metadata.as_mut().unwrap().annotations = Some(pod_annotations);

        // Build StatefulSet
        let scale = replica.spec.scale.max(1);

        StatefulSet {
            metadata: replica.managed_resource_meta(statefulset_name),
            spec: Some(StatefulSetSpec {
                selector: LabelSelector {
                    match_labels: Some(match_labels),
                    ..Default::default()
                },
                service_name: Some(service_name),
                replicas: Some(scale.into()),
                template: pod_template_spec,
                update_strategy: Some(StatefulSetUpdateStrategy {
                    type_: Some("OnDelete".to_string()),
                    ..Default::default()
                }),
                pod_management_policy: Some("Parallel".to_string()),
                ..Default::default()
            }),
            status: None,
        }
    }

    /// Builds resource requirements from the replica spec.
    fn build_resource_requirements(
        &self,
        replica: &MaterializeClusterReplica,
    ) -> (BTreeMap<String, Quantity>, BTreeMap<String, Quantity>) {
        let mut limits = BTreeMap::new();
        let mut requests = BTreeMap::new();

        if let Some(memory_limit) = &replica.spec.memory_limit {
            limits.insert("memory".to_string(), Quantity(memory_limit.clone()));
            // Memory request defaults to limit if not specified
            requests.insert(
                "memory".to_string(),
                Quantity(
                    replica
                        .spec
                        .memory_request
                        .as_ref()
                        .unwrap_or(memory_limit)
                        .clone(),
                ),
            );
        }

        if let Some(cpu_limit) = &replica.spec.cpu_limit {
            limits.insert("cpu".to_string(), Quantity(cpu_limit.clone()));
            // CPU request defaults to limit if not specified
            requests.insert(
                "cpu".to_string(),
                Quantity(
                    replica
                        .spec
                        .cpu_request
                        .as_ref()
                        .unwrap_or(cpu_limit)
                        .clone(),
                ),
            );
        }

        (limits, requests)
    }

    /// Builds affinity rules from the replica spec.
    fn build_affinity(
        &self,
        replica: &MaterializeClusterReplica,
    ) -> Option<k8s_openapi::api::core::v1::Affinity> {
        use k8s_openapi::api::core::v1::{
            Affinity, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
            PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
        };
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

        // Build pod anti-affinity based on other_replicas_selector
        let pod_anti_affinity = if !replica.spec.other_replicas_selector.is_empty() {
            let requirements: Vec<LabelSelectorRequirement> = replica
                .spec
                .other_replicas_selector
                .iter()
                .map(|selector| self.label_selector_to_k8s(selector))
                .collect();

            let ls = LabelSelector {
                match_expressions: Some(requirements),
                ..Default::default()
            };

            let pat = PodAffinityTerm {
                label_selector: Some(ls),
                topology_key: "kubernetes.io/hostname".to_string(),
                ..Default::default()
            };

            // Use hard anti-affinity by default
            Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: Some(vec![pat]),
                ..Default::default()
            })
        } else {
            None
        };

        // Build node affinity for availability zones
        let node_affinity = replica.spec.availability_zones.as_ref().map(|azs| {
            let selector = NodeSelectorTerm {
                match_expressions: Some(vec![NodeSelectorRequirement {
                    key: "materialize.cloud/availability-zone".to_string(),
                    operator: "In".to_string(),
                    values: Some(azs.clone()),
                }]),
                match_fields: None,
            };

            // Use hard node affinity by default
            NodeAffinity {
                preferred_during_scheduling_ignored_during_execution: None,
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![selector],
                }),
            }
        });

        if pod_anti_affinity.is_some() || node_affinity.is_some() {
            Some(Affinity {
                pod_anti_affinity,
                node_affinity,
                ..Default::default()
            })
        } else {
            None
        }
    }

    /// Converts a ClusterLabelSelector to a Kubernetes LabelSelectorRequirement.
    fn label_selector_to_k8s(
        &self,
        selector: &ClusterLabelSelector,
    ) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

        let operator = match selector.operator {
            ClusterLabelSelectorOperator::In => "In".to_string(),
            ClusterLabelSelectorOperator::NotIn => "NotIn".to_string(),
            ClusterLabelSelectorOperator::Exists => "Exists".to_string(),
            ClusterLabelSelectorOperator::DoesNotExist => "DoesNotExist".to_string(),
        };

        LabelSelectorRequirement {
            key: selector.key.clone(),
            operator,
            values: if selector.values.is_empty() {
                None
            } else {
                Some(selector.values.clone())
            },
        }
    }

    /// Synchronizes replica status based on StatefulSet state.
    async fn sync_replica_status(
        &self,
        client: &Client,
        replica: &MaterializeClusterReplica,
    ) -> Result<(), kube::Error> {
        let namespace = replica.namespace();
        let replica_api: Api<MaterializeClusterReplica> =
            Api::namespaced(client.clone(), &namespace);

        let statefulset_name = replica.statefulset_name();
        let Some(statefulset) = self
            .statefulsets
            .get(&ObjectRef::new(&statefulset_name).within(&namespace))
        else {
            return Ok(());
        };

        // Determine replica phase based on StatefulSet status
        let (phase, ready) = if let Some(status) = &statefulset.status {
            let ready_replicas = status.ready_replicas.unwrap_or(0);
            let desired_replicas = status.replicas;

            if ready_replicas == desired_replicas && ready_replicas > 0 {
                (ReplicaPhase::Running, true)
            } else if ready_replicas > 0 {
                (ReplicaPhase::Creating, false)
            } else {
                (ReplicaPhase::Pending, false)
            }
        } else {
            (ReplicaPhase::Pending, false)
        };

        let ready_str = if ready { "True" } else { "False" };

        // Check if status needs update
        let current_status = replica.status.as_ref();
        let needs_update = current_status.map_or(true, |s| {
            s.phase != phase
                || !s
                    .conditions
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == ready_str)
        });

        if !needs_update {
            return Ok(());
        }

        // Update status
        let mut new_status = replica.status();
        new_status.phase = phase;
        new_status.statefulset_name = Some(statefulset_name.clone());
        new_status.service_name = Some(replica.service_name());
        new_status.addresses = replica.compute_addresses(&namespace);
        new_status.observed_generation = replica.meta().generation.unwrap_or(0);
        new_status.conditions = vec![Condition {
            type_: "Ready".to_string(),
            status: ready_str.to_string(),
            last_transition_time: Time(Timestamp::now()),
            message: format!("Replica is{} ready", if ready { "" } else { " not" }),
            observed_generation: replica.meta().generation,
            reason: "StatefulSetStatus".to_string(),
        }];

        let mut new_replica = replica.clone();
        new_replica.status = Some(new_status);

        replica_api
            .replace_status(
                &replica.name_unchecked(),
                &PostParams::default(),
                &new_replica,
            )
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl k8s_controller::Context for Context {
    type Resource = MaterializeClusterReplica;
    type Error = Error;

    const FINALIZER_NAME: Option<&'static str> =
        Some("orchestratord.materialize.cloud/cluster-replica");

    #[instrument(fields(replica_name = %replica.name_unchecked()))]
    async fn apply(
        &self,
        client: Client,
        replica: &Self::Resource,
    ) -> Result<Option<Action>, Self::Error> {
        // Initialize status if needed
        if replica.status.is_none() {
            let replica_api: Api<MaterializeClusterReplica> =
                Api::namespaced(client.clone(), &replica.namespace());
            let mut new_replica = replica.clone();
            new_replica.status = Some(replica.status());
            replica_api
                .replace_status(
                    &replica.name_unchecked(),
                    &PostParams::default(),
                    &new_replica,
                )
                .await?;
            // Updating the status should trigger a reconciliation
            return Ok(None);
        }

        let namespace = replica.namespace();
        let service_api: Api<Service> = Api::namespaced(client.clone(), &namespace);
        let statefulset_api: Api<StatefulSet> = Api::namespaced(client.clone(), &namespace);

        // Create/update Service
        let service = self.build_service(replica);
        trace!("creating/updating service for replica");
        apply_resource(&service_api, &service).await?;

        // Create/update StatefulSet
        let statefulset = self.build_statefulset(replica);
        trace!("creating/updating statefulset for replica");
        apply_resource(&statefulset_api, &statefulset).await?;

        // Sync status
        self.sync_replica_status(&client, replica).await?;

        Ok(None)
    }
}
