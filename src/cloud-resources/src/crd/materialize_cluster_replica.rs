// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CRD definitions for MaterializeClusterReplica resources.
//!
//! MaterializeClusterReplica represents a single replica of a SQL cluster.
//! Each replica maps to a StatefulSet and Service in Kubernetes.
//!
//! The spec contains everything needed to generate the underlying Kubernetes
//! resources, making the CRD fully declarative.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;
use kube::{CustomResource, Resource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{ManagedResource, new_resource_id};

pub mod v1alpha1 {
    use super::*;

    /// The current phase of the replica.
    #[derive(
        Clone,
        Debug,
        Default,
        PartialEq,
        Eq,
        Deserialize,
        Serialize,
        JsonSchema
    )]
    #[serde(rename_all = "camelCase")]
    pub enum ReplicaPhase {
        /// Replica resources are being created.
        #[default]
        Pending,
        /// Replica is creating underlying Kubernetes resources.
        Creating,
        /// Replica is running and ready.
        Running,
        /// Replica has failed.
        Failed,
        /// Replica is being deleted.
        Terminating,
    }

    /// A named port to expose on the replica.
    #[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct ClusterPort {
        /// The name of the port (e.g., "controller", "compute").
        pub name: String,
        /// The port number.
        pub port: u16,
    }

    /// A label selector expression for pod affinity/anti-affinity rules.
    #[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct ClusterLabelSelector {
        /// The label key to match.
        pub key: String,
        /// The operator for the match (In, NotIn, Exists, DoesNotExist).
        pub operator: ClusterLabelSelectorOperator,
        /// The values to match against (used with In/NotIn operators).
        #[serde(default)]
        pub values: Vec<String>,
    }

    /// Operator for label selector expressions.
    #[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub enum ClusterLabelSelectorOperator {
        /// Label value must be in the set.
        In,
        /// Label value must not be in the set.
        NotIn,
        /// Label must exist.
        Exists,
        /// Label must not exist.
        DoesNotExist,
    }

    /// MaterializeClusterReplicaSpec defines the desired state of a replica.
    ///
    /// This spec contains everything needed to create the underlying StatefulSet
    /// and Service. The orchestratord controller translates this spec directly
    /// into Kubernetes resources without injecting defaults.
    #[derive(
        CustomResource,
        Clone,
        Debug,
        Default,
        PartialEq,
        Deserialize,
        Serialize,
        JsonSchema
    )]
    #[serde(rename_all = "camelCase")]
    #[kube(
        namespaced,
        group = "materialize.cloud",
        version = "v1alpha1",
        kind = "MaterializeClusterReplica",
        singular = "materializeclusterreplica",
        plural = "materializeclusterreplicas",
        shortname = "mzcr",
        status = "MaterializeClusterReplicaStatus",
        printcolumn = r#"{"name": "ClusterRef", "type": "string", "description": "Parent cluster name", "jsonPath": ".spec.clusterRef"}"#,
        printcolumn = r#"{"name": "ReplicaID", "type": "string", "description": "Replica ID", "jsonPath": ".spec.replicaId"}"#,
        printcolumn = r#"{"name": "Phase", "type": "string", "description": "Replica phase", "jsonPath": ".status.phase"}"#,
        printcolumn = r#"{"name": "Scale", "type": "integer", "description": "Number of processes", "jsonPath": ".spec.scale"}"#
    )]
    pub struct MaterializeClusterReplicaSpec {
        // === Parent reference ===
        /// Name of the parent MaterializeCluster CRD.
        pub cluster_ref: String,

        // === Identity ===
        /// The internal replica ID (e.g., "r1").
        pub replica_id: String,

        /// The human-readable replica name as shown in SQL.
        pub replica_name: String,

        // === Container configuration ===
        /// Container image to run.
        pub image: String,

        /// Init container image (for setup tasks).
        #[serde(skip_serializing_if = "Option::is_none")]
        pub init_container_image: Option<String>,

        /// Command-line arguments for the container.
        #[serde(default)]
        pub args: Vec<String>,

        /// Ports to expose.
        #[serde(default)]
        pub ports: Vec<ClusterPort>,

        // === Resource limits and requests ===
        /// CPU limit in millicpus (e.g., "1000" = 1 CPU).
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cpu_limit: Option<String>,

        /// CPU request in millicpus.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub cpu_request: Option<String>,

        /// Memory limit (e.g., "4Gi").
        #[serde(skip_serializing_if = "Option::is_none")]
        pub memory_limit: Option<String>,

        /// Memory request.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub memory_request: Option<String>,

        /// Disk/scratch volume limit (e.g., "10Gi").
        #[serde(skip_serializing_if = "Option::is_none")]
        pub disk_limit: Option<String>,

        // === Scaling and scheduling ===
        /// Number of processes in this replica (typically 1 for single-process replicas).
        #[serde(default = "default_scale")]
        pub scale: u16,

        /// Availability zones where this replica can be scheduled.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub availability_zones: Option<Vec<String>>,

        /// Node selector key-value pairs for scheduling.
        #[serde(default)]
        pub node_selector: BTreeMap<String, String>,

        // === Metadata for pods ===
        /// Labels to apply to pods.
        #[serde(default)]
        pub labels: BTreeMap<String, String>,

        /// Annotations to apply to pods.
        #[serde(default)]
        pub annotations: BTreeMap<String, String>,

        // === Affinity selectors ===
        /// Label selectors for other replicas (used for anti-affinity).
        /// Prevents co-scheduling with pods matching these selectors.
        #[serde(default)]
        pub other_replicas_selector: Vec<ClusterLabelSelector>,

        /// Label selectors for all replicas including self (used for topology spread).
        #[serde(default)]
        pub replicas_selector: Vec<ClusterLabelSelector>,

        // === Disk configuration ===
        /// Whether scratch disk is required.
        #[serde(default)]
        pub disk: bool,
    }

    fn default_scale() -> u16 {
        1
    }

    /// MaterializeClusterReplicaStatus defines the observed state of a replica.
    #[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct MaterializeClusterReplicaStatus {
        /// Resource identifier for naming Kubernetes resources.
        #[serde(default)]
        pub resource_id: String,

        /// Current phase of the replica.
        #[serde(default)]
        pub phase: ReplicaPhase,

        /// Addresses for each port, keyed by port name.
        /// Each value is a list of addresses (one per process in scale).
        #[serde(default)]
        pub addresses: BTreeMap<String, Vec<String>>,

        /// Standard Kubernetes conditions.
        #[serde(default)]
        pub conditions: Vec<Condition>,

        /// The generation most recently observed by the controller.
        #[serde(default)]
        pub observed_generation: i64,

        /// Name of the StatefulSet created for this replica.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub statefulset_name: Option<String>,

        /// Name of the Service created for this replica.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub service_name: Option<String>,
    }

    impl MaterializeClusterReplica {
        /// Returns the replica's namespace.
        pub fn namespace(&self) -> String {
            self.meta().namespace.clone().unwrap()
        }

        /// Returns the replica name without checking.
        pub fn name_unchecked(&self) -> String {
            self.metadata.name.clone().unwrap()
        }

        /// Returns the resource ID.
        pub fn resource_id(&self) -> &str {
            &self.status.as_ref().unwrap().resource_id
        }

        /// Creates a name prefixed with the resource ID for uniqueness.
        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("mzcr{}-{}", self.resource_id(), suffix)
        }

        /// Returns the StatefulSet name for this replica.
        pub fn statefulset_name(&self) -> String {
            self.name_prefixed(&format!("replica-{}", self.spec.replica_id))
        }

        /// Returns the Service name for this replica.
        pub fn service_name(&self) -> String {
            self.name_prefixed(&format!("replica-{}", self.spec.replica_id))
        }

        /// Returns the replica's status, initializing it if necessary.
        pub fn status(&self) -> MaterializeClusterReplicaStatus {
            self.status
                .clone()
                .unwrap_or_else(|| MaterializeClusterReplicaStatus {
                    resource_id: new_resource_id(),
                    ..Default::default()
                })
        }

        /// Computes DNS addresses for this replica.
        /// Returns a map from port name to list of addresses.
        pub fn compute_addresses(&self, namespace: &str) -> BTreeMap<String, Vec<String>> {
            let service_name = self.service_name();
            let scale = self.spec.scale.max(1);

            let hosts: Vec<String> = (0..scale)
                .map(|i| format!("{service_name}-{i}.{service_name}.{namespace}.svc.cluster.local"))
                .collect();

            let mut addresses = BTreeMap::new();
            for port in &self.spec.ports {
                let port_addrs: Vec<String> = hosts
                    .iter()
                    .map(|host| format!("{host}:{}", port.port))
                    .collect();
                addresses.insert(port.name.clone(), port_addrs);
            }
            addresses
        }

        /// Checks if conditions need to be updated based on observed generation.
        pub fn conditions_need_update(&self) -> bool {
            let Some(status) = self.status.as_ref() else {
                return true;
            };
            if status.conditions.is_empty() {
                return true;
            }
            for condition in &status.conditions {
                if condition.observed_generation != self.meta().generation {
                    return true;
                }
            }
            false
        }

        /// Creates a new condition with the current timestamp.
        pub fn make_condition(
            &self,
            type_: &str,
            status: &str,
            reason: &str,
            message: &str,
        ) -> Condition {
            Condition {
                type_: type_.to_string(),
                status: status.to_string(),
                last_transition_time: Time(Timestamp::now()),
                message: message.to_string(),
                observed_generation: self.meta().generation,
                reason: reason.to_string(),
            }
        }
    }

    impl MaterializeClusterReplicaStatus {
        /// Checks if the status needs to be updated compared to another status.
        pub fn needs_update(&self, other: &Self) -> bool {
            let now = Timestamp::now();
            let mut a = self.clone();
            for condition in &mut a.conditions {
                condition.last_transition_time = Time(now);
            }
            let mut b = other.clone();
            for condition in &mut b.conditions {
                condition.last_transition_time = Time(now);
            }
            a != b
        }
    }

    impl ManagedResource for MaterializeClusterReplica {
        fn default_labels(&self) -> BTreeMap<String, String> {
            BTreeMap::from_iter([
                (
                    "materialize.cloud/cluster-ref".to_owned(),
                    self.spec.cluster_ref.clone(),
                ),
                (
                    "materialize.cloud/replica-id".to_owned(),
                    self.spec.replica_id.clone(),
                ),
                (
                    "materialize.cloud/replica-name".to_owned(),
                    self.spec.replica_name.clone(),
                ),
                (
                    "materialize.cloud/mzcr-resource-id".to_owned(),
                    self.resource_id().to_owned(),
                ),
            ])
        }
    }
}

#[cfg(test)]
mod tests {
    use kube::core::ObjectMeta;

    use super::v1alpha1::{
        ClusterPort, MaterializeClusterReplica, MaterializeClusterReplicaSpec,
        MaterializeClusterReplicaStatus, ReplicaPhase,
    };

    #[mz_ore::test]
    fn test_replica_phase_default() {
        let phase: ReplicaPhase = Default::default();
        assert_eq!(phase, ReplicaPhase::Pending);
    }

    #[mz_ore::test]
    fn test_compute_addresses() {
        let replica = MaterializeClusterReplica {
            spec: MaterializeClusterReplicaSpec {
                cluster_ref: "cluster-u1-gen-1".to_owned(),
                replica_id: "r1".to_owned(),
                replica_name: "replica1".to_owned(),
                image: "materialize/clusterd:latest".to_owned(),
                scale: 2,
                ports: vec![
                    ClusterPort {
                        name: "controller".to_owned(),
                        port: 6878,
                    },
                    ClusterPort {
                        name: "compute".to_owned(),
                        port: 6877,
                    },
                ],
                ..Default::default()
            },
            metadata: ObjectMeta {
                name: Some("replica-r1-gen-1".to_owned()),
                namespace: Some("mz-ns".to_owned()),
                ..Default::default()
            },
            status: Some(MaterializeClusterReplicaStatus {
                resource_id: "xyz789".to_owned(),
                ..Default::default()
            }),
        };

        let addresses = replica.compute_addresses("mz-ns");

        // Check controller port addresses
        let controller_addrs = addresses.get("controller").unwrap();
        assert_eq!(controller_addrs.len(), 2);
        assert!(controller_addrs[0].contains("6878"));
        assert!(controller_addrs[0].contains("mz-ns.svc.cluster.local"));

        // Check compute port addresses
        let compute_addrs = addresses.get("compute").unwrap();
        assert_eq!(compute_addrs.len(), 2);
        assert!(compute_addrs[0].contains("6877"));
    }

    #[mz_ore::test]
    fn test_replica_naming() {
        let replica = MaterializeClusterReplica {
            spec: MaterializeClusterReplicaSpec {
                cluster_ref: "cluster-u1-gen-1".to_owned(),
                replica_id: "r1".to_owned(),
                replica_name: "replica1".to_owned(),
                image: "materialize/clusterd:latest".to_owned(),
                ..Default::default()
            },
            metadata: ObjectMeta {
                name: Some("replica-r1-gen-1".to_owned()),
                namespace: Some("mz-ns".to_owned()),
                ..Default::default()
            },
            status: Some(MaterializeClusterReplicaStatus {
                resource_id: "abc123".to_owned(),
                ..Default::default()
            }),
        };

        assert_eq!(replica.statefulset_name(), "mzcrabc123-replica-r1");
        assert_eq!(replica.service_name(), "mzcrabc123-replica-r1");
        assert_eq!(replica.namespace(), "mz-ns");
    }
}
