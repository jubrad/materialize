// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CRD definitions for MaterializeCluster resources.
//!
//! MaterializeCluster represents a logical SQL cluster in Materialize.
//! Each cluster can have multiple replicas (MaterializeClusterReplica CRDs).
//!
//! The hierarchy is:
//! ```text
//! Materialize (existing)
//!     └── owns → MaterializeCluster (one per SQL cluster)
//!                    └── owns → MaterializeClusterReplica (one per replica)
//! ```

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;
use kube::{CustomResource, Resource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{ManagedResource, new_resource_id};

pub mod v1alpha1 {
    use super::*;

    /// The role/criticality of a cluster, used for scheduling and resource management.
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
    pub enum ClusterRole {
        /// System-critical clusters that should be scheduled with high priority.
        SystemCritical,
        /// System clusters used for internal operations.
        System,
        /// User-created clusters for running user workloads.
        #[default]
        User,
    }

    /// MaterializeClusterSpec defines the desired state of a MaterializeCluster.
    ///
    /// This CRD represents a logical SQL cluster in Materialize. The actual compute
    /// resources are managed through child MaterializeClusterReplica CRDs.
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
        kind = "MaterializeCluster",
        singular = "materializecluster",
        plural = "materializeclusters",
        shortname = "mzc",
        status = "MaterializeClusterStatus",
        printcolumn = r#"{"name": "ClusterID", "type": "string", "description": "The SQL cluster ID", "jsonPath": ".spec.clusterId"}"#,
        printcolumn = r#"{"name": "Role", "type": "string", "description": "Cluster role", "jsonPath": ".spec.role"}"#,
        printcolumn = r#"{"name": "Ready", "type": "integer", "description": "Ready replicas", "jsonPath": ".status.readyReplicas"}"#,
        printcolumn = r#"{"name": "Replicas", "type": "integer", "description": "Total replicas", "jsonPath": ".status.replicaCount"}"#
    )]
    pub struct MaterializeClusterSpec {
        /// The internal cluster ID (e.g., "u1", "s1").
        pub cluster_id: String,

        /// The human-readable cluster name as shown in SQL.
        pub cluster_name: String,

        /// The role/criticality of the cluster.
        #[serde(default)]
        pub role: ClusterRole,

        /// The environmentd generation that created this cluster.
        /// Used for coordinating upgrades and identifying stale resources.
        pub environmentd_generation: u64,
    }

    /// MaterializeClusterStatus defines the observed state of a MaterializeCluster.
    #[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct MaterializeClusterStatus {
        /// Resource identifier used as a name prefix to avoid resource name collisions.
        /// Generated automatically if not set.
        #[serde(default)]
        pub resource_id: String,

        /// Total number of MaterializeClusterReplica resources owned by this cluster.
        #[serde(default)]
        pub replica_count: u32,

        /// Number of replicas in the Ready state.
        #[serde(default)]
        pub ready_replicas: u32,

        /// Standard Kubernetes conditions for the cluster.
        #[serde(default)]
        pub conditions: Vec<Condition>,
    }

    impl MaterializeCluster {
        /// Returns the cluster's namespace.
        pub fn namespace(&self) -> String {
            self.meta().namespace.clone().unwrap()
        }

        /// Returns the cluster name without checking.
        pub fn name_unchecked(&self) -> String {
            self.metadata.name.clone().unwrap()
        }

        /// Returns the resource ID, generating one if necessary.
        pub fn resource_id(&self) -> &str {
            &self.status.as_ref().unwrap().resource_id
        }

        /// Creates a name prefixed with the resource ID for uniqueness.
        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("mzc{}-{}", self.resource_id(), suffix)
        }

        /// Returns the cluster's status, initializing it if necessary.
        pub fn status(&self) -> MaterializeClusterStatus {
            self.status
                .clone()
                .unwrap_or_else(|| MaterializeClusterStatus {
                    resource_id: new_resource_id(),
                    ..Default::default()
                })
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

    impl MaterializeClusterStatus {
        /// Checks if the status needs to be updated compared to another status.
        /// Ignores last_transition_time differences.
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

    impl ManagedResource for MaterializeCluster {
        fn default_labels(&self) -> BTreeMap<String, String> {
            BTreeMap::from_iter([
                (
                    "materialize.cloud/cluster-id".to_owned(),
                    self.spec.cluster_id.clone(),
                ),
                (
                    "materialize.cloud/cluster-name".to_owned(),
                    self.spec.cluster_name.clone(),
                ),
                (
                    "materialize.cloud/mzc-resource-id".to_owned(),
                    self.resource_id().to_owned(),
                ),
            ])
        }
    }
}

#[cfg(test)]
mod tests {
    use kube::core::ObjectMeta;

    use super::v1alpha1::{ClusterRole, MaterializeCluster, MaterializeClusterSpec};

    #[mz_ore::test]
    fn test_cluster_role_default() {
        let role: ClusterRole = Default::default();
        assert_eq!(role, ClusterRole::User);
    }

    #[mz_ore::test]
    fn test_cluster_name_prefixed() {
        use super::v1alpha1::MaterializeClusterStatus;

        let cluster = MaterializeCluster {
            spec: MaterializeClusterSpec {
                cluster_id: "u1".to_owned(),
                cluster_name: "default".to_owned(),
                role: ClusterRole::User,
                environmentd_generation: 1,
            },
            metadata: ObjectMeta {
                name: Some("cluster-u1-gen-1".to_owned()),
                namespace: Some("test-ns".to_owned()),
                ..Default::default()
            },
            status: Some(MaterializeClusterStatus {
                resource_id: "abc123".to_owned(),
                ..Default::default()
            }),
        };

        assert_eq!(cluster.name_prefixed("test"), "mzcabc123-test");
        assert_eq!(cluster.namespace(), "test-ns");
    }
}
