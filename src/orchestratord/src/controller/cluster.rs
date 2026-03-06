// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Controller for MaterializeCluster resources.
//!
//! This controller watches MaterializeCluster CRDs and aggregates status
//! from child MaterializeClusterReplica resources. It:
//!
//! - Counts total replicas owned by the cluster
//! - Counts replicas in the Ready state
//! - Sets conditions based on replica health

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;
use kube::api::PostParams;
use kube::runtime::controller::Action;
use kube::runtime::reflector::Store;
use kube::{Api, Client, Resource};
use mz_ore::instrument;
use tracing::trace;

use crate::Error;
use crate::k8s::make_reflector;
use mz_cloud_resources::crd::materialize_cluster::v1alpha1::MaterializeCluster;
use mz_cloud_resources::crd::materialize_cluster_replica::v1alpha1::{
    MaterializeClusterReplica, ReplicaPhase,
};

/// Configuration for the MaterializeCluster controller.
pub struct Config {
    // Currently no configuration needed, but keeping for consistency
    // and future extensibility.
}

/// Controller context for MaterializeCluster resources.
pub struct Context {
    #[allow(dead_code)]
    config: Config,
    replicas: Store<MaterializeClusterReplica>,
}

impl Context {
    /// Creates a new controller context.
    pub async fn new(config: Config, client: Client) -> Self {
        Self {
            config,
            replicas: make_reflector(client).await,
        }
    }

    /// Aggregates status from child replicas.
    fn aggregate_replica_status(&self, cluster: &MaterializeCluster) -> (u32, u32) {
        let cluster_name = cluster.name_unchecked();
        let namespace = cluster.namespace();

        let mut total_replicas: u32 = 0;
        let mut ready_replicas: u32 = 0;

        // Iterate over all replicas and count those belonging to this cluster
        for replica in self.replicas.state() {
            // Check if replica belongs to this cluster (same namespace and references this cluster)
            let replica_namespace = replica.meta().namespace.as_deref().unwrap_or("");
            if replica_namespace != namespace {
                continue;
            }

            if replica.spec.cluster_ref != cluster_name {
                continue;
            }

            total_replicas += 1;

            // Check if replica is ready
            if let Some(status) = &replica.status {
                if status.phase == ReplicaPhase::Running {
                    ready_replicas += 1;
                }
            }
        }

        (total_replicas, ready_replicas)
    }

    /// Synchronizes cluster status based on child replica states.
    async fn sync_cluster_status(
        &self,
        client: &Client,
        cluster: &MaterializeCluster,
    ) -> Result<(), kube::Error> {
        let namespace = cluster.namespace();
        let cluster_api: Api<MaterializeCluster> = Api::namespaced(client.clone(), &namespace);

        let (replica_count, ready_replicas) = self.aggregate_replica_status(cluster);

        // Determine overall cluster health
        let (ready, message) = if replica_count == 0 {
            (false, "No replicas configured".to_string())
        } else if ready_replicas == replica_count {
            (true, format!("All {replica_count} replicas ready"))
        } else {
            (
                false,
                format!("{ready_replicas} of {replica_count} replicas ready"),
            )
        };

        let ready_str = if ready { "True" } else { "False" };

        // Check if status needs update
        let current_status = cluster.status.as_ref();
        let needs_update = current_status.map_or(true, |s| {
            s.replica_count != replica_count
                || s.ready_replicas != ready_replicas
                || !s
                    .conditions
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == ready_str)
        });

        if !needs_update {
            return Ok(());
        }

        trace!(replica_count, ready_replicas, "updating cluster status");

        // Update status
        let mut new_status = cluster.status();
        new_status.replica_count = replica_count;
        new_status.ready_replicas = ready_replicas;
        new_status.conditions = vec![Condition {
            type_: "Ready".to_string(),
            status: ready_str.to_string(),
            last_transition_time: Time(Timestamp::now()),
            message,
            observed_generation: cluster.meta().generation,
            reason: "ReplicaStatus".to_string(),
        }];

        let mut new_cluster = cluster.clone();
        new_cluster.status = Some(new_status);

        cluster_api
            .replace_status(
                &cluster.name_unchecked(),
                &PostParams::default(),
                &new_cluster,
            )
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl k8s_controller::Context for Context {
    type Resource = MaterializeCluster;
    type Error = Error;

    const FINALIZER_NAME: Option<&'static str> = Some("orchestratord.materialize.cloud/cluster");

    #[instrument(fields())]
    async fn apply(
        &self,
        client: Client,
        cluster: &Self::Resource,
    ) -> Result<Option<Action>, Self::Error> {
        // Initialize status if needed
        if cluster.status.is_none() {
            let cluster_api: Api<MaterializeCluster> =
                Api::namespaced(client.clone(), &cluster.namespace());
            let mut new_cluster = cluster.clone();
            new_cluster.status = Some(cluster.status());
            cluster_api
                .replace_status(
                    &cluster.name_unchecked(),
                    &PostParams::default(),
                    &new_cluster,
                )
                .await?;
            // Updating the status should trigger a reconciliation
            return Ok(None);
        }

        // Sync status from child replicas
        self.sync_cluster_status(&client, cluster).await?;

        Ok(None)
    }
}
