#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# mkpipeline.sh — dynamically renders a pipeline.yml for Buildkite.

# This script's path is hardcoded into the Buildkite UI. It bootstraps the CI
# process by building the CI builder image, in which all other dependencies
# are installed. Unfortunately that means this script needs to be written in
# Bash, since our Python tools are only available once the CI builder image has
# been built.

set -euo pipefail

pipeline=${1:-test}
bootstrap_steps=

for arch in x86_64 aarch64; do
    for toolchain in stable nightly; do
        if ! MZ_DEV_CI_BUILDER_ARCH=$arch bin/ci-builder exists $toolchain; then
            queue=builder-linux-x86_64
            if [[ $arch = aarch64 ]]; then
                queue=builder-linux-aarch64-mem
            fi
            bootstrap_steps+="
  - label: bootstrap $toolchain $arch
    command: bin/ci-builder push $toolchain
    agents:
      queue: $queue
"
        fi
    done
done

exec buildkite-agent pipeline upload <<EOF
steps:
  $bootstrap_steps
  - wait
  - label: mkpipeline
    env:
      CI_BUILD_WTIH_BAZEL: 1
    command: bin/ci-builder run stable bin/pyactivate -m ci.mkpipeline $pipeline $@
    priority: 200
    agents:
      queue: hetzner-aarch64-4cpu-8gb
    retry:
      automatic:
        - exit_status: -1
          signal_reason: none
          limit: 2
        - signal_reason: agent_stop
          limit: 2
EOF
