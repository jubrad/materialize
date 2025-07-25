# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import logging
import subprocess

from materialize import ui
from materialize.cloudtest import DEFAULT_K8S_CONTEXT_NAME
from materialize.cloudtest.util.print_pods import print_pods
from materialize.ui import UIError

LOGGER = logging.getLogger(__name__)


def wait(
    condition: str,
    resource: str,
    timeout_secs: int = 300,
    context: str = DEFAULT_K8S_CONTEXT_NAME,
    *,
    label: str | None = None,
    namespace: str | None = None,
    server: str | None = None,
) -> None:
    cmd = [
        "kubectl",
        "wait",
        "--for",
        condition,
        resource,
        "--timeout",
        f"{timeout_secs}s",
        "--context",
        context,
    ]

    if label is not None:
        cmd.extend(["--selector", label])

    if namespace is not None:
        cmd.extend(["--namespace", namespace])

    if server is not None:
        cmd.extend(["--server", server])

    ui.progress(f'waiting for {" ".join(cmd)} ... ')

    error = None
    for remaining in ui.timeout_loop(timeout_secs, tick=0.1):
        try:
            output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode(
                "ascii"
            )
            # output is:
            # - an empty string when a 'delete' condition is satisfied
            # - 'condition met' for all other conditions
            if len(output) == 0 or "condition met" in output:
                ui.progress("success!", finish=True)
                return
        except subprocess.CalledProcessError as e:
            # use a less verbose output than log_subprocess_error here
            LOGGER.info(f"{e} {e.output.decode('ascii')}")
            error = e

    ui.progress(finish=True)
    print_pods()
    raise UIError(f"kubectl wait never returned 'condition met': {error}")
