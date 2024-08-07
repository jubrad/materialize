# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import re
from enum import Enum

from materialize.output_consistency.output.format_constants import (
    COMMENT_PREFIX,
    OUTPUT_LINE_SEPARATOR,
    SECTION_COLLAPSED_PREFIX,
    SECTION_EXPANDED_PREFIX,
)


class OutputPrinterMode(Enum):
    PRINT = 1
    COLLECT = 2


class BaseOutputPrinter:

    def __init__(self, mode: OutputPrinterMode = OutputPrinterMode.PRINT):
        self.mode = mode
        self.collected_output = []

    def print_empty_line(self) -> None:
        self._print_raw("")

    def start_section(self, header: str, collapsed: bool = True) -> None:
        prefix = SECTION_COLLAPSED_PREFIX if collapsed else SECTION_EXPANDED_PREFIX
        self._print_raw(f"{prefix}{header}")

    def print_separator_line(self) -> None:
        self._print_text(OUTPUT_LINE_SEPARATOR)

    def _print_executable(self, sql: str) -> None:
        self._print_raw(sql)

    def _print_text(self, text: str) -> None:
        adjusted_text = re.sub("\n", f"\n{COMMENT_PREFIX}", text)
        adjusted_text = f"{COMMENT_PREFIX}{adjusted_text}"

        self._print_raw(adjusted_text)

    def _print_raw(self, sql: str) -> None:
        if self.mode == OutputPrinterMode.PRINT:
            print(sql)
        elif self.mode == OutputPrinterMode.COLLECT:
            self.collected_output.append(sql)
        else:
            raise RuntimeError(f"Unsupported mode: {self.mode}")
