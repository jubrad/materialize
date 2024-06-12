# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""GitHub utilities."""

import os
import re
from dataclasses import dataclass
from typing import Any

import requests

from materialize.observed_error import ObservedBaseError

CI_RE = re.compile("ci-regexp: (.*)")
CI_APPLY_TO = re.compile("ci-apply-to: (.*)")


@dataclass
class KnownGitHubIssue:
    regex: re.Pattern[Any]
    apply_to: str | None
    info: dict[str, Any]


@dataclass(kw_only=True, unsafe_hash=True)
class GitHubIssueWithInvalidRegexp(ObservedBaseError):
    html_url: str
    title: str
    issue_number: int
    regex_pattern: str

    def to_markdown(self) -> str:
        return f'<a href="{self.html_url}">{self.title} (#{self.issue_number})</a>: Invalid regex in ci-regexp: {self.regex_pattern}, ignoring'


def get_known_issues_from_github_page(page: int = 1) -> Any:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token := os.getenv("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"

    response = requests.get(
        f'https://api.github.com/search/issues?q=repo:MaterializeInc/materialize%20type:issue%20in:body%20"ci-regexp%3A"&per_page=100&page={page}',
        headers=headers,
    )

    if response.status_code != 200:
        raise ValueError(f"Bad return code from GitHub: {response.status_code}")

    issues_json = response.json()
    assert issues_json["incomplete_results"] == False
    return issues_json


def get_known_issues_from_github() -> (
    tuple[list[KnownGitHubIssue], list[GitHubIssueWithInvalidRegexp]]
):
    page = 1
    issues_json = get_known_issues_from_github_page(page)
    while issues_json["total_count"] > len(issues_json["items"]):
        page += 1
        next_page_json = get_known_issues_from_github_page(page)
        if not next_page_json["items"]:
            break
        issues_json["items"].extend(next_page_json["items"])

    known_issues = []
    issues_with_invalid_regex = []

    for issue in issues_json["items"]:
        matches = CI_RE.findall(issue["body"])
        matches_apply_to = CI_APPLY_TO.findall(issue["body"])
        for match in matches:
            try:
                regex_pattern = re.compile(match.strip().encode("utf-8"))
            except:
                issues_with_invalid_regex.append(
                    GitHubIssueWithInvalidRegexp(
                        html_url=issue.info["html_url"],
                        title=issue.info["title"],
                        issue_number=issue.info["number"],
                        regex_pattern=match.strip(),
                    )
                )
                continue

            if matches_apply_to:
                for match_apply_to in matches_apply_to:
                    known_issues.append(
                        KnownGitHubIssue(
                            regex_pattern, match_apply_to.strip().lower(), issue
                        )
                    )
            else:
                known_issues.append(KnownGitHubIssue(regex_pattern, None, issue))

    return (known_issues, issues_with_invalid_regex)


def for_github_re(text: bytes) -> bytes:
    """
    Matching newlines in regular expressions is kind of annoying, don't expect
    ci-regexp to do that correctly, but instead replace all newlines with a
    space. For examples this makes matching this panic easier:

      thread 'test_auth_deduplication' panicked at src/environmentd/tests/auth.rs:1878:5:
      assertion `left == right` failed

    Previously the regex should have been:
      thread 'test_auth_deduplication' panicked at src/environmentd/tests/auth.rs.*\n.*left == right

    With this function it can be:
      thread 'test_auth_deduplication' panicked at src/environmentd/tests/auth.rs.*left == right
    """
    return text.replace(b"\n", b" ")
