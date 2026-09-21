"""Shared Lakebase (Autoscaling Postgres) connection helper.

Uses the Databricks CLI to fetch the endpoint host and a short-lived OAuth token,
then connects with psycopg2. Tokens expire after ~1h, so we cache and refresh
automatically. Import from local scripts:

    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    from lib.lakebase import LakebaseClient
"""
from __future__ import annotations

import json
import os
import subprocess
import time

import psycopg2


def _cli(args: list[str]) -> dict | list:
    """Run a databricks CLI command and parse JSON output."""
    proc = subprocess.run(
        ["databricks", *args, "-o", "json"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args)} failed:\n{proc.stderr.strip()}")
    return json.loads(proc.stdout)


class LakebaseClient:
    """Thin wrapper: resolves host/email once, refreshes the OAuth token on demand."""

    TOKEN_TTL_SECONDS = 45 * 60  # refresh well before the 1h expiry

    def __init__(
        self,
        profile: str | None = None,
        project: str | None = None,
        branch: str | None = None,
        endpoint: str | None = None,
    ):
        self.profile = profile or os.environ["DATABRICKS_PROFILE"]
        self.project = project or os.environ.get("LB_PROJECT", "allianz-hackathon")
        self.branch = branch or os.environ.get("LB_BRANCH", "production")
        self.endpoint = endpoint or os.environ.get("LB_ENDPOINT", "primary")
        self._host: str | None = None
        self._email: str | None = None
        self._token: str | None = None
        self._token_at: float = 0.0

    @property
    def _branch_path(self) -> str:
        return f"projects/{self.project}/branches/{self.branch}"

    @property
    def _endpoint_path(self) -> str:
        return f"{self._branch_path}/endpoints/{self.endpoint}"

    def host(self) -> str:
        if not self._host:
            endpoints = _cli(["postgres", "list-endpoints", self._branch_path, "-p", self.profile])
            if not endpoints:
                raise RuntimeError(f"No endpoints found on {self._branch_path}")
            self._host = endpoints[0]["status"]["hosts"]["host"]
        return self._host

    def email(self) -> str:
        if not self._email:
            self._email = _cli(["current-user", "me", "-p", self.profile])["userName"]
        return self._email

    def token(self) -> str:
        if not self._token or (time.time() - self._token_at) > self.TOKEN_TTL_SECONDS:
            cred = _cli(
                ["postgres", "generate-database-credential", self._endpoint_path, "-p", self.profile]
            )
            self._token = cred["token"]
            self._token_at = time.time()
        return self._token

    def connect(self, database: str, autocommit: bool = True):
        conn = psycopg2.connect(
            host=self.host(),
            port=5432,
            dbname=database,
            user=self.email(),
            password=self.token(),
            sslmode="require",
            connect_timeout=30,
        )
        conn.autocommit = autocommit
        return conn
