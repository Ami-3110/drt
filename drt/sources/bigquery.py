"""BigQuery source implementation.

Requires: pip install drt-core[bigquery]

Authentication methods:
  application_default — uses gcloud ADC (recommended for local dev)
  keyfile             — explicit service account JSON file
"""

from __future__ import annotations

import os
from collections.abc import Iterator

from drt.config.credentials import ProfileConfig


class BigQuerySource:
    """Extract records from Google BigQuery."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict]:
        """Run a SQL query and yield rows as dicts."""
        client = self._build_client(config)
        rows = client.query(query).result()
        for row in rows:
            yield dict(row)

    def test_connection(self, config: ProfileConfig) -> bool:
        """Return True if BigQuery is reachable with the given profile."""
        try:
            client = self._build_client(config)
            client.query("SELECT 1").result()
            return True
        except Exception:
            return False

    def _build_client(self, config: ProfileConfig):  # type: ignore[return]
        try:
            from google.cloud import bigquery  # type: ignore[import]
        except ImportError as e:
            raise ImportError(
                "BigQuery support requires: pip install drt-core[bigquery]"
            ) from e

        if config.method == "keyfile" and config.keyfile:
            from google.oauth2 import service_account  # type: ignore[import]

            creds = service_account.Credentials.from_service_account_file(
                os.path.expanduser(config.keyfile)
            )
            return bigquery.Client(project=config.project, credentials=creds)

        # Application Default Credentials (gcloud auth application-default login)
        return bigquery.Client(project=config.project)
