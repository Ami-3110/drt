"""Model reference resolver — translates `ref()` to runnable SQL.

Mirrors dbt's ref() concept but without requiring a dbt manifest.
Resolution order:
  1. syncs/models/{name}.sql  — raw SQL file wins
  2. ref('name')              — SELECT * FROM {dataset}.{name}
  3. anything else            — used as SQL directly

Future: integrate with dbt manifest.json for full dbt compatibility.
"""

from __future__ import annotations

import re
from pathlib import Path

from drt.config.credentials import BigQueryProfile, DuckDBProfile, PostgresProfile, ProfileConfig

# Matches: ref('table') or ref("table")
_REF_PATTERN = re.compile(r"""^ref\(\s*['"]([^'"]+)['"]\s*\)$""", re.IGNORECASE)


def parse_ref(model_str: str) -> str | None:
    """Extract table name from ref() syntax.

    Returns:
        Table name string if model_str matches ref() pattern, else None.

    Examples:
        >>> parse_ref("ref('new_users')")
        'new_users'
        >>> parse_ref("ref(\\"orders\\")")
        'orders'
        >>> parse_ref("SELECT * FROM orders")
        None
    """
    m = _REF_PATTERN.match(model_str.strip())
    return m.group(1) if m else None


def resolve_model_ref(
    model_str: str,
    project_dir: Path,
    profile: ProfileConfig,
) -> str:
    """Resolve a model reference to a runnable SQL query.

    Args:
        model_str: Value of the ``model:`` field in sync YAML.
            Can be ref('table_name'), a raw SQL string, or a table name.
        project_dir: Root of the drt project (contains syncs/).
        profile: Resolved profile (supplies dataset for ref() expansion).

    Returns:
        A SQL query string ready to send to the source.
    """
    table_name = parse_ref(model_str)

    if table_name is not None:
        # Check for a hand-written SQL file first
        sql_file = project_dir / "syncs" / "models" / f"{table_name}.sql"
        if sql_file.exists():
            return sql_file.read_text().strip()
        # Fall back to qualified table SELECT — syntax differs by source
        if isinstance(profile, BigQueryProfile):
            return f"SELECT * FROM `{profile.dataset}`.`{table_name}`"
        if isinstance(profile, DuckDBProfile):
            return f"SELECT * FROM {table_name}"
        if isinstance(profile, PostgresProfile):
            return f'SELECT * FROM "{table_name}"'
        return f"SELECT * FROM {table_name}"

    # Not a ref() — treat as raw SQL or bare table name
    return model_str
