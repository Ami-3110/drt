"""JSON Schema generation from Pydantic models.

Used for:
- YAML editor autocomplete (drt validate --emit-schema)
- LLM-readable API reference (docs/llm/)
"""

import json
from pathlib import Path

from drt.config.models import ProjectConfig, SyncConfig


def generate_project_schema() -> dict:
    return ProjectConfig.model_json_schema()


def generate_sync_schema() -> dict:
    return SyncConfig.model_json_schema()


def write_schemas(output_dir: Path) -> list[Path]:
    """Write drt_project.schema.json and sync.schema.json to output_dir.

    Returns list of written file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    project_path = output_dir / "drt_project.schema.json"
    project_path.write_text(json.dumps(generate_project_schema(), indent=2))
    written.append(project_path)

    sync_path = output_dir / "sync.schema.json"
    sync_path.write_text(json.dumps(generate_sync_schema(), indent=2))
    written.append(sync_path)

    return written
