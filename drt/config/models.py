"""Pydantic models for drt project and sync configuration."""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class BearerAuth(BaseModel):
    type: Literal["bearer"]
    token: str | None = None          # 直書き（非推奨。env var を優先）
    token_env: str | None = None      # env var 名 → os.environ から解決


class ApiKeyAuth(BaseModel):
    type: Literal["api_key"]
    header: str = "X-API-Key"
    value: str | None = None
    value_env: str | None = None


class BasicAuth(BaseModel):
    type: Literal["basic"]
    username_env: str
    password_env: str


AuthConfig = Annotated[
    BearerAuth | ApiKeyAuth | BasicAuth,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Source (inline — kept for backward compat; prefer profiles.yml)
# ---------------------------------------------------------------------------

class SourceConfig(BaseModel):
    type: Literal["bigquery", "snowflake", "postgres", "duckdb"]
    project: str | None = None
    dataset: str | None = None
    credentials: str | None = None


# ---------------------------------------------------------------------------
# Project
# ---------------------------------------------------------------------------

class ProjectConfig(BaseModel):
    name: str
    version: str = "0.1"
    profile: str = "default"          # references ~/.drt/profiles.yml
    source: SourceConfig | None = None  # optional; profile is authoritative


# ---------------------------------------------------------------------------
# Destination
# ---------------------------------------------------------------------------

class DestinationConfig(BaseModel):
    type: Literal["rest_api"]
    url: str
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body_template: str | None = None
    auth: AuthConfig | None = None


# ---------------------------------------------------------------------------
# Sync options
# ---------------------------------------------------------------------------

class RateLimitConfig(BaseModel):
    requests_per_second: int = 10


class RetryConfig(BaseModel):
    max_attempts: int = 3
    initial_backoff: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff: float = 60.0
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504)


class SyncOptions(BaseModel):
    mode: Literal["full", "incremental"] = "full"
    batch_size: int = 100
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    on_error: Literal["skip", "fail"] = "fail"


class SyncConfig(BaseModel):
    name: str
    description: str = ""
    model: str
    destination: DestinationConfig
    sync: SyncOptions = Field(default_factory=SyncOptions)
