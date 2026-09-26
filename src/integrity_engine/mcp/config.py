"""MCP service settings."""
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MCPSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MCP_", extra="ignore")

    transport: Literal["stdio", "streamable-http"] = "streamable-http"
    host: str = "127.0.0.1"
    port: int = Field(default=8001, ge=1, le=65535)
    query_timeout_seconds: int = Field(default=5, ge=1, le=30)
    max_result_rows: int = Field(default=500, ge=1, le=5000)
