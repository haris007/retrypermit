from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_SOURCE_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = Path(os.getenv("RETRYPERMIT_PROJECT_ROOT", _SOURCE_ROOT)).resolve()
if not (PROJECT_ROOT / "fixtures").is_dir() and (Path.cwd() / "fixtures").is_dir():
    PROJECT_ROOT = Path.cwd().resolve()


class Settings(BaseSettings):
    """Runtime configuration with fail-closed cloud defaults."""

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "RetryPermit"
    app_env: str = "local"
    demo_mode: bool = True
    use_in_memory_store: bool = True
    use_fake_model: bool = True
    allow_local_service_auth: bool = False
    cloud_deployment_verified: bool = False
    log_level: str = "INFO"
    demo_admin_token: str = ""

    google_cloud_project: str = ""
    google_cloud_location: str = "global"
    google_genai_use_enterprise: str = "TRUE"
    gemini_model: str = "gemini-3.7-flash"
    cloud_run_region: str = "us-central1"

    pubsub_topic: str = "orders.dlq"
    pubsub_subscription: str = ""
    cloud_tasks_location: str = "us-central1"
    cloud_tasks_queue: str = "retrypermit-processing"
    service_base_url: str = ""
    oidc_audience: str = ""
    pubsub_push_service_account: str = ""
    cloud_tasks_service_account: str = ""
    recovery_service_account: str = ""

    tenant_id: str = "synthetic-demo"
    policy_fixture_path: Path = Field(
        default=PROJECT_ROOT / "fixtures" / "policies" / "runbook-v1.json"
    )
    frontend_dist_path: Path = Field(default=PROJECT_ROOT / "frontend" / "dist")
    replay_lease_seconds: int = 30
    replay_retry_base_seconds: float = 1.0
    replay_retry_max_seconds: float = 10.0
    inbox_lease_seconds: int = 60
    model_timeout_seconds: float = 20.0
    downstream_timeout_seconds: float = 10.0
    policy_extraction_timeout_seconds: float = 45.0
    recovery_batch_size: int = 100
    triage_concurrency: int = Field(default=4, ge=1, le=32)
    demo_recheck_seconds: float = Field(default=10.0, ge=0.01, le=60.0)
    simulated_transient_recovery_seconds: float = Field(default=10.0, ge=0.01, le=300.0)

    @model_validator(mode="after")
    def reject_unsafe_cloud_configuration(self) -> "Settings":
        if not self.use_in_memory_store and not self.google_cloud_project:
            raise ValueError("GOOGLE_CLOUD_PROJECT is required for the Firestore store")
        if not self.demo_mode:
            required = {
                "DEMO_ADMIN_TOKEN": self.demo_admin_token,
                "SERVICE_BASE_URL": self.service_base_url,
                "OIDC_AUDIENCE": self.oidc_audience,
                "PUBSUB_PUSH_SERVICE_ACCOUNT": self.pubsub_push_service_account,
                "PUBSUB_SUBSCRIPTION": self.pubsub_subscription,
                "CLOUD_TASKS_SERVICE_ACCOUNT": self.cloud_tasks_service_account,
                "RECOVERY_SERVICE_ACCOUNT": self.recovery_service_account,
            }
            missing = [name for name, value in required.items() if not value]
            if missing:
                raise ValueError(
                    f"cloud mode is missing required settings: {', '.join(missing)}"
                )
            if self.use_in_memory_store:
                raise ValueError("cloud mode refuses USE_IN_MEMORY_STORE=true")
            if self.use_fake_model:
                raise ValueError("cloud mode refuses USE_FAKE_MODEL=true")
        if self.allow_local_service_auth and not self.demo_mode:
            raise ValueError("local service-auth bypass is allowed only in demo mode")
        return self

    @property
    def mode_label(self) -> str:
        if self.use_in_memory_store:
            return "Local / in-memory"
        if self.demo_mode or self.use_fake_model:
            return "Hybrid / development"
        return "Google Cloud"

    @property
    def task_parent(self) -> str:
        return (
            f"projects/{self.google_cloud_project}/locations/"
            f"{self.cloud_tasks_location}/queues/{self.cloud_tasks_queue}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
