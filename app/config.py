"""
Centralised typed configuration settings for PingGuard.

Loads environment variables from `.env` or the environment, validates types and constraints,
and provides computed database connection strings for both async (FastAPI) and sync (Celery, Alembic) engines.
"""

from functools import lru_cache
from typing import Literal
from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

WEAK_PASSWORDS = {
    "password",
    "postgres",
    "admin",
    "root",
    "123456",
    "change-me",
    "change-me-generate-a-random-one",
}


class Settings(BaseSettings):
    """Application settings backed by environment variables and .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 0. Environment Profile & Authentication
    environment: Literal["dev", "test", "prod"] = "dev"
    api_key: SecretStr

    # 1. Database Configuration
    database_url: str

    # 2. Redis Broker & Backend Configuration
    redis_broker_url: str = "redis://localhost:6379/0"
    redis_result_backend_url: str = "redis://localhost:6379/1"

    # 3. Celery Beat Periodic Sweep Frequency (seconds)
    sweep_interval_seconds: float = 15.0
    celerybeat_schedule_filename: str = "celerybeat-schedule"

    # 4. Network Resilience & HTTPX Prober Settings
    http_connect_timeout: float = 2.0
    http_read_timeout: float = 5.0
    http_write_timeout: float = 5.0
    http_pool_timeout: float = 2.0
    http_max_response_bytes: int = 1048576
    http_max_redirects: int = 5
    http_user_agent: str = "PingGuard/1.0"
    http_keep_alive_user_agent: str = "PingGuard-KeepAlive/1.0"

    # 5. Probe Total Deadline & Celery Task Time Limits
    probe_total_timeout_seconds: float = 8.0
    celery_soft_time_limit: int = 10
    celery_hard_time_limit: int = 15

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: SecretStr) -> SecretStr:
        """Validate that api_key is at least 24 characters and not a weak placeholder."""
        val = v.get_secret_value()
        if len(val) < 24:
            raise ValueError("API_KEY must be at least 24 characters in length")
        if val.lower() in WEAK_PASSWORDS or val.lower().startswith("change-me"):
            raise ValueError("API_KEY cannot be a known weak or placeholder value")
        return v

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str | None) -> str:
        """Validate that database_url is provided and parses into a valid SQLAlchemy URL."""
        if not v or not isinstance(v, str) or not v.strip():
            raise ValueError("DATABASE_URL must be provided and non-empty")
        try:
            url = make_url(v)
            if not url.database:
                raise ValueError("DATABASE_URL must include a database name")
        except Exception as e:
            raise ValueError(f"Invalid DATABASE_URL: {e}") from e
        return v.strip()

    @field_validator(
        "sweep_interval_seconds",
        "http_connect_timeout",
        "http_read_timeout",
        "http_write_timeout",
        "http_pool_timeout",
        "probe_total_timeout_seconds",
    )
    @classmethod
    def validate_positive_float(cls, v: float) -> float:
        """Validate that timeout and interval values are strictly positive."""
        if v <= 0:
            raise ValueError("Timeout and interval values must be greater than zero")
        return v

    @field_validator("http_max_response_bytes", "celery_soft_time_limit", "celery_hard_time_limit")
    @classmethod
    def validate_positive_int(cls, v: int) -> int:
        """Validate that integer values are strictly positive."""
        if v <= 0:
            raise ValueError("Value must be greater than zero")
        return v

    @field_validator("http_max_redirects")
    @classmethod
    def validate_max_redirects(cls, v: int) -> int:
        """Validate that redirect count is between 0 and 20."""
        if not (0 <= v <= 20):
            raise ValueError("http_max_redirects must be between 0 and 20")
        return v

    @model_validator(mode="after")
    def validate_timeout_budget(self) -> "Settings":
        """
        Validate that probe total timeout and Celery time limits align.

        Requires:
        - celery_soft_time_limit >= probe_total_timeout_seconds + 2
        - celery_hard_time_limit >= celery_soft_time_limit + 3
        """
        if self.celery_soft_time_limit < self.probe_total_timeout_seconds + 2:
            raise ValueError(
                f"celery_soft_time_limit ({self.celery_soft_time_limit}) must be at least "
                f"probe_total_timeout_seconds + 2 ({self.probe_total_timeout_seconds + 2:.1f})"
            )
        if self.celery_hard_time_limit < self.celery_soft_time_limit + 3:
            raise ValueError(
                f"celery_hard_time_limit ({self.celery_hard_time_limit}) must be at least "
                f"celery_soft_time_limit + 3 ({self.celery_soft_time_limit + 3})"
            )
        return self

    @model_validator(mode="after")
    def validate_db_password(self) -> "Settings":
        """Reject default or weak database passwords when environment is prod."""
        if self.environment == "prod":
            url = make_url(self.database_url)
            pwd = url.password
            if not pwd or pwd.lower() in WEAK_PASSWORDS or pwd.lower().startswith("change-me"):
                raise ValueError(
                    f"Production environment rejects default or weak database password: '{pwd}'. "
                    "Set a strong random password in .env."
                )
        return self

    @property
    def async_database_url(self) -> str:
        """Return the database URL normalized to the postgresql+asyncpg driver for AsyncEngine."""
        url = make_url(self.database_url)
        return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)

    @property
    def sync_database_url(self) -> str:
        """Return the database URL normalized to the postgresql+psycopg2 driver for sync Engine / Alembic."""
        url = make_url(self.database_url)
        return url.set(drivername="postgresql+psycopg2").render_as_string(hide_password=False)


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings singleton."""
    return Settings()
