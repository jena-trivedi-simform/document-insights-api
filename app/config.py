from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # MongoDB
    mongodb_url: str = "mongodb://mongodb:27017"
    mongodb_db_name: str = "document_insights"

    # Redis
    redis_url: str = "redis://redis:6379"

    # Per-user rate limit: max queued + processing documents
    max_active_per_user: int = 3

    # Content cache TTL in seconds (24 hours default)
    content_cache_ttl: int = 86400

    # Worker behaviour
    worker_poll_interval: float = 2.0
    worker_min_delay: int = 10
    worker_max_delay: int = 30
    worker_failure_rate: float = 0.1  # 10% random failure rate

    # Retry mechanism for failed jobs
    max_retries: int = 3
    retry_backoff_base: int = 5  # seconds; delay = base * 2^attempt (5s, 10s, 20s)

    app_name: str = "Document Insights API"
    debug: bool = False

    model_config = {"env_file": ".env"}


@lru_cache
def get_settings() -> Settings:
    return Settings()
