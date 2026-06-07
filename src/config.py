from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    torbox_api_key: str
    torbox_api_base_url: str = "https://api.torbox.app/v1"
    torbox_cache_threshold: float = 0.95

    qbit_host: str = "127.0.0.1"
    qbit_port: int = 8080
    qbit_username: str | None = None
    qbit_password: str | None = None
    qbit_poll_interval: int = 3

    proxy_host: str = "127.0.0.1"
    proxy_port: int = 8888
    proxy_max_connections: int = 1
    proxy_chunk_size: int = 65536

    log_level: str = "INFO"
    log_format: str = "json"

    enable_jit_prefetch: bool = True
    enable_zip_extraction: bool = True
    cloudflare_resilience_enabled: bool = True
    cloudflare_retry_attempts: int = 3
    cloudflare_retry_backoff: float = 2.0

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
