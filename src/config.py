from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    torbox_api_key: str
    torbox_api_base_url: str = "https://api.torbox.app/v1"

    qbit_host: str = "127.0.0.1"
    qbit_port: int = 8080
    qbit_username: str | None = None
    qbit_password: str | None = None
    qbit_poll_interval: int = 3

    proxy_host: str = "127.0.0.1"
    proxy_port: int = 8888
    proxy_max_connections: int = 16
    proxy_chunk_size: int = 4194304
    proxy_prefetch_buffer_mb: int = 512
    torrent_process_tag: str = ""

    log_level: str = "INFO"
    log_format: str = "json"

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    import os
    settings = Settings()
    
    overrides = []
    for var in ["PROXY_CHUNK_SIZE", "PROXY_MAX_CONNECTIONS", "PROXY_PREFETCH_BUFFER_MB", "PROXY_HOST", "PROXY_PORT"]:
        if var in os.environ:
            overrides.append(f"{var}={os.environ[var]}")
    if overrides:
        print(f"\n\033[93m[WARNING] Terminal environment variables are OVERRIDING your .env file:\033[0m")
        print(f"  -> \033[91m{', '.join(overrides)}\033[0m")
        print(f"  -> To fix and use your .env file values, run this command in your terminal:")
        print(f"     \033[92munset {' '.join(v.split('=')[0] for v in overrides)}\033[0m\n")
        
    return settings
