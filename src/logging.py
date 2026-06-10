import logging
import structlog
from typing import Any


def configure_logging(log_level: str = "INFO", log_format: str = "json") -> None:
    target_level = getattr(logging, log_level.upper())
    
    logging.basicConfig(
        format="%(message)s",
        level=target_level,
    )
    
    if target_level > logging.DEBUG:
        # Suppress verbose third-party loggers that bypass structlog in standard operation
        logging.getLogger("urllib3").setLevel(logging.CRITICAL)
        logging.getLogger("qbittorrentapi").setLevel(logging.CRITICAL)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        logging.getLogger("aiohttp").setLevel(logging.WARNING)
        logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    else:
        # In DEBUG mode, explicitly allow all raw third-party text logs to bleed through for deep tracing
        logging.getLogger("urllib3").setLevel(logging.DEBUG)
        logging.getLogger("qbittorrentapi").setLevel(logging.DEBUG)
        logging.getLogger("httpx").setLevel(logging.DEBUG)
        logging.getLogger("httpcore").setLevel(logging.DEBUG)
        logging.getLogger("aiohttp").setLevel(logging.DEBUG)
        logging.getLogger("uvicorn.error").setLevel(logging.DEBUG)
        logging.getLogger("uvicorn.access").setLevel(logging.DEBUG)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            (
                structlog.processors.JSONRenderer()
                if log_format == "json"
                else structlog.dev.ConsoleRenderer()
            ),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)
