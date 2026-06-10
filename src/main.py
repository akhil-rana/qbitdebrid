import asyncio
import uvicorn

from qbitdebrid.config import get_settings
from qbitdebrid.logging import configure_logging, get_logger
from qbitdebrid.clients.torbox import TorBoxClient
from qbitdebrid.clients.qbit import QBitController
from qbitdebrid.core.daemon import AutomationDaemon
from qbitdebrid.api.proxy import ProxyServer

logger = get_logger(__name__)


async def main():
    settings = get_settings()
    
    # Print non-sensitive active environment variables
    print("\n\033[94m====================================================\033[0m")
    print("\033[94m           ACTIVE PROXY ENVIRONMENT CONFIG          \033[0m")
    print("\033[94m====================================================\033[0m")
    print(f"  -> PROXY_HOST:                 \033[92m{settings.proxy_host}\033[0m")
    print(f"  -> PROXY_PORT:                 \033[92m{settings.proxy_port}\033[0m")
    print(f"  -> PROXY_MAX_CONNECTIONS:      \033[92m{settings.proxy_max_connections}\033[0m")
    print(f"  -> PROXY_CHUNK_SIZE:           \033[92m{settings.proxy_chunk_size} bytes ({settings.proxy_chunk_size // 1024} KB)\033[0m")
    print(f"  -> PROXY_PREFETCH_BUFFER_MB:   \033[92m{settings.proxy_prefetch_buffer_mb} MB\033[0m")
    print(f"  -> TORRENT_PROCESS_TAG:        \033[92m{settings.torrent_process_tag or '(Process All)'}\033[0m")
    print(f"  -> LOG_LEVEL:                  \033[92m{settings.log_level}\033[0m")
    print("\033[94m====================================================\033[0m\n")

    configure_logging(settings.log_level, settings.log_format)

    logger.info("starting_torqproxy", version="0.1.0")

    qbit = QBitController(
        host=settings.qbit_host,
        port=settings.qbit_port,
        username=settings.qbit_username,
        password=settings.qbit_password,
    )

    if not await qbit.connect():
        logger.warning("failed_to_connect_qbittorrent_initial_startup_monitoring_in_background")

    async with TorBoxClient(settings.torbox_api_key) as torbox:
        daemon = AutomationDaemon(
            torbox,
            qbit,
            proxy_host=settings.proxy_host,
            proxy_port=settings.proxy_port,
            poll_interval=settings.qbit_poll_interval,
            process_tag=settings.torrent_process_tag,
        )

        proxy = ProxyServer(settings, daemon)

        daemon_task = asyncio.create_task(daemon.start())

        is_debug = settings.log_level.upper() == "DEBUG"
        
        config = uvicorn.Config(
            proxy.app,
            host=settings.proxy_host,
            port=settings.proxy_port,
            log_level=settings.log_level.lower(),
            access_log=is_debug,
            server_header=False,
            timeout_graceful_shutdown=3,
        )
        server = uvicorn.Server(config)

        try:
            logger.info(
                "server_starting",
                host=settings.proxy_host,
                port=settings.proxy_port,
            )
            await server.serve()
        except KeyboardInterrupt:
            logger.info("shutdown_requested")
        finally:
            await daemon.stop()
            daemon_task.cancel()
            try:
                await asyncio.wait_for(daemon_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
            except Exception as e:
                logger.error("daemon_shutdown_error", error=str(e))


if __name__ == "__main__":
    asyncio.run(main())
