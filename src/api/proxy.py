from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse

from qbitdebrid.config import Settings
from qbitdebrid.core.daemon import AutomationDaemon
from qbitdebrid.services.streaming import StreamingService
from qbitdebrid.logging import get_logger

logger = get_logger(__name__)


class ProxyServer:
    def __init__(self, settings: Settings, daemon: AutomationDaemon):
        self.settings = settings
        self.daemon = daemon
        self.streaming_service = StreamingService(
            chunk_size=settings.proxy_chunk_size,
            max_retries=settings.cloudflare_retry_attempts,
            retry_backoff=settings.cloudflare_retry_backoff,
        )
        self.app = FastAPI()
        self._setup_routes()

    def _setup_routes(self):
        @self.app.get("/proxy/{torrent_hash}/{file_path:path}")
        async def stream_file(
            torrent_hash: str,
            file_path: str,
            request: Request,
        ):
            try:
                range_header = request.headers.get("range")

                if not range_header:
                    raise HTTPException(status_code=400, detail="Range header required")

                logger.info(
                    "proxy_request",
                    torrent_hash=torrent_hash,
                    file_path=file_path,
                    range=range_header,
                )

                range_start, range_end = self._parse_range(range_header)

                cached_link = self.daemon.get_cached_link(torrent_hash)
                if not cached_link:
                    raise HTTPException(status_code=404, detail="Torrent not cached")

                file_url = f"{cached_link.rstrip('/')}/{file_path}"

                status, headers, stream = await self.streaming_service.stream_range(
                    file_url,
                    range_start,
                    range_end,
                )

                return StreamingResponse(
                    stream,
                    status_code=status,
                    headers={
                        k: v
                        for k, v in headers.items()
                        if k.lower()
                        in ["content-type", "content-length", "content-range"]
                    },
                    media_type="application/octet-stream",
                )

            except Exception as e:
                logger.error("proxy_error", torrent_hash=torrent_hash, error=str(e))
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/health")
        async def health_check():
            return {"status": "ok"}

    def _parse_range(self, range_header: str) -> tuple[int, int]:
        try:
            range_part = range_header.split("=")[1]
            start, end = range_part.split("-")
            return int(start), int(end)
        except (IndexError, ValueError):
            raise ValueError("Invalid range header format")
