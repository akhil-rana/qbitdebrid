from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from typing import Optional

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
            prefetch_mb=settings.proxy_prefetch_buffer_mb,
            max_connections=settings.proxy_max_connections,
            max_splits=settings.proxy_max_splits,
            max_retries=settings.cloudflare_retry_attempts,
            retry_backoff=settings.cloudflare_retry_backoff,
        )
        self.app = FastAPI()
        self._printed_urls = set()
        self._setup_routes()

    def _setup_routes(self):
        @self.app.api_route("/proxy/{info_hash}/{file_path:path}", methods=["GET", "HEAD"])
        async def stream_file(
            info_hash: str,
            file_path: str,
            request: Request,
        ):
            try:
                range_header = request.headers.get("range")
                
                logger.info(
                    "proxy_request",
                    info_hash=info_hash,
                    file_path=file_path,
                    range=range_header or "bytes=0-",
                )

                # Fetch the direct unzipped link dynamically based on the file_path
                file_url = await self.daemon.torbox_client.get_direct_link(info_hash, file_path)
                
                if not file_url:
                    raise HTTPException(status_code=404, detail="Direct file link not found on TorBox")

                if file_url not in self._printed_urls:
                    from urllib.parse import quote
                    encoded_file_path = quote(file_path)
                    proxy_url = f"http://{request.client.host if request.client else '127.0.0.1'}:{self.settings.proxy_port}/proxy/{info_hash}/{encoded_file_path}"
                    print(f"\n{'='*60}\nPROXY DOWNLOAD LINK (Paste in browser):\n{proxy_url}\n{'='*60}\n")
                    self._printed_urls.add(file_url)
                
                file_size = None
                
                if not range_header:
                    if request.method == "HEAD":
                        file_size = await self.streaming_service.get_file_size(file_url)
                        if file_size is not None:
                            return StreamingResponse(
                                content=iter([]), 
                                status_code=200, 
                                headers={"Accept-Ranges": "bytes", "Content-Length": str(file_size)}
                            )
                    range_header = "bytes=0-"

                # Stream directly from the exact file URL, passing the range exactly as the client requested
                status, headers, stream = await self.streaming_service.stream_range(
                    file_url,
                    range_header,
                )

                return StreamingResponse(
                    stream,
                    status_code=status,
                    headers={
                        k: v
                        for k, v in headers.items()
                        if k.lower() in ["content-type", "content-length", "content-range", "connection", "keep-alive"]
                    },
                    media_type="application/octet-stream",
                )

            except HTTPException:
                raise
            except Exception as e:
                logger.error("proxy_error", info_hash=info_hash, error=str(e))
                raise HTTPException(status_code=500, detail=str(e))


