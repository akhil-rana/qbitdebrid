from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
from urllib.parse import quote

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
        )
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            yield
            # Shutdown
            import asyncio
            try:
                await asyncio.wait_for(self.streaming_service.close(), timeout=3.0)
            except asyncio.TimeoutError:
                logger.warning("streaming_service_shutdown_timeout_forcing_exit")
            except Exception as e:
                logger.error("streaming_service_shutdown_error", error=str(e))
            
        self.app = FastAPI(lifespan=lifespan)
        self._setup_routes()

    def _setup_routes(self):
        @self.app.get("/health")
        async def health_check():
            return {"status": "ok"}

        @self.app.api_route("/proxy/{info_hash}/{file_path:path}", methods=["GET", "HEAD"])
        async def stream_file(
            info_hash: str,
            file_path: str,
            request: Request,
        ):
            try:
                range_header = request.headers.get("range")
                logger.debug(
                    "proxy_request",
                    info_hash=info_hash,
                    file_path=file_path,
                    range=range_header or "bytes=0-",
                )

                # Fetch the direct unzipped link dynamically based on the file_path
                file_info = await self.daemon.torbox_client.get_direct_link(info_hash, file_path)

                if not file_info:
                    raise HTTPException(status_code=404, detail="Direct file link not found on TorBox")

                file_url, true_file_size = file_info

                if not range_header:
                    if request.method == "HEAD":
                        return StreamingResponse(
                            content=iter([]), 
                            status_code=200, 
                            headers={"Accept-Ranges": "bytes", "Content-Length": str(true_file_size)}
                        )
                    range_header = "bytes=0-"

                # Stream directly from the exact file URL, passing the range exactly as the client requested
                status, headers, stream = await self.streaming_service.stream_range(
                    file_url,
                    range_header,
                    request,
                    true_file_size,
                )

                response_headers = {
                    k: v
                    for k, v in headers.items()
                    if k.lower() in ["content-type", "content-length", "content-range", "connection", "keep-alive"]
                }
                response_headers["Server"] = "qbitdebrid"

                return StreamingResponse(
                    stream,
                    status_code=status,
                    headers=response_headers,
                    media_type="application/octet-stream",
                )

            except HTTPException:
                raise
            except Exception as e:
                logger.error("proxy_error", info_hash=info_hash, error=str(e))
                raise HTTPException(status_code=500, detail=str(e))


