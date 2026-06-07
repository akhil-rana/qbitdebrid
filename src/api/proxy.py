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
            max_retries=settings.cloudflare_retry_attempts,
            retry_backoff=settings.cloudflare_retry_backoff,
        )
        self.app = FastAPI()
        self._printed_urls = set()
        self._setup_routes()

    def _setup_routes(self):
        @self.app.api_route("/proxy/{torrent_hash}/{file_path:path}", methods=["GET", "HEAD"])
        async def stream_file(
            torrent_hash: str,
            file_path: str,
            request: Request,
        ):
            try:
                range_header = request.headers.get("range")
                
                torrent = self.daemon.get_torrent(torrent_hash)
                if not torrent:
                    # If the torrent isn't in our local cache yet (e.g. qBittorrent requested a file immediately on startup), fetch it directly
                    logger.info("proxy_fetching_unknown_torrent", torrent_hash=torrent_hash)
                    try:
                        qbit_torrents = await self.daemon.qbit_controller.get_torrents()
                        torrent = next((t for t in qbit_torrents if t.hash == torrent_hash), None)
                        if torrent:
                            self.daemon._known_torrents[torrent.hash] = torrent
                    except Exception as e:
                        logger.error("proxy_qbit_fetch_failed", error=str(e))

                if not torrent:
                    logger.error("proxy_torrent_not_found", torrent_hash=torrent_hash)
                    raise HTTPException(status_code=404, detail="Torrent not found")

                info_hash = torrent.info_hash

                logger.info(
                    "proxy_request",
                    torrent_hash=torrent_hash,
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
                    proxy_url = f"http://{request.client.host if request.client else '127.0.0.1'}:{self.settings.proxy_port}/proxy/{torrent_hash}/{encoded_file_path}"
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
                        if k.lower() in ["content-type", "content-length", "content-range"]
                    },
                    media_type="application/octet-stream",
                )

            except HTTPException:
                raise
            except Exception as e:
                logger.error("proxy_error", torrent_hash=torrent_hash, error=str(e))
                raise HTTPException(status_code=500, detail=str(e))


