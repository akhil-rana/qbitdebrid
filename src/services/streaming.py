import aiohttp
import asyncio
import time
from typing import AsyncIterator, Optional, Tuple, Dict, Any
from dataclasses import dataclass
from qbitdebrid.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ActiveStream:
    url: str
    start_byte: int
    queue: asyncio.Queue
    worker_task: asyncio.Task
    is_eof: bool = False
    last_accessed: float = 0.0
    bytes_produced: int = 0
    total_requested_length: Optional[int] = None
    excess_bytes: bytes = b""
    is_reading: bool = False


class StreamingService:
    def __init__(
        self,
        chunk_size: int = 524288,
        prefetch_mb: int = 512,
        max_connections: int = 16,
    ):
        self.chunk_size = chunk_size
        self.prefetch_mb = prefetch_mb
        self.max_connections = max_connections
        
        connector = aiohttp.TCPConnector(
            limit=200, 
            ttl_dns_cache=300, 
            use_dns_cache=True,
            force_close=False,
            enable_cleanup_closed=True
        )
        
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        self.session = aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": user_agent}
        )
        
        self._file_sizes: Dict[str, int] = {}
        self._active_streams: Dict[str, ActiveStream] = {}
        
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def close(self):
        """Explicitly cleans up background tasks and TCP connections on shutdown."""
        if hasattr(self, "_cleanup_task"):
            self._cleanup_task.cancel()
            
        tasks_to_await = []
        for stream_id, stream in list(self._active_streams.items()):
            if stream.worker_task and not stream.worker_task.done():
                stream.worker_task.cancel()
                tasks_to_await.append(stream.worker_task)
                
        if tasks_to_await:
            await asyncio.gather(*tasks_to_await, return_exceptions=True)
            
        self._active_streams.clear()
            
        if hasattr(self, "session") and self.session and not self.session.closed:
            await self.session.close()

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(30)
            now = time.time()
            to_delete = []
            for stream_id, stream in self._active_streams.items():
                if now - stream.last_accessed > 60:  # Terminate idle streams after 60s
                    stream.worker_task.cancel()
                    to_delete.append(stream_id)
            for sid in to_delete:
                del self._active_streams[sid]

    async def _prefetch_worker(self, stream_id: str, url: str, headers: dict, queue: asyncio.Queue, stream: ActiveStream, file_size: int):
        """Downloads continuously from the provider and pushes chunks into the bounded memory queue."""
        logger.info("prefetch_worker_starting", stream_id=stream_id, range=headers.get("Range"))
            
        current_offset = stream.start_byte
        # Enforce a strict 15-second read timeout. If Cloudflare stalls without closing the TCP socket,
        # this forces a TimeoutError, instantly triggering our auto-resume loop to reconnect and fix the stall!
        timeout = aiohttp.ClientTimeout(total=None, connect=10.0, sock_read=15.0)
        
        try:
            connector = aiohttp.TCPConnector(limit=self.max_connections)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                
                # Auto-Resume Connection Loop
                while current_offset < file_size and not self.session.closed:
                    worker_headers = headers.copy()
                    worker_headers["Range"] = f"bytes={current_offset}-"
                    
                    try:
                        async with session.get(url, headers=worker_headers, allow_redirects=True) as resp:
                            if resp.status not in (200, 206):
                                logger.error("prefetch_bad_status", status=resp.status)
                                await queue.put(Exception(f"Bad status: {resp.status}"))
                                return

                            while not resp.content.at_eof() and not self.session.closed:
                                chunk = await resp.content.read(self.chunk_size)
                                if chunk:
                                    await queue.put(chunk)
                                    current_offset += len(chunk)
                                else:
                                    break
                                    
                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        logger.warning("prefetch_connection_lost_reconnecting", offset=current_offset, error=str(e))
                        await asyncio.sleep(0.5)
                        continue  # Safe retry and resume
                        
                    if current_offset < file_size:
                        logger.warning("prefetch_connection_closed_by_cdn_resuming", offset=current_offset)
                        await asyncio.sleep(0.2)
                        continue
                    else:
                        break
                            
        except Exception as e:
            if not isinstance(e, asyncio.CancelledError):
                logger.debug("prefetch_worker_interrupted", error=str(e))
            await queue.put(e) # Pass exception to client
        finally:
            stream.is_eof = True
            await queue.put(None) # Signal EOF to client
            logger.info("prefetch_worker_finished", stream_id=stream_id)

    async def stream_range(
        self,
        url: str,
        range_header: str,
        request: Any,
        file_size: int,
        headers: Optional[dict[str, str]] = None,
    ) -> Tuple[int, dict[str, str], AsyncIterator[bytes]]:
        if headers is None:
            headers = {}

        # 1. Parse the requested range
        req_start = 0
        req_end = None
        try:
            range_part = range_header.replace("bytes=", "")
            parts = range_part.split("-")
            req_start = int(parts[0]) if parts[0] else 0
            req_end = int(parts[1]) if len(parts) > 1 and parts[1] else None
        except Exception:
            pass

        # 2. Cap req_end to avoid Content-Length mismatch
        if file_size and req_end and req_end >= file_size:
            req_end = file_size - 1

        content_length = None
        if file_size:
            content_length = (req_end - req_start + 1) if req_end is not None else (file_size - req_start)

        # 3. Check for an active stream starting at this byte
        stream_id = f"{url}_{req_start}"
        
        if stream_id in self._active_streams and not self._active_streams[stream_id].is_reading:
            stream = self._active_streams[stream_id]
            stream.last_accessed = time.time()
            stream.total_requested_length = content_length
            logger.info("resuming_active_stream", stream_id=stream_id)
        else:
            # 4. Create a new prefetch worker
            prefetch_headers = headers.copy()
            prefetch_headers["Range"] = f"bytes={req_start}-"
            
            max_queue_chunks = max(4, (self.prefetch_mb * 1024 * 1024) // self.chunk_size)
            
            queue = asyncio.Queue(maxsize=max_queue_chunks)
            stream = ActiveStream(
                url=url,
                start_byte=req_start,
                queue=queue,
                worker_task=None,
                total_requested_length=content_length
            )
            
            worker_task = asyncio.create_task(self._prefetch_worker(stream_id, url, prefetch_headers, queue, stream, file_size))
            stream.worker_task = worker_task
            self._active_streams[stream_id] = stream

        # 5. Construct response headers
        response_headers = {
            "Accept-Ranges": "bytes",
            "Connection": "keep-alive"
        }
        if content_length:
            response_headers["Content-Length"] = str(content_length)
        if file_size:
            response_headers["Content-Range"] = f"bytes {req_start}-{req_end if req_end is not None else file_size-1}/{file_size}"
        else:
            response_headers["Content-Range"] = f"bytes {req_start}-{req_end if req_end is not None else ''}/*"

        # 6. Build the non-blocking consumer generator
        async def stream_body() -> AsyncIterator[bytes]:
            bytes_sent = 0
            target_bytes = stream.total_requested_length
            stream.is_reading = True
            
            try:
                while True:
                    # Explicitly check if qBittorrent slammed the socket closed before yielding
                    if await request.is_disconnected():
                        logger.debug("client_disconnected_aborting_stream", stream_id=stream_id)
                        stream.worker_task.cancel()
                        if stream_id in self._active_streams:
                            del self._active_streams[stream_id]
                        break

                    if target_bytes and bytes_sent >= target_bytes:
                        next_byte = stream.start_byte + stream.bytes_produced
                        next_stream_id = f"{url}_{next_byte}"
                        self._active_streams[next_stream_id] = stream
                        if stream_id in self._active_streams:
                            del self._active_streams[stream_id]
                        logger.debug("stream_paused_and_handed_over", next_stream_id=next_stream_id)
                        break

                    if stream.excess_bytes:
                        chunk = stream.excess_bytes
                        stream.excess_bytes = b""
                    else:
                        chunk = await stream.queue.get()
                        stream.last_accessed = time.time()
                    
                    if chunk is None:
                        break
                    if isinstance(chunk, Exception):
                        raise chunk
                        
                    if target_bytes and (bytes_sent + len(chunk)) > target_bytes:
                        excess = (bytes_sent + len(chunk)) - target_bytes
                        valid_chunk = chunk[:-excess]
                        
                        stream.excess_bytes = chunk[-excess:]
                        
                        yield valid_chunk
                        bytes_sent += len(valid_chunk)
                        stream.bytes_produced += len(valid_chunk)
                        
                        next_byte = stream.start_byte + stream.bytes_produced
                        next_stream_id = f"{url}_{next_byte}"
                        self._active_streams[next_stream_id] = stream
                        if stream_id in self._active_streams:
                            del self._active_streams[stream_id]
                        logger.debug("stream_paused_and_handed_over_on_chunk_boundary", next_stream_id=next_stream_id)
                        break
                    else:
                        yield chunk
                        bytes_sent += len(chunk)
                        stream.bytes_produced += len(chunk)
                        
            except asyncio.CancelledError:
                stream.worker_task.cancel()
                if stream_id in self._active_streams:
                    del self._active_streams[stream_id]
                raise
            finally:
                stream.is_reading = False

        return 206, response_headers, stream_body()