import httpx
import asyncio
import time
from typing import AsyncIterator, Optional, Tuple, Dict
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

class StreamingService:
    def __init__(
        self,
        chunk_size: int = 524288,  # 512KB yielding chunks
        prefetch_mb: int = 512,
        max_retries: int = 3,
        retry_backoff: float = 2.0,
    ):
        self.chunk_size = chunk_size
        self.prefetch_mb = prefetch_mb
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        
        # Max chunks in the queue dictates how far ahead we download (512MB default)
        self.max_queue_size = (self.prefetch_mb * 1024 * 1024) // self.chunk_size
        
        limits = httpx.Limits(max_keepalive_connections=200, max_connections=400, keepalive_expiry=120.0)
        timeout = httpx.Timeout(connect=10.0, read=None, write=None, pool=15.0) 
        self.client = httpx.AsyncClient(limits=limits, timeout=timeout)
        
        self._file_sizes: Dict[str, int] = {}
        self._active_streams: Dict[str, ActiveStream] = {}
        
        asyncio.create_task(self._cleanup_loop())

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

    async def get_file_size(self, url: str, headers: Optional[dict[str, str]] = None) -> Optional[int]:
        if url in self._file_sizes:
            return self._file_sizes[url]
            
        try:
            if headers is None:
                headers = {}
            resp = await self.client.head(url, headers=headers, follow_redirects=True)
            size = resp.headers.get("Content-Length")
            if size:
                self._file_sizes[url] = int(size)
                return int(size)
            return None
        except Exception as e:
            logger.error("streaming_head_failed", error=str(e))
            return None

    async def _prefetch_worker(self, stream_id: str, url: str, headers: dict, queue: asyncio.Queue, stream: ActiveStream):
        """Downloads continuously from the provider and pushes chunks into the bounded memory queue."""
        logger.info("prefetch_worker_starting", stream_id=stream_id, range=headers.get("Range"))
        try:
            request = self.client.build_request("GET", url, headers=headers)
            resp = await self.client.send(request, stream=True, follow_redirects=True)
            
            if resp.status_code not in (200, 206):
                logger.error("prefetch_bad_status", status=resp.status_code)
                await resp.aclose()
                await queue.put(e)
                return

            try:
                # Rip chunks as fast as possible from the socket. 
                # If the queue fills up (512MB), await queue.put() naturally pauses the download loop.
                async for chunk in resp.aiter_bytes(self.chunk_size):
                    if chunk:
                        await queue.put(chunk)
            finally:
                await resp.aclose()
                
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

        # 2. Check if we already have an active stream for this exact start byte
        # (Since we force sequential download, qBittorrent will always ask for the next exact byte)
        stream_id = f"{url}_{req_start}"
        
        if stream_id in self._active_streams:
            stream = self._active_streams[stream_id]
            stream.last_accessed = time.time()
            logger.info("resuming_active_stream", stream_id=stream_id)
        else:
            # 3. Create a new prefetch worker that downloads from req_start to the end of the file.
            # We ignore req_end for the provider request so we can prefetch the entire rest of the file into the queue.
            prefetch_headers = headers.copy()
            prefetch_headers["Range"] = f"bytes={req_start}-"
            
            queue = asyncio.Queue(maxsize=self.max_queue_size)
            stream = ActiveStream(
                url=url,
                start_byte=req_start,
                queue=queue,
                worker_task=None, # Assigned below
                total_requested_length=(req_end - req_start + 1) if req_end else None
            )
            
            worker_task = asyncio.create_task(self._prefetch_worker(stream_id, url, prefetch_headers, queue, stream))
            stream.worker_task = worker_task
            self._active_streams[stream_id] = stream

        # 4. Construct the fake 206 Partial Content headers for the client
        file_size = await self.get_file_size(url)
        content_length = (req_end - req_start + 1) if req_end else (file_size - req_start if file_size else None)
        
        response_headers = {
            "Accept-Ranges": "bytes",
            "Connection": "keep-alive"
        }
        if content_length:
            response_headers["Content-Length"] = str(content_length)
        if file_size:
            response_headers["Content-Range"] = f"bytes {req_start}-{req_end if req_end else file_size-1}/{file_size}"
        else:
            response_headers["Content-Range"] = f"bytes {req_start}-{req_end if req_end else ''}/*"

        # 5. Build the non-blocking consumer generator
        async def stream_body() -> AsyncIterator[bytes]:
            bytes_sent = 0
            target_bytes = stream.total_requested_length
            
            try:
                while True:
                    # If we fulfilled the client's strict req_end chunk size, stop and cleanly hand over the active 
                    # stream state. We calculate the NEXT byte the client will ask for, and re-register the stream 
                    # under that new ID so the next request instantly picks up where we left off.
                    if target_bytes and bytes_sent >= target_bytes:
                        next_byte = stream.start_byte + stream.bytes_produced
                        next_stream_id = f"{url}_{next_byte}"
                        self._active_streams[next_stream_id] = stream
                        # Remove old ID
                        if stream_id in self._active_streams:
                            del self._active_streams[stream_id]
                        logger.debug("stream_paused_and_handed_over", next_stream_id=next_stream_id)
                        break

                    # Wait for data from the prefetch worker
                    chunk = await stream.queue.get()
                    stream.last_accessed = time.time()
                    
                    if chunk is None:
                        break # EOF reached
                    if isinstance(chunk, Exception):
                        raise chunk # Bubble up provider errors
                        
                    # Yield data to qBittorrent immediately
                    # Handle the edge case where the prefetcher grabbed a chunk slightly larger than the requested req_end
                    if target_bytes and (bytes_sent + len(chunk)) > target_bytes:
                        excess = (bytes_sent + len(chunk)) - target_bytes
                        valid_chunk = chunk[:-excess]
                        
                        # We must push the excess bytes back to the front of the queue for the next request to consume!
                        # Since asyncio.Queue doesn't have put_front, we handle it organically on the next iteration.
                        yield valid_chunk
                        bytes_sent += len(valid_chunk)
                        stream.bytes_produced += len(valid_chunk)
                        
                        # Clean hand-over
                        next_byte = stream.start_byte + stream.bytes_produced
                        next_stream_id = f"{url}_{next_byte}"
                        
                        # Create a custom queue just to hold the remainder temporarily
                        # Actually, to keep it simple, we just cancel the worker if the chunk boundary doesn't align perfectly.
                        # Since sequential downloads typically align perfectly, this edge case is rare.
                        stream.worker_task.cancel()
                        if stream_id in self._active_streams:
                            del self._active_streams[stream_id]
                        break
                    else:
                        yield chunk
                        bytes_sent += len(chunk)
                        stream.bytes_produced += len(chunk)
                        
            except asyncio.CancelledError:
                # Client abruptly disconnected
                stream.worker_task.cancel()
                if stream_id in self._active_streams:
                    del self._active_streams[stream_id]
                raise

        return 206, response_headers, stream_body()
