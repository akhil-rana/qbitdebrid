import httpx
import asyncio
import time
from typing import AsyncIterator, Optional, Tuple, Dict
from qbitdebrid.logging import get_logger

logger = get_logger(__name__)

BLOCK_SIZE = 16 * 1024 * 1024  # 16MB blocks
MAX_AHEAD_BLOCKS = 32  # 512MB lookahead
MAX_CONCURRENT_DOWNLOADS = 4

class FilePrefetcher:
    def __init__(self, url: str, file_size: int, client: httpx.AsyncClient):
        self.url = url
        self.file_size = file_size
        self.client = client
        
        self.blocks: Dict[int, bytes] = {}
        self.downloading_blocks = set()
        
        self.highest_requested_block = 0
        self.last_accessed = time.time()
        
        self.condition = asyncio.Condition()
        self.workers = []
        self.is_closed = False
        
        for i in range(MAX_CONCURRENT_DOWNLOADS):
            self.workers.append(asyncio.create_task(self._worker(i)))

    async def _worker(self, worker_id: int):
        logger.info("prefetch_worker_started", worker_id=worker_id)
        while not self.is_closed:
            block_to_download = None
            
            async with self.condition:
                for i in range(self.highest_requested_block, self.highest_requested_block + MAX_AHEAD_BLOCKS):
                    if i * BLOCK_SIZE >= self.file_size:
                        break 
                    if i not in self.blocks and i not in self.downloading_blocks:
                        block_to_download = i
                        self.downloading_blocks.add(i)
                        break
                
                if block_to_download is None:
                    await self.condition.wait()
                    continue
            
            start_byte = block_to_download * BLOCK_SIZE
            end_byte = min(start_byte + BLOCK_SIZE - 1, self.file_size - 1)
            
            headers = {"Range": f"bytes={start_byte}-{end_byte}"}
            
            success = False
            try:
                request = self.client.build_request("GET", self.url, headers=headers)
                resp = await self.client.send(request, follow_redirects=True)
                
                if resp.status_code in (200, 206):
                    data = await resp.aread()
                    if len(data) > 0:
                        async with self.condition:
                            self.blocks[block_to_download] = data
                            self.downloading_blocks.remove(block_to_download)
                            self.condition.notify_all()
                        success = True
                        logger.debug("block_downloaded", block=block_to_download)
            except Exception as e:
                logger.debug("block_download_failed", block=block_to_download, error=str(e))
                
            if not success:
                async with self.condition:
                    if block_to_download in self.downloading_blocks:
                        self.downloading_blocks.remove(block_to_download)
                    self.condition.notify_all()
                await asyncio.sleep(1.0)
                
    async def get_range(self, start_byte: int, end_byte: int) -> AsyncIterator[bytes]:
        start_block = start_byte // BLOCK_SIZE
        
        async with self.condition:
            self.last_accessed = time.time()
            if start_block > self.highest_requested_block:
                self.highest_requested_block = start_block
                self.condition.notify_all()
                
            keys_to_delete = [k for k in self.blocks.keys() if k < self.highest_requested_block - 2]
            for k in keys_to_delete:
                del self.blocks[k]

        current_byte = start_byte
        while current_byte <= end_byte:
            current_block = current_byte // BLOCK_SIZE
            block_start_byte = current_block * BLOCK_SIZE
            
            async with self.condition:
                while current_block not in self.blocks and not self.is_closed:
                    if current_block > self.highest_requested_block:
                        self.highest_requested_block = current_block
                    self.condition.notify_all()
                    await self.condition.wait()
                    
            if self.is_closed or current_block not in self.blocks:
                 break
                 
            block_data = self.blocks[current_block]
            
            offset_in_block = current_byte - block_start_byte
            bytes_to_read = min(end_byte - current_byte + 1, len(block_data) - offset_in_block)
            
            if bytes_to_read > 0:
                chunk_size = 1048576
                for i in range(0, bytes_to_read, chunk_size):
                    yield block_data[offset_in_block + i : offset_in_block + i + min(chunk_size, bytes_to_read - i)]
            
            current_byte += bytes_to_read

    def close(self):
        self.is_closed = True
        async def _notify():
            async with self.condition:
                self.condition.notify_all()
        try:
            asyncio.create_task(_notify())
        except Exception:
            pass
        for w in self.workers:
            w.cancel()

class StreamingService:
    def __init__(
        self,
        chunk_size: int = 1048576,
        max_retries: int = 3,
        retry_backoff: float = 2.0,
    ):
        limits = httpx.Limits(max_keepalive_connections=200, max_connections=400, keepalive_expiry=60.0)
        timeout = httpx.Timeout(connect=5.0, read=60.0, write=10.0, pool=15.0)
        self.client = httpx.AsyncClient(limits=limits, timeout=timeout)
        self._prefetchers: Dict[str, FilePrefetcher] = {}
        self._file_sizes: Dict[str, int] = {}
        asyncio.create_task(self._cleanup_loop())

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(60)
            now = time.time()
            to_delete = []
            for url, prefetcher in self._prefetchers.items():
                if now - prefetcher.last_accessed > 120:
                    prefetcher.close()
                    to_delete.append(url)
            for url in to_delete:
                del self._prefetchers[url]
                if url in self._file_sizes:
                    del self._file_sizes[url]
                logger.info("prefetcher_evicted", url=url)

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

    async def stream_range(
        self,
        url: str,
        range_header: str,
        headers: Optional[dict[str, str]] = None,
    ) -> Tuple[int, dict[str, str], AsyncIterator[bytes]]:
        
        req_start = 0
        req_end = None
        try:
            range_part = range_header.replace("bytes=", "")
            parts = range_part.split("-")
            req_start = int(parts[0]) if parts[0] else 0
            req_end = int(parts[1]) if len(parts) > 1 and parts[1] else None
        except Exception:
            pass

        file_size = await self.get_file_size(url)
        if not file_size:
            raise RuntimeError("Could not determine file size for prefetching")
            
        if req_end is None:
            req_end = file_size - 1

        if url not in self._prefetchers:
            self._prefetchers[url] = FilePrefetcher(url, file_size, self.client)
            
        prefetcher = self._prefetchers[url]

        response_headers = {
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {req_start}-{req_end}/{file_size}",
            "Content-Length": str(req_end - req_start + 1)
        }
        
        return 206, response_headers, prefetcher.get_range(req_start, req_end)
