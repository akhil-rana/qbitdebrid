import aiohttp
import asyncio
from typing import AsyncIterator, Optional, Tuple

from qbitdebrid.logging import get_logger

logger = get_logger(__name__)


class StreamingService:
    def __init__(
        self,
        chunk_size: int = 65536,
        max_retries: int = 3,
        retry_backoff: float = 2.0,
    ):
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff

    async def stream_range(
        self,
        url: str,
        range_start: int,
        range_end: int,
        headers: Optional[dict[str, str]] = None,
    ) -> Tuple[int, dict[str, str], AsyncIterator[bytes]]:
        if headers is None:
            headers = {}

        range_header = f"bytes={range_start}-{range_end}"
        headers["Range"] = range_header

        attempt = 0
        last_error: Optional[Exception] = None

        while attempt < self.max_retries:
            try:
                logger.info(
                    "streaming_range_request",
                    url=url,
                    range=range_header,
                    attempt=attempt + 1,
                )

                session = aiohttp.ClientSession()
                resp = await session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=300),
                )

                response_headers = dict(resp.headers)
                logger.info(
                    "streaming_range_response",
                    status_code=resp.status,
                )

                async def stream_body() -> AsyncIterator[bytes]:
                    try:
                        async for chunk in resp.content.iter_chunked(self.chunk_size):
                            if chunk:
                                yield chunk
                    finally:
                        resp.close()
                        await session.close()

                return resp.status, response_headers, stream_body()

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = e
                attempt += 1

                if attempt < self.max_retries:
                    wait_time = self.retry_backoff ** (attempt - 1)
                    logger.warning(
                        "streaming_range_retry",
                        attempt=attempt,
                        error=str(e),
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("streaming_range_failed", error=str(e))

        if last_error:
            raise last_error
        raise RuntimeError("Streaming failed")

    async def get_file_size(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None,
    ) -> Optional[int]:
        try:
            if headers is None:
                headers = {}

            async with aiohttp.ClientSession() as session:
                async with session.head(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    return resp.content_length

        except Exception as e:
            logger.error("streaming_head_failed", error=str(e))
            return None

    async def validate_range_support(self, url: str) -> bool:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.head(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    return resp.headers.get("Accept-Ranges") == "bytes"

        except Exception as e:
            logger.error("streaming_range_support_check_failed", error=str(e))
            return False
