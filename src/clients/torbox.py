import httpx
from typing import Optional
from datetime import datetime, timedelta

from qbitdebrid.models import CacheVerificationResponse
from qbitdebrid.logging import get_logger

logger = get_logger(__name__)


class TorBoxClient:
    def __init__(self, api_key: str, base_url: str = "https://api.torbox.app/v1"):
        self.api_key = api_key
        self.base_url = base_url
        self.client: Optional[httpx.AsyncClient] = None
        self._link_cache: dict[str, tuple[str, datetime]] = {}
        self._cache_ttl = timedelta(hours=24)

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            headers={"User-Agent": "qbitdebrid/0.1.0"},
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    def _get_headers(self) -> dict[str, str]:
        return {
            "User-Agent": "qbitdebrid/0.1.0",
            "Authorization": f"Bearer {self.api_key}",
        }

    async def verify_cache(
        self,
        info_hash: str,
        unhashed_id: Optional[str] = None,
    ) -> CacheVerificationResponse:
        if not self.client:
            raise RuntimeError("Client not initialized")

        try:
            logger.info("torbox_cache_check", info_hash=info_hash)

            endpoint = "/api/torrents/checkcached"
            params = {"hash": info_hash, "format": "object", "list_files": "true"}

            response = await self.client.get(
                endpoint,
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            data = response.json()

            # Check if data contains success flag
            if isinstance(data, dict) and "data" in data:
                data = data["data"]

            # The endpoint returns the cached status in the data
            cached = data.get("cached", False) if isinstance(data, dict) else False
            download_link = (
                data.get("download_link") if isinstance(data, dict) else None
            )

            logger.info(
                "torbox_cache_result",
                info_hash=info_hash,
                cached=cached,
            )

            if download_link:
                self._link_cache[info_hash] = (
                    download_link,
                    datetime.utcnow() + self._cache_ttl,
                )

            return CacheVerificationResponse(
                cached=cached,
                download_link=download_link,
                zip_link=None,
            )

        except httpx.HTTPStatusError as e:
            logger.error(
                "torbox_cache_check_failed",
                info_hash=info_hash,
                status_code=e.response.status_code,
            )
            raise
        except Exception as e:
            logger.error("torbox_cache_check_error", info_hash=info_hash, error=str(e))
            raise

    async def get_direct_link(
        self,
        info_hash: str,
        file_index: Optional[int] = None,
    ) -> Optional[str]:
        if not self.client:
            raise RuntimeError("Client not initialized")

        try:
            logger.info(
                "torbox_direct_link_request",
                info_hash=info_hash,
                file_index=file_index,
            )

            # First, get the torrent info to get the torrent_id
            endpoint = "/api/torrents/torrentinfo"
            params = {"hash": info_hash}

            response = await self.client.get(
                endpoint,
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            torrent_data = response.json()
            if isinstance(torrent_data, dict) and "data" in torrent_data:
                torrent_data = torrent_data["data"]

            if (
                not torrent_data
                or not isinstance(torrent_data, list)
                or len(torrent_data) == 0
            ):
                logger.error("torbox_torrent_not_found", info_hash=info_hash)
                return None

            torrent_info = torrent_data[0]
            torrent_id = torrent_info.get("id")

            if not torrent_id:
                logger.error("torbox_no_torrent_id", info_hash=info_hash)
                return None

            # Now get the download link
            dl_endpoint = "/api/torrents/requestdl"
            dl_params = {
                "torrent_id": torrent_id,
            }

            if file_index is not None:
                dl_params["file_id"] = file_index

            response = await self.client.get(
                dl_endpoint,
                params=dl_params,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            dl_data = response.json()
            if isinstance(dl_data, dict) and "data" in dl_data:
                link = dl_data["data"]
            else:
                link = dl_data

            if link:
                self._link_cache[info_hash] = (
                    link,
                    datetime.utcnow() + self._cache_ttl,
                )
                logger.info(
                    "torbox_direct_link_obtained",
                    info_hash=info_hash,
                    file_index=file_index,
                )

            return link

        except httpx.HTTPStatusError as e:
            logger.error(
                "torbox_direct_link_failed",
                info_hash=info_hash,
                file_index=file_index,
                status_code=e.response.status_code,
            )
            raise
        except Exception as e:
            logger.error(
                "torbox_direct_link_error",
                info_hash=info_hash,
                error=str(e),
            )
            raise

    async def get_webdav_list(self, info_hash: str) -> Optional[list[dict]]:
        if not self.client:
            raise RuntimeError("Client not initialized")

        try:
            logger.info("torbox_webdav_list_request", info_hash=info_hash)

            # Get torrent info which includes file list
            endpoint = "/api/torrents/torrentinfo"
            params = {
                "hash": info_hash,
            }

            response = await self.client.get(
                endpoint,
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            data = response.json()
            if isinstance(data, dict) and "data" in data:
                data = data["data"]

            if not data or not isinstance(data, list) or len(data) == 0:
                return None

            torrent_info = data[0]
            files = torrent_info.get("files", [])

            logger.info(
                "torbox_webdav_list_obtained",
                info_hash=info_hash,
                file_count=len(files),
            )

            return files

        except httpx.HTTPStatusError as e:
            logger.error(
                "torbox_webdav_list_failed",
                info_hash=info_hash,
                status_code=e.response.status_code,
            )
            raise
        except Exception as e:
            logger.error("torbox_webdav_list_error", info_hash=info_hash, error=str(e))
            raise

    def get_cached_link(self, info_hash: str) -> Optional[str]:
        if info_hash in self._link_cache:
            link, expiry = self._link_cache[info_hash]
            if datetime.utcnow() < expiry:
                return link
            else:
                del self._link_cache[info_hash]

        return None

    def clear_cache(self) -> None:
        self._link_cache.clear()
        logger.info("torbox_cache_cleared")
