import httpx
import asyncio
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

            # TorBox indicates a cache hit by returning the info_hash as a key in the data object
            cached = False
            if isinstance(data, dict):
                # Check both lowercase and uppercase to be completely safe against formatting differences
                cached = info_hash.lower() in data or info_hash.upper() in data
                
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

    async def create_torrent(self, info_hash: str) -> bool:
        """Adds the torrent to the TorBox dashboard so files can be requested."""
        if not self.client:
            raise RuntimeError("Client not initialized")

        try:
            logger.info("torbox_creating_torrent", info_hash=info_hash)
            endpoint = "/api/torrents/createtorrent"
            
            # Form-data with a generic magnet link built from the hash
            data = {
                "magnet": f"magnet:?xt=urn:btih:{info_hash}",
                "seed": "3", # TorBox standard rule
                "allow_zip": "false" # Force unzipped behavior
            }

            response = await self.client.post(
                endpoint,
                data=data,
                headers=self._get_headers(),
            )
            response.raise_for_status()
            
            logger.info("torbox_torrent_created", info_hash=info_hash)
            return True
        except Exception as e:
            logger.error("torbox_create_torrent_failed", info_hash=info_hash, error=str(e))
            return False

    async def wait_for_dashboard_sync(self, info_hash: str) -> Optional[dict]:
        """Polls the dashboard until the torrent appears, returning its info."""
        if not self.client:
            raise RuntimeError("Client not initialized")
            
        logger.info("torbox_waiting_for_dashboard_sync", info_hash=info_hash)
        torrent_info = None
        for _ in range(20):
            try:
                endpoint = "/api/torrents/mylist"
                response = await self.client.get(
                    endpoint,
                    headers=self._get_headers(),
                )
                response.raise_for_status()

                torrent_list = response.json().get("data", [])
                
                for t in torrent_list:
                    if t.get("hash", "").lower() == info_hash.lower():
                        torrent_info = t
                        break
                        
                if torrent_info:
                    logger.info("torbox_dashboard_synced", info_hash=info_hash)
                    return torrent_info
                    
            except Exception as e:
                logger.debug("torbox_sync_poll_error", error=str(e))
                
            logger.debug("torbox_torrent_not_yet_in_dashboard", info_hash=info_hash)
            await asyncio.sleep(2.0)
            
        logger.error("torbox_torrent_not_in_dashboard_timeout", info_hash=info_hash)
        return None

    async def get_direct_link(
        self,
        info_hash: str,
        file_path: str,
    ) -> Optional[str]:
        if not self.client:
            raise RuntimeError("Client not initialized")

        # Check local cache to prevent spamming TorBox API for the same file
        cache_key = f"{info_hash}_{file_path}"
        if cache_key in self._link_cache:
            link, expiry = self._link_cache[cache_key]
            if datetime.utcnow() < expiry:
                return link

        try:
            logger.info("torbox_direct_link_request", info_hash=info_hash, file_path=file_path)

            # 1. Get your personal torrent list to find the torrent_id and file list
            endpoint = "/api/torrents/mylist"
            response = await self.client.get(
                endpoint,
                headers=self._get_headers(),
            )
            response.raise_for_status()
            torrent_list = response.json().get("data", [])
            
            torrent_info = None
            for t in torrent_list:
                if t.get("hash", "").lower() == info_hash.lower():
                    torrent_info = t
                    break
                    
            if not torrent_info:
                logger.error("torbox_torrent_not_in_dashboard", info_hash=info_hash)
                return None

            torrent_id = torrent_info.get("id")
            files = torrent_info.get("files", [])

            # 3. Find the exact file_id by matching the qBittorrent file_path
            file_id = None
            for f in files:
                tb_name = f.get("name", "").replace("\\", "/")
                req_name = file_path.replace("\\", "/")
                # Substring match to handle folder structural differences
                if tb_name in req_name or req_name in tb_name:
                    file_id = f.get("id")
                    break

            if file_id is None:
                logger.error("torbox_file_not_found_in_torrent", file_path=file_path)
                return None

            # 4. Request the direct download link (requires token in params)
            dl_endpoint = "/api/torrents/requestdl"
            dl_params = {
                "token": self.api_key,
                "torrent_id": torrent_id,
                "file_id": file_id,
            }

            response = await self.client.get(
                dl_endpoint,
                params=dl_params,
                headers=self._get_headers(),
            )
            response.raise_for_status()

            link = response.json().get("data")
            
            if link:
                self._link_cache[cache_key] = (link, datetime.utcnow() + self._cache_ttl)
                logger.info("torbox_direct_link_obtained", file_path=file_path)

            return link

        except Exception as e:
            logger.error("torbox_direct_link_error", info_hash=info_hash, error=str(e))
            return None



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
