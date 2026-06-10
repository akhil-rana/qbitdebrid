import httpx
import asyncio
from typing import Optional
from datetime import datetime, timedelta

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

    async def create_torrent(self, info_hash: str) -> Optional[int]:
        """Adds the torrent to the TorBox dashboard and returns its torrent_id."""
        if not self.client:
            raise RuntimeError("Client not initialized")

        try:
            logger.info("torbox_creating_torrent", info_hash=info_hash)
            endpoint = "/api/torrents/createtorrent"
            
            data = {
                "magnet": f"magnet:?xt=urn:btih:{info_hash}",
                "seed": "3",
                "allow_zip": "false"
            }

            response = await self.client.post(
                endpoint,
                data=data,
                headers=self._get_headers(),
            )
            response.raise_for_status()
            
            data = response.json()
            torrent_id = data.get("data", {}).get("torrent_id")
            
            if torrent_id:
                logger.info("torbox_torrent_created", info_hash=info_hash, torrent_id=torrent_id)
                return torrent_id
            else:
                logger.error("torbox_create_torrent_missing_id", info_hash=info_hash, response=data)
                return None
                
        except Exception as e:
            logger.error("torbox_create_torrent_failed", info_hash=info_hash, error=str(e))
            return None

    async def wait_for_dashboard_sync(self, info_hash: str) -> Optional[dict]:
        """Polls the checkcached API to see when the torrent finishes downloading globally."""
        if not self.client:
            raise RuntimeError("Client not initialized")
            
        logger.info("torbox_waiting_for_dashboard_sync", info_hash=info_hash)
        
        # Strategy: 0s, 5s, 30s, then every 10s up to 6 minutes
        poll_delays = [0, 5, 30] + [10] * 33
        
        for delay in poll_delays:
            if delay > 0:
                await asyncio.sleep(delay)
                
            try:
                endpoint = "/api/torrents/checkcached"
                params = {"hash": info_hash, "format": "object", "bypass_cache": "true"}
                response = await self.client.get(
                    endpoint,
                    params=params,
                    headers=self._get_headers(),
                )
                response.raise_for_status()

                data = response.json()
                
                # Unwrap Torbox nested data structure
                if isinstance(data, dict) and "data" in data:
                    data = data["data"]
                    
                cached = False
                if isinstance(data, dict):
                    # Check both lower and upper to be completely safe
                    cached = info_hash.lower() in data or info_hash.upper() in data
                    
                if cached:
                    logger.info("torbox_dashboard_synced", info_hash=info_hash)
                    return {"hash": info_hash, "download_finished": True}
                else:
                    logger.debug("torbox_torrent_syncing_but_not_finished", info_hash=info_hash)
                        
            except Exception as e:
                logger.debug("torbox_sync_poll_error", error=str(e))
                
            logger.debug("torbox_torrent_not_yet_ready_in_dashboard", info_hash=info_hash)
            
        logger.error("torbox_torrent_not_in_dashboard_timeout", info_hash=info_hash)
        return None

    async def get_direct_link(
        self,
        info_hash: str,
        file_path: str,
    ) -> Optional[str]:
        if not self.client:
            raise RuntimeError("Client not initialized")

        cache_key = f"{info_hash}_{file_path}"
        if cache_key in self._link_cache:
            link, expiry = self._link_cache[cache_key]
            if datetime.utcnow() < expiry:
                return link

        try:
            logger.info("torbox_direct_link_request", info_hash=info_hash, file_path=file_path)

            # 1. Get torrent_id (Idempotent, instant, guarantees it's in our dashboard)
            torrent_id = await self.create_torrent(info_hash)
            if not torrent_id:
                logger.error("torbox_direct_link_failed_to_get_torrent_id", info_hash=info_hash)
                return None

            # 2. Get file list from checkcached (Ultra-fast, O(1), bypasses user cache)
            endpoint = "/api/torrents/checkcached"
            params = {"hash": info_hash, "format": "object", "list_files": "true", "bypass_cache": "true"}
            
            response = await self.client.get(
                endpoint,
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, dict) and "data" in data:
                data = data["data"]
                
            # Handle both cases
            torrent_data = data.get(info_hash.lower()) or data.get(info_hash.upper())
            
            if not torrent_data:
                logger.error("torbox_torrent_not_in_checkcached", info_hash=info_hash)
                return None

            files = torrent_data.get("files", [])

            # 3. Find the exact file_id
            file_id = None
            for f in files:
                tb_name = f.get("name", "").replace("\\", "/")
                req_name = file_path.replace("\\", "/")
                if tb_name in req_name or req_name in tb_name:
                    file_id = f.get("id")
                    break

            if file_id is None:
                logger.error("torbox_file_not_found_in_torrent", file_path=file_path)
                return None

            # 4. Request the direct download link
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
