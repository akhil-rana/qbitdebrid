import asyncio
from datetime import datetime, timedelta
from typing import Optional

from qbitdebrid.clients.torbox import TorBoxClient
from qbitdebrid.clients.qbit import QBitController
from qbitdebrid.models import TorrentInfo, CacheStatus, PrefetchTask
from qbitdebrid.logging import get_logger

logger = get_logger(__name__)


class AutomationDaemon:
    def __init__(
        self,
        torbox_client: TorBoxClient,
        qbit_controller: QBitController,
        proxy_host: str = "127.0.0.1",
        proxy_port: int = 8888,
        poll_interval: int = 3,
        cache_threshold: float = 0.95,
        enable_jit_prefetch: bool = True,
    ):
        self.torbox_client = torbox_client
        self.qbit_controller = qbit_controller
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.poll_interval = poll_interval
        self.cache_threshold = cache_threshold
        self.enable_jit_prefetch = enable_jit_prefetch

        self._running = False
        self._known_torrents: dict[str, TorrentInfo] = {}
        self._pending_prefetch_tasks: dict[str, PrefetchTask] = {}
        self._last_prefetch_check: dict[str, datetime] = {}

    async def start(self) -> None:
        self._running = True
        logger.info("daemon_started")

        try:
            while self._running:
                try:
                    await self._poll_torrents()
                    await asyncio.sleep(self.poll_interval)
                except Exception as e:
                    logger.error("daemon_poll_error", error=str(e))
                    await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            logger.info("daemon_cancelled")
        finally:
            self._running = False
            logger.info("daemon_stopped")

    async def stop(self) -> None:
        self._running = False

    async def _poll_torrents(self) -> None:
        try:
            current_torrents = await self.qbit_controller.get_torrents()
            current_hashes = {t.hash for t in current_torrents}

            new_hashes = self.qbit_controller.get_new_torrents(current_hashes)

            if new_hashes:
                logger.info("new_torrents_detected", count=len(new_hashes))

                for new_hash in new_hashes:
                    torrent = next(
                        (t for t in current_torrents if t.hash == new_hash), None
                    )
                    if torrent:
                        await self._process_new_torrent(torrent)

            if self.enable_jit_prefetch:
                await self._check_prefetch_opportunities(current_torrents)

        except Exception as e:
            logger.error("poll_torrents_error", error=str(e))

    async def _process_new_torrent(self, torrent: TorrentInfo) -> None:
        logger.info("processing_new_torrent", name=torrent.name)

        try:
            # Immediately pause the torrent so it doesn't try to download prematurely
            await self.qbit_controller.pause_torrent(torrent.hash)

            cache_response = await self.torbox_client.verify_cache(
                info_hash=torrent.info_hash
            )

            if cache_response.cached:
                torrent.cache_status = CacheStatus.CACHED
                torrent.cached_link = cache_response.download_link
                logger.info("torrent_cached", name=torrent.name)
                
                # Add to TorBox dashboard
                success = await self.torbox_client.create_torrent(torrent.info_hash)
                if success:
                    await self._apply_isolation_protocol(torrent)
                    # Finally resume the torrent now that everything is set up
                    await self.qbit_controller.resume_torrent(torrent.hash)
                else:
                    logger.error("torbox_create_torrent_failed_skipping_isolation", name=torrent.name)
                    await self.qbit_controller.resume_torrent(torrent.hash)
            else:
                torrent.cache_status = CacheStatus.NOT_CACHED
                logger.info("torrent_not_cached", name=torrent.name)
                # If not cached, maybe just resume it and let qbittorrent download normally?
                await self.qbit_controller.resume_torrent(torrent.hash)

        except Exception as e:
            logger.error("process_torrent_error", name=torrent.name, error=str(e))
            torrent.cache_status = CacheStatus.UNKNOWN

        self._known_torrents[torrent.hash] = torrent
        self.qbit_controller.track_torrent(torrent.hash)

    def get_torrent(self, torrent_hash: str) -> Optional[TorrentInfo]:
        return self._known_torrents.get(torrent_hash)

    async def _apply_isolation_protocol(self, torrent: TorrentInfo) -> None:
        logger.info("applying_isolation_protocol", name=torrent.name)

        try:
            # Provide two aliases to the same proxy to bypass qBittorrent's per-host connection limits
            web_seed_url_1 = f"http://{self.proxy_host}:{self.proxy_port}/proxy/{torrent.hash}/"
            web_seed_url_2 = f"http://localhost:{self.proxy_port}/proxy/{torrent.hash}/"
            
            torrent.web_seed_url = web_seed_url_1

            # qBittorrent sometimes rejects multi-URL string formats depending on the API version.
            # We will supply the single IPv4 alias as the primary web seed for now.
            await self.qbit_controller.add_web_seed(torrent.hash, web_seed_url_1)
            
            # Enable sequential downloading to force qBittorrent to pull chunks sequentially, allowing HTTP connections to stay hot and stream perfectly
            await self.qbit_controller.set_sequential_download(
                torrent.hash, enabled=True
            )
            
            # Remove all trackers and disable DHT/PEX for this torrent to force pure Web Seed mode.
            # Since we now use Force Start and Top Priority, libtorrent will no longer stall the torrent.
            await self.qbit_controller.remove_trackers(torrent.hash)
            
            # Libtorrent heavily limits per-connection bandwidth.
            # We explicitly UNLIMIT connections (-1) so libtorrent can rip data from the fast memory prefetch proxy.
            await self.qbit_controller.set_max_connections(
                torrent.hash, max_connections=-1
            )

            torrent.isolation_applied = True
            logger.info("isolation_applied", name=torrent.name)

        except Exception as e:
            logger.error("apply_isolation_error", name=torrent.name, error=str(e))

    async def _check_prefetch_opportunities(
        self,
        current_torrents: list[TorrentInfo],
    ) -> None:
        try:
            for torrent in current_torrents:
                if torrent.cache_status != CacheStatus.CACHED:
                    continue

                last_check = self._last_prefetch_check.get(torrent.hash)
                if last_check and datetime.utcnow() - last_check < timedelta(
                    seconds=30
                ):
                    continue

                self._last_prefetch_check[torrent.hash] = datetime.utcnow()

                files = await self.qbit_controller.get_torrent_files(torrent.hash)

                for file in files:
                    if file.progress >= self.cache_threshold:
                        if file.index + 1 < len(files):
                            next_file = files[file.index + 1]
                            await self._schedule_prefetch(
                                torrent, next_file.index, next_file.name
                            )

        except Exception as e:
            logger.error("check_prefetch_error", error=str(e))

    async def _schedule_prefetch(
        self,
        torrent: TorrentInfo,
        file_index: int,
        file_name: str,
    ) -> None:
        task_key = f"{torrent.hash}:{file_index}"

        if task_key in self._pending_prefetch_tasks:
            return

        logger.info(
            "scheduling_prefetch", torrent_hash=torrent.hash, file_index=file_index
        )

        try:
            direct_link = await self.torbox_client.get_direct_link(
                info_hash=torrent.info_hash,
                file_index=file_index,
            )

            if direct_link:
                task = PrefetchTask(
                    torrent_hash=torrent.hash,
                    file_index=file_index,
                    file_name=file_name,
                    cached_link=direct_link,
                )
                self._pending_prefetch_tasks[task_key] = task
                logger.info("prefetch_link_obtained", file_index=file_index)

        except Exception as e:
            logger.error("schedule_prefetch_error", file_index=file_index, error=str(e))

    def get_cached_link(
        self,
        torrent_hash: str,
        file_index: Optional[int] = None,
    ) -> Optional[str]:
        if file_index is not None:
            task_key = f"{torrent_hash}:{file_index}"
            task = self._pending_prefetch_tasks.get(task_key)
            if task:
                return task.cached_link

        torrent = self._known_torrents.get(torrent_hash)
        if torrent:
            return self.torbox_client.get_cached_link(torrent.info_hash)

        return None
