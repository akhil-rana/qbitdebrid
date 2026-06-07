from os import sync
from typing import Optional
import qbittorrentapi

from qbitdebrid.models import TorrentInfo, TorrentState, FileInfo
from qbitdebrid.logging import get_logger

logger = get_logger(__name__)


class QBitController:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        url = f"http://{host}:{port}"
        self.client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
        )
        self._tracked_hashes: set[str] = set()

    async def connect(self) -> bool:
        try:
            self.client.auth_log_in()
            logger.info("qbit_connected", host=self.host, port=self.port)
            return True
        except Exception as e:
            logger.error("qbit_connection_failed", host=self.host, error=str(e))
            return False

    async def get_torrents(self) -> list[TorrentInfo]:
        try:
            torrents_data = self.client.torrents_info()
            torrents: list[TorrentInfo] = []

            for t in torrents_data:
                info_hash = t.get("hash", "")[:40]
                state_str = t.get("state", "unknown").lower()
                state = (
                    TorrentState(state_str)
                    if state_str in TorrentState._value2member_map_
                    else TorrentState.UNKNOWN
                )

                torrent_info = TorrentInfo(
                    hash=t.get("hash", ""),
                    name=t.get("name", ""),
                    info_hash=info_hash,
                    state=state,
                    progress=float(t.get("progress", 0.0)),
                    total_size=int(t.get("total_size", 0)),
                )
                torrents.append(torrent_info)

            return torrents
        except Exception as e:
            logger.error("qbit_get_torrents_failed", error=str(e))
            raise

    async def get_torrent_files(self, torrent_hash: str) -> list[FileInfo]:
        try:
            files_data = self.client.torrents_files(torrent_hash)
            files: list[FileInfo] = []

            for i, f in enumerate(files_data):
                file_info = FileInfo(
                    index=i,
                    name=f.get("name", ""),
                    size=int(f.get("size", 0)),
                    progress=float(f.get("progress", 0.0)),
                    priority=int(f.get("priority", 0)),
                    is_downloaded=f.get("is_downloaded", False),
                )
                files.append(file_info)

            return files
        except Exception as e:
            logger.error(
                "qbit_get_files_failed", torrent_hash=torrent_hash, error=str(e)
            )
            raise

    async def pause_torrent(self, torrent_hash: str) -> bool:
        try:
            self.client.torrents_pause(torrent_hashes=torrent_hash)
            logger.info("qbit_torrent_paused", torrent_hash=torrent_hash)
            return True
        except Exception as e:
            logger.error("qbit_pause_torrent_failed", error=str(e))
            return False

    async def resume_torrent(self, torrent_hash: str) -> bool:
        try:
            self.client.torrents_resume(torrent_hashes=torrent_hash)
            logger.info("qbit_torrent_resumed", torrent_hash=torrent_hash)
            return True
        except Exception as e:
            logger.error("qbit_resume_torrent_failed", error=str(e))
            return False

    async def add_web_seed(self, torrent_hash: str, web_seed_url: str) -> bool:
        try:
            # Use the correct python wrapper API method
            if hasattr(self.client.torrents, "add_webseeds"):
                self.client.torrents.add_webseeds(torrent_hash=torrent_hash, urls=web_seed_url)
            else:
                self.client.torrents_add_webseeds(torrent_hash, web_seed_url)
                
            logger.info("qbit_web_seed_added", torrent_hash=torrent_hash)
            return True
        except Exception as e:
            logger.error(
                "qbit_web_seed_failed", torrent_hash=torrent_hash, error=str(e)
            )
            return False
        
    async def set_sequential_download(
        self,
        torrent_hash: str,
        enabled: bool = True,
    ) -> bool:
        try:
            # 1. Get current torrent info
            torrent = self.client.torrents_info(torrent_hashes=torrent_hash)[0]
            current_state = torrent.get("seq_dl", False)
            
            # 2. Only toggle if the current state doesn't match the desired state
            if current_state != enabled:
                self.client.torrents_toggle_sequential_download(torrent_hashes=torrent_hash)
                
            logger.info("qbit_sequential_download_set", torrent_hash=torrent_hash)
            return True
        except Exception as e:
            logger.error("qbit_sequential_download_failed", error=str(e))
            return False

    async def remove_trackers(self, torrent_hash: str) -> bool:
        try:
            trackers = self.client.torrents_trackers(torrent_hash)

            for tracker in trackers:
                tracker_url = tracker.get("url", "")
                if tracker_url:
                    try:
                        self.client.torrents_remove_trackers(torrent_hash, tracker_url)
                    except Exception:
                        pass
            
            # Disable DHT, PeX, and LSD globally or per-torrent if supported to mimic an archive.org pure webseed
            try:
                # Try to use generic edit if supported
                if hasattr(self.client, "torrents_edit"):
                    self.client.torrents_edit(torrent_hash=torrent_hash, tracker_url="")
            except Exception:
                pass

            logger.info("qbit_trackers_removed", torrent_hash=torrent_hash)
            return True
        except Exception as e:
            logger.error("qbit_remove_trackers_failed", error=str(e))
            return False

    async def set_max_connections(
        self,
        torrent_hash: str,
        max_connections: int = -1,
    ) -> bool:
        try:
            # We explicitly set the download limit to unlimited just to make sure qbittorrent doesn't internally throttle
            self.client.torrents_set_download_limit(torrent_hashes=torrent_hash, limit=-1)
            
            # We choke the upload limit to 1 KB/s to discourage P2P activity
            self.client.torrents_set_upload_limit(torrent_hashes=torrent_hash, limit=1024)
            
            # We must set max connections to unlimited (-1) because web seeds consume concurrent connections. 
            # If limited to 1, qbittorrent will NEVER download faster than 1 slow HTTP stream.
            if hasattr(self.client, "torrents_edit"):
                self.client.torrents_edit(torrent_hash=torrent_hash, max_connections=-1, max_uploads=0)
            
            # Force top priority so the client actively allocates network IO
            try:
                self.client.torrents_top_priority(torrent_hashes=torrent_hash)
                self.client.torrents_set_force_start(torrent_hashes=torrent_hash, value=True)
            except Exception:
                pass
                
            # Use the correct API properties to set connection limits
            try:
                self.client.torrents_set_share_limits(
                    torrent_hashes=torrent_hash, 
                    ratio_limit=0, 
                    seeding_time_limit=0
                )
            except Exception:
                pass

            logger.info("qbit_p2p_throttled", torrent_hash=torrent_hash)
            return True
        except Exception as e:
            logger.error("qbit_max_connections_failed", error=str(e))
            return False

    async def set_file_priority(
        self,
        torrent_hash: str,
        file_priorities: dict[int, int],
    ) -> bool:
        try:
            for file_idx, priority in file_priorities.items():
                self.client.torrents.file_priority(torrent_hash, file_idx, priority)

            logger.info("qbit_file_priority_set", torrent_hash=torrent_hash)
            return True
        except Exception as e:
            logger.error("qbit_file_priority_failed", error=str(e))
            return False

    def track_torrent(self, torrent_hash: str) -> None:
        self._tracked_hashes.add(torrent_hash)

    def get_new_torrents(self, current_hashes: set[str]) -> set[str]:
        return current_hashes - self._tracked_hashes
