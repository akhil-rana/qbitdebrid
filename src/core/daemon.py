import asyncio
from typing import Optional

from qbitdebrid.clients.torbox import TorBoxClient
from qbitdebrid.clients.qbit import QBitController
from qbitdebrid.models import TorrentInfo
from qbitdebrid.logging import get_logger
import qbitdebrid.utils.bencode as bencode

logger = get_logger(__name__)


class AutomationDaemon:
    def __init__(
        self,
        torbox_client: TorBoxClient,
        qbit_controller: QBitController,
        proxy_host: str = "127.0.0.1",
        proxy_port: int = 8888,
        poll_interval: int = 3,
        process_tag: str = "",
    ):
        self.torbox_client = torbox_client
        self.qbit_controller = qbit_controller
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.poll_interval = poll_interval
        self.process_tag = process_tag

        self._running = False
        self._known_torrents: dict[str, TorrentInfo] = {}

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

        except Exception as e:
            logger.debug("poll_torrents_error", error=str(e))

    async def _process_new_torrent(self, torrent: TorrentInfo) -> None:
        # Filter by tag if configured
        if self.process_tag:
            # Check if this torrent is missing the required process tag
            current_tags = [t.strip() for t in torrent.tags.split(",") if t.strip()]
            if self.process_tag not in current_tags:
                # Users often add a torrent and then tag it manually.
                # Do NOT add to _known_torrents yet so we can see the tag update later.
                logger.debug("ignoring_torrent_due_to_missing_tag", name=torrent.name, required_tag=self.process_tag)
                return

        logger.debug("processing_new_torrent", name=torrent.name)

        try:
            # Check if this is an already mutated torrent by looking for our signature in the comment
            is_mutated = torrent.comment and "qbitdebrid_original_hash:" in torrent.comment
            
            if is_mutated:
                original_hash = None
                prios = {}
                target_state = "downloading"
                
                # Retrieve original_hash, priorities, and state from the comment field
                parts = torrent.comment.split("|")
                for part in parts:
                    if part.startswith("qbitdebrid_original_hash:"):
                        original_hash = part.split(":")[1].strip()
                    elif part.startswith("prios:"):
                        prios_str = part.split("prios:")[1].strip()
                        if prios_str:
                            try:
                                prios = {int(p.split("=")[0]): int(p.split("=")[1]) for p in prios_str.split(",")}
                            except Exception as e:
                                logger.error("failed_to_parse_priorities", error=str(e))
                    elif part.startswith("state:"):
                        target_state = part.split(":")[1].strip()
                    
                # Backward compatibility: Check tags if not in comment (for older mutated torrents)
                if not original_hash:
                    for tag in torrent.tags.split(","):
                        if tag.strip().startswith("original_hash:"):
                            original_hash = tag.strip().split(":")[1]
                            break
                        
                if not original_hash:
                    logger.error("mutated_torrent_missing_original_hash", name=torrent.name)
                    # Track it so we don't endlessly spam the logs trying to process a broken torrent
                    self._known_torrents[torrent.hash] = torrent
                    self.qbit_controller.track_torrent(torrent.hash)
                    return
                    
                torrent.info_hash = original_hash
                logger.debug("detected_mutated_torrent", name=torrent.name, original_hash=original_hash)
                
                # Restore original file selection/priorities if they were saved
                if prios:
                    await self.qbit_controller.set_file_priority(torrent.hash, prios)
                    logger.debug("restored_file_priorities", name=torrent.name)
                    
                # Apply isolation protocol (which enables sequential download and removes trackers)
                await self._apply_isolation_protocol(torrent)
                
            else:
                # Handle raw, newly added torrent via background task to prevent blocking
                logger.info("queuing_raw_torrent_for_background_mutation", name=torrent.name)
                asyncio.create_task(self._wait_and_mutate_task(torrent))
                
        except Exception as e:
            logger.error("process_torrent_error", name=torrent.name, error=str(e))

        # Always add to known torrents so the daemon doesn't poll it again immediately
        self._known_torrents[torrent.hash] = torrent
        self.qbit_controller.track_torrent(torrent.hash)

    async def _wait_and_mutate_task(self, torrent: TorrentInfo) -> None:
        """Background task to wait for TorBox, capture current state, and mutate seamlessly."""
        try:
            # 1. Export raw .torrent file first so we can upload it to TorBox
            torrent_data = await self.qbit_controller.export_torrent(torrent.hash)
            if not torrent_data:
                logger.error("failed_to_export_torrent_for_torbox", name=torrent.name)
                return
                
            # 2. Add to TorBox dashboard by uploading the raw .torrent
            success = await self.torbox_client.create_torrent(torrent.info_hash, torrent_data)
            if not success:
                logger.error("torbox_create_torrent_failed_aborting_mutation", name=torrent.name)
                return

            # 3. Wait for TorBox dashboard sync before mutating
            sync_info = await self.torbox_client.wait_for_dashboard_sync(torrent.info_hash)
            if sync_info is None:
                logger.error("dashboard_sync_timeout_will_retry_later", name=torrent.name)
                # Remove from known_torrents so the daemon natively retries processing this on the next cycle
                self._known_torrents.pop(torrent.hash, None)
                self.qbit_controller._tracked_hashes.discard(torrent.hash)
                return

            # 4. Capture current state and priorities before mutation
            current_torrents = await self.qbit_controller.get_torrents()
            latest_torrent = next((t for t in current_torrents if t.hash == torrent.hash), None)
            
            if not latest_torrent:
                logger.warning("torrent_deleted_while_waiting_for_torbox", name=torrent.name)
                return
                
            # Fetch the raw torrent data again to get the exact unparsed state string
            raw_torrents = self.qbit_controller.client.torrents_info(torrent_hashes=latest_torrent.hash)
            raw_state = raw_torrents[0].get("state", "").lower() if raw_torrents else ""
            
            was_paused = "pause" in raw_state or "stop" in raw_state
            is_checking = "check" in raw_state
            
            files = await self.qbit_controller.get_torrent_files(latest_torrent.hash)
            prios_str = ",".join(f"{f.index}={f.priority}" for f in files if f.priority != 1)

            # 5. Mutate the already exported torrent_data
            decoded = bencode.decode(torrent_data)
            
            # Make it private to explicitly disable DHT/PEX/LSD
            decoded[b"info"][b"private"] = 1
            decoded[b"info"][b"source"] = f"qbitdebrid_{latest_torrent.hash}".encode("utf-8")
            web_seed_url = f"http://{self.proxy_host}:{self.proxy_port}/proxy/{latest_torrent.hash}/"
            decoded[b"url-list"] = web_seed_url.encode("utf-8")
            
            # Store the original hash, priorities, and play/pause state securely in the torrent comment
            comment_str = f"qbitdebrid_original_hash:{latest_torrent.info_hash}"
            if prios_str:
                comment_str += f"|prios:{prios_str}"
                
            if was_paused:
                state_str = "paused"
            elif is_checking:
                state_str = "checking"
            else:
                state_str = "downloading"
                
            comment_str += f"|state:{state_str}"
            
            decoded[b"comment"] = comment_str.encode("utf-8")
            encoded_data = bencode.encode(decoded)
            
            # 6. Swap torrents without modifying tags
            success = await self.qbit_controller.add_raw_torrent(
                encoded_data, 
                save_path=latest_torrent.save_path, 
                tags=latest_torrent.tags,
                is_paused=False,
                stop_condition="FilesChecked" if was_paused else None
            )
            
            if success:
                # Only delete the original torrent after successfully adding the TorBox mirror!
                await self.qbit_controller.delete_torrent(latest_torrent.hash)
                logger.info("raw_torrent_mutated_and_replaced", name=latest_torrent.name)
                
        except Exception as e:
            logger.error("background_mutate_error", name=torrent.name, error=str(e))

    async def _apply_isolation_protocol(self, torrent: TorrentInfo) -> None:
        logger.debug("applying_isolation_protocol", name=torrent.name)

        try:
            # Enforce sequential download for HTTP stream optimization
            await self.qbit_controller.set_sequential_download(
                torrent.hash, enabled=True
            )
            
            # Remove trackers to force pure Web Seed mode
            await self.qbit_controller.remove_trackers(torrent.hash)
            
            # Limit to exactly 1 connection to force a single stable stream
            await self.qbit_controller.set_max_connections(
                torrent.hash, max_connections=1
            )

            torrent.isolation_applied = True
            logger.info("isolation_applied", name=torrent.name)

        except Exception as e:
            logger.error("apply_isolation_error", name=torrent.name, error=str(e))
