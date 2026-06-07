from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from datetime import datetime


class TorrentState(str, Enum):
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    PAUSED = "paused"
    CHECKING = "checking"
    METADL = "metaDL"
    FORCE_DL = "forcedDL"
    FORCE_UL = "forcedUL"
    STALLED_DL = "stalledDL"
    STALLED_UL = "stalledUL"
    QUEUED_FOR_CHECKING = "queuedForChecking"
    ALLOCATING = "allocating"
    MISSING_FILES = "missingFiles"
    ERROR = "error"
    UNKNOWN = "unknown"


class CacheStatus(str, Enum):
    CACHED = "cached"
    NOT_CACHED = "notCached"
    UNKNOWN = "unknown"


@dataclass
class TorrentInfo:
    hash: str
    name: str
    info_hash: str
    state: TorrentState
    progress: float
    total_size: int
    first_seen: datetime = field(default_factory=datetime.utcnow)
    cache_status: CacheStatus = CacheStatus.UNKNOWN
    cached_link: Optional[str] = None
    isolation_applied: bool = False
    web_seed_url: Optional[str] = None
    banned_peers: set[str] = field(default_factory=set)
    tags: str = ""
    save_path: str = ""


@dataclass
class FileInfo:
    index: int
    name: str
    size: int
    progress: float
    priority: int
    is_downloaded: bool


@dataclass
class CacheVerificationRequest:
    hash: str
    unhashed_id: Optional[str] = None


@dataclass
class CacheVerificationResponse:
    cached: bool
    download_link: Optional[str] = None
    file_index: Optional[int] = None


@dataclass
class StreamingRange:
    start: int
    end: int
    total: int

    @property
    def length(self) -> int:
        return self.end - self.start + 1





@dataclass
class PrefetchTask:
    torrent_hash: str
    file_index: int
    file_name: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    cached_link: Optional[str] = None
