from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class TorrentState(str, Enum):
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    PAUSED = "paused"
    PAUSED_DL = "pauseddl"
    PAUSED_UP = "pausedup"
    STOPPED_DL = "stoppeddl"
    STOPPED_UP = "stoppedup"
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


@dataclass
class TorrentInfo:
    hash: str
    name: str
    info_hash: str
    state: TorrentState
    progress: float
    total_size: int
    isolation_applied: bool = False
    tags: str = ""
    comment: str = ""
    save_path: str = ""


@dataclass
class FileInfo:
    index: int
    name: str
    size: int
    progress: float
    priority: int
    is_downloaded: bool
