import struct
import httpx
from typing import Optional

from qbitdebrid.logging import get_logger

logger = get_logger(__name__)


class ZipEntry:
    def __init__(
        self,
        filename: str,
        local_offset: int,
        compressed_size: int,
        uncompressed_size: int,
        compression_method: int,
    ):
        self.filename = filename
        self.local_offset = local_offset
        self.compressed_size = compressed_size
        self.uncompressed_size = uncompressed_size
        self.compression_method = compression_method


class ZipParser:
    @staticmethod
    async def fetch_central_directory(url: str) -> Optional[bytes]:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.head(url)
                total_size = resp.content_length

            if not total_size:
                return None

            read_size = min(65536, total_size)
            range_start = max(0, total_size - read_size)

            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    url,
                    headers={"Range": f"bytes={range_start}-{total_size - 1}"},
                )

            if resp.status_code == 206:
                return resp.content

        except Exception as e:
            logger.error("zip_central_directory_fetch_failed", error=str(e))

        return None

    @staticmethod
    def parse_central_directory(data: bytes) -> list[ZipEntry]:
        entries = []
        offset = 0

        while offset < len(data):
            if offset + 46 > len(data):
                break

            sig = struct.unpack("<I", data[offset : offset + 4])[0]

            if sig != 0x02014B50:
                break

            filename_len = struct.unpack("<H", data[offset + 28 : offset + 30])[0]
            extra_len = struct.unpack("<H", data[offset + 30 : offset + 32])[0]
            local_offset = struct.unpack("<I", data[offset + 42 : offset + 46])[0]
            compressed = struct.unpack("<I", data[offset + 20 : offset + 24])[0]
            uncompressed = struct.unpack("<I", data[offset + 24 : offset + 28])[0]
            compression = struct.unpack("<H", data[offset + 10 : offset + 12])[0]

            filename_start = offset + 46
            filename = data[filename_start : filename_start + filename_len].decode(
                "utf-8", errors="ignore"
            )

            entries.append(
                ZipEntry(
                    filename=filename,
                    local_offset=local_offset,
                    compressed_size=compressed,
                    uncompressed_size=uncompressed,
                    compression_method=compression,
                )
            )

            offset += 46 + filename_len + extra_len

        return entries

    @staticmethod
    async def get_file_range_in_zip(
        url: str,
        file_index: int,
        entries: list[ZipEntry],
    ) -> Optional[tuple[int, int]]:
        if file_index >= len(entries):
            return None

        entry = entries[file_index]

        if entry.compression_method != 0:
            logger.warning("zip_file_compressed", filename=entry.filename)
            return None

        return (entry.local_offset, entry.local_offset + entry.uncompressed_size - 1)
