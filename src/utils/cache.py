"""Disk-based caching to ensure zero redundant requests to HLTV."""
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DiskCache:
    """Manages raw HTML caching on disk."""

    @staticmethod
    def exists(file_path: Path) -> bool:
        return file_path.exists() and file_path.stat().st_size > 0

    @staticmethod
    def read(file_path: Path) -> Optional[str]:
        if DiskCache.exists(file_path):
            try:
                return file_path.read_text(encoding="utf-8")
            except Exception as e:
                logger.warning("Failed to read cache file %s: %s", file_path, e)
        return None

    @staticmethod
    def write(file_path: Path, content: str) -> None:
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content, encoding="utf-8")
            logger.debug("Cached %d bytes to %s", len(content), file_path)
        except Exception as e:
            logger.error("Failed to write cache file %s: %s", file_path, e)
