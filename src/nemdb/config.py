from pathlib import Path

from .logger import log


class Config:
    """Global configuration class for the application."""

    CACHE_DIR = Path.home() / ".nemweb_cache"
    FILESYSTEM = "local"

    @classmethod
    def set_cache_dir(cls, cache_dir):
        """Sets the cache directory location."""
        cls.CACHE_DIR = cache_dir.rstrip("/")
        log.info("Set cache directory to %s", cls.CACHE_DIR)

    @classmethod
    def set_filesystem(cls, filesystem):
        """Sets the cache directory location."""
        cls.FILESYSTEM = filesystem
        log.info("Set filesystem to %s", cls.FILESYSTEM)
