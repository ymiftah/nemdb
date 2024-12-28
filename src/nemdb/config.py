import os

from .logger import log

class Config:
    CACHE_DIR = os.path.join(".", ".nemweb_cache")
    FILESYSTEM = "local"

    @classmethod
    def set_cache_dir(cls, cache_dir):
        cls.CACHE_DIR = cache_dir
        log.info("Set cache directory to %s", cls.CACHE_DIR)

