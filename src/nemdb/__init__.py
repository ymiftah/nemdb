from pathlib import Path

from .config import Config
from .isp import ISPAssumptions
from .logger import log
from .nemweb import NEMWEBManager

# These imports must come after Config to avoid circular imports
from . import main, utils

if Config.FILESYSTEM == "local":
    log.info("Creating cache directory at %s", Config.CACHE_DIR)
    Path(Config.CACHE_DIR).mkdir(parents=True, exist_ok=True)

log.info("Creating temp cache directory at %s", Config.TEMP_DIR)
Path(Config.TEMP_DIR).mkdir(parents=True, exist_ok=True)


__all__ = ["Config", "ISPAssumptions", "NEMWEBManager", "main", "utils"]
