from pathlib import Path

from .config import config
from .isp import ISPAssumptions
from .logger import log
from .nemweb import NEMWEBManager

# These imports must come after Config to avoid circular imports
from . import main, utils

if config.filesystem == "local":
    log.info("Creating cache directory at %s", config.cache_dir)
    Path(config.cache_dir).mkdir(parents=True, exist_ok=True)

log.info("Creating temp cache directory at %s", config.temp_dir)
Path(config.temp_dir).mkdir(parents=True, exist_ok=True)


__all__ = ["ISPAssumptions", "NEMWEBManager", "config", "main", "utils"]
