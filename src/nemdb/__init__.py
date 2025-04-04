import os
from .config import Config
from .logger import log

from . import utils
from .nemweb import NEMWEBManager
from .isp import ISPAssumptions


if Config.FILESYSTEM == "local" and not os.path.exists(Config.CACHE_DIR):
    log.info("Creating cache directory at %s", Config.CACHE_DIR)
    os.makedirs(Config.CACHE_DIR)


__all__ = ["Config", "NEMWEBManager", "ISPAssumptions", "utils"]
