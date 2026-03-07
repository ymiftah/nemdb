from pathlib import Path
from tempfile import gettempdir

from pydantic_settings import BaseSettings, SettingsConfigDict

from .logger import log


class Config(BaseSettings):
    """Global configuration — reads from environment variables with NEMDB_ prefix."""

    model_config = SettingsConfigDict(env_prefix="NEMDB_", frozen=False)

    cache_dir: Path = Path.home() / ".nemdb_cache"
    filesystem: str = "local"
    temp_dir: Path = Path(gettempdir()) / ".nemweb_temp"

    def set_cache_dir(self, cache_dir: str | Path) -> None:
        self.cache_dir = Path(str(cache_dir).rstrip("/"))
        log.info("Set cache directory to %s", self.cache_dir)

    def set_filesystem(self, filesystem: str) -> None:
        self.filesystem = filesystem
        log.info("Set filesystem to %s", self.filesystem)


config = Config()
