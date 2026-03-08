import pytest

from nemdb.config import config


@pytest.fixture(autouse=True)
def reset_config():
    """Restore config singleton to its original state after each test."""
    original_cache_dir = config.cache_dir
    original_temp_dir = config.temp_dir
    original_filesystem = config.filesystem
    yield
    config.cache_dir = original_cache_dir
    config.temp_dir = original_temp_dir
    config.filesystem = original_filesystem
