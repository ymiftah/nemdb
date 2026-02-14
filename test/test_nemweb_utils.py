from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from nemdb import Config
from nemdb.nemweb.utils import cache_response_zip, retry
from nemdb.utils import cache_to_parquet


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for testing."""
    Config.TEMP_DIR = Path(tmp_path)
    return tmp_path


def test_cache_response_zip_new_file(temp_dir):
    """Test caching a new file."""
    url = "http://example.com/test.zip"
    content = b"test content"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = content
        path = cache_response_zip(url)
        path_obj = Path(path)
        assert path_obj.exists()
        assert path_obj.read_bytes() == content


def test_cache_response_zip_existing_file(temp_dir):
    """Test caching an existing file."""
    url = "http://example.com/test.zip"
    path = Path(temp_dir) / "test.zip"
    path.write_bytes(b"existing content")
    with patch("requests.get") as mock_get:
        new_path = cache_response_zip(url)
        mock_get.assert_not_called()
        assert new_path == str(path)


def test_cache_response_zip_failed_download(temp_dir):
    """Test a failed download."""
    url = "http://example.com/test.zip"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 404
        with pytest.raises(ValueError):
            cache_response_zip(url)


def test_cache_to_parquet_new_file(temp_dir):
    """Test caching a new parquet file."""
    file_path = Path(temp_dir) / "test.parquet"

    @cache_to_parquet(file_path)
    def sample_df():
        return pl.DataFrame({"a": [1, 2], "b": [3, 4]})

    df = sample_df()
    assert file_path.exists()
    assert df.equals(pl.read_parquet(file_path))


def test_cache_to_parquet_existing_file(temp_dir):
    """Test caching an existing parquet file."""
    file_path = Path(temp_dir) / "test.parquet"
    existing_df = pl.DataFrame({"a": [5, 6], "b": [7, 8]})
    existing_df.write_parquet(file_path)

    @cache_to_parquet(file_path)
    def sample_df():
        return pl.DataFrame({"a": [1, 2], "b": [3, 4]})

    df = sample_df()
    assert df.equals(existing_df)


@patch("time.sleep", return_value=None)
def test_retry_success(mock_sleep):
    """Test retry on success."""
    func = MagicMock(side_effect=[Exception, "success"])

    @retry(tries=2, delay=1)
    def f():
        return func()

    assert f() == "success"
    assert func.call_count == 2
