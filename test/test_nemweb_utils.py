import os
import pytest
import polars as pl
from unittest.mock import patch, MagicMock

from nemdb.nemweb.utils import cache_response_zip, retry
from nemdb.utils import cache_to_parquet
from nemdb import Config


from pathlib import Path


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
        assert os.path.exists(path)
        with open(path, "rb") as f:
            assert f.read() == content


def test_cache_response_zip_existing_file(temp_dir):
    """Test caching an existing file."""
    url = "http://example.com/test.zip"
    path = os.path.join(temp_dir, "test.zip")
    with open(path, "wb") as f:
        f.write(b"existing content")
    with patch("requests.get") as mock_get:
        new_path = cache_response_zip(url)
        mock_get.assert_not_called()
        assert new_path == path


def test_cache_response_zip_failed_download(temp_dir):
    """Test a failed download."""
    url = "http://example.com/test.zip"
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 404
        with pytest.raises(ValueError):
            cache_response_zip(url)


def test_cache_to_parquet_new_file(temp_dir):
    """Test caching a new parquet file."""
    file_path = os.path.join(temp_dir, "test.parquet")

    @cache_to_parquet(file_path)
    def sample_df():
        return pl.DataFrame({"a": [1, 2], "b": [3, 4]})

    df = sample_df()
    assert os.path.exists(file_path)
    assert df.equals(pl.read_parquet(file_path))


def test_cache_to_parquet_existing_file(temp_dir):
    """Test caching an existing parquet file."""
    file_path = os.path.join(temp_dir, "test.parquet")
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
