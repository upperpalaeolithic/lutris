"""Tests for Downloader HTTP Range request resume support."""

import os
from unittest.mock import MagicMock, patch

import pytest
import requests

from lutris.util.downloader import Downloader


@pytest.fixture
def dest(tmp_path):
    return str(tmp_path / "file.bin")


# ------------------------------------------------------------------
# Range header is sent when partial file exists
# ------------------------------------------------------------------


class TestRangeHeaderOnResume:
    def test_range_header_sent_when_bytes_on_disk(self, dest):
        """When downloaded_size > 0, Downloader sends Range: bytes=N- header."""
        with open(dest, "wb") as f:
            f.write(b"A" * 500)

        dl = Downloader("https://example.com/file.bin", dest, overwrite=False)
        dl.stop_request = MagicMock()
        dl.stop_request.is_set.return_value = False
        dl.state = dl.DOWNLOADING
        dl.downloaded_size = 500

        captured_headers = {}

        def fake_get(url, headers=None, stream=None, timeout=None, cookies=None):
            captured_headers.update(headers or {})
            resp = MagicMock()
            resp.status_code = 206
            resp.headers = {"Content-Range": "bytes 500-999/1000", "Content-Length": "500"}
            resp.iter_content = MagicMock(return_value=[b"B" * 500])
            return resp

        dl.file_pointer = open(dest, "ab")
        with patch("requests.get", side_effect=fake_get):
            dl._do_download()
        dl.file_pointer.close()

        assert "Range" in captured_headers
        assert captured_headers["Range"] == "bytes=500-"

    def test_no_range_header_when_no_partial_file(self, dest):
        """When downloaded_size == 0, no Range header is sent."""
        dl = Downloader("https://example.com/file.bin", dest)
        dl.stop_request = MagicMock()
        dl.stop_request.is_set.return_value = False
        dl.state = dl.DOWNLOADING
        dl.downloaded_size = 0

        captured_headers = {}

        def fake_get(url, headers=None, stream=None, timeout=None, cookies=None):
            captured_headers.update(headers or {})
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {"Content-Length": "1000"}
            resp.iter_content = MagicMock(return_value=[b"X" * 1000])
            return resp

        dl.file_pointer = open(dest, "wb")
        with patch("requests.get", side_effect=fake_get):
            dl._do_download()
        dl.file_pointer.close()

        assert "Range" not in captured_headers


# ------------------------------------------------------------------
# 206 Partial Content handling
# ------------------------------------------------------------------


class TestPartialContentResponse:
    def test_206_with_content_range_header(self, dest):
        """206 response with Content-Range header sets full_size correctly."""
        dl = Downloader("https://example.com/file.bin", dest)
        dl.stop_request = MagicMock()
        dl.stop_request.is_set.return_value = False
        dl.downloaded_size = 300

        def fake_get(url, headers=None, stream=None, timeout=None, cookies=None):
            resp = MagicMock()
            resp.status_code = 206
            resp.headers = {"Content-Range": "bytes 300-999/1000"}
            resp.iter_content = MagicMock(return_value=[b"Y" * 700])
            return resp

        dl.file_pointer = open(dest, "ab")
        with patch("requests.get", side_effect=fake_get):
            dl._do_download()
        dl.file_pointer.close()

        assert dl.full_size == 1000

    def test_206_without_content_range_falls_back_to_content_length(self, dest):
        """206 without Content-Range falls back to resume_from + Content-Length."""
        dl = Downloader("https://example.com/file.bin", dest)
        dl.stop_request = MagicMock()
        dl.stop_request.is_set.return_value = False
        dl.downloaded_size = 400

        def fake_get(url, headers=None, stream=None, timeout=None, cookies=None):
            resp = MagicMock()
            resp.status_code = 206
            resp.headers = {"Content-Length": "600"}
            resp.iter_content = MagicMock(return_value=[b"Z" * 600])
            return resp

        dl.file_pointer = open(dest, "ab")
        with patch("requests.get", side_effect=fake_get):
            dl._do_download()
        dl.file_pointer.close()

        assert dl.full_size == 1000  # 400 already + 600 remaining


# ------------------------------------------------------------------
# 200 response when Range was requested — server ignored Range header
# ------------------------------------------------------------------


class TestRangeIgnored:
    def test_200_when_range_sent_restarts_from_zero(self, dest):
        """If we sent Range but got 200, downloaded_size is reset and file truncated."""
        # Write some bytes to simulate a partial download
        with open(dest, "wb") as f:
            f.write(b"OLD" * 100)

        dl = Downloader("https://example.com/file.bin", dest)
        dl.stop_request = MagicMock()
        dl.stop_request.is_set.return_value = False
        dl.downloaded_size = 300  # Pretend we had 300 bytes

        def fake_get(url, headers=None, stream=None, timeout=None, cookies=None):
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {"Content-Length": "1000"}
            resp.iter_content = MagicMock(return_value=[b"N" * 1000])
            return resp

        dl.file_pointer = open(dest, "ab")
        with patch("requests.get", side_effect=fake_get):
            dl._do_download()
        dl.file_pointer.close()

        # downloaded_size should be reset to 0 then incremented by new bytes
        assert dl.downloaded_size == 1000  # 0 reset + 1000 new

    def test_200_without_range_sets_full_size(self, dest):
        """200 response sets full_size from Content-Length header."""
        dl = Downloader("https://example.com/file.bin", dest)
        dl.stop_request = MagicMock()
        dl.stop_request.is_set.return_value = False
        dl.downloaded_size = 0

        def fake_get(url, headers=None, stream=None, timeout=None, cookies=None):
            resp = MagicMock()
            resp.status_code = 200
            resp.headers = {"Content-Length": "5000"}
            resp.iter_content = MagicMock(return_value=[b"D" * 5000])
            return resp

        dl.file_pointer = open(dest, "wb")
        with patch("requests.get", side_effect=fake_get):
            dl._do_download()
        dl.file_pointer.close()

        assert dl.full_size == 5000


# ------------------------------------------------------------------
# _prepare_retry — append mode and byte preservation
# ------------------------------------------------------------------


class TestPrepareRetry:
    def test_opens_in_append_mode_when_partial_file_exists(self, dest):
        """_prepare_retry reopens in append mode when dest file has bytes."""
        with open(dest, "wb") as f:
            f.write(b"X" * 2048)

        dl = Downloader("https://example.com/file.bin", dest)
        dl.file_pointer = open(dest, "ab")
        dl._prepare_retry()

        assert dl.file_pointer is not None
        assert dl.downloaded_size == 2048
        dl.file_pointer.close()

    def test_opens_in_write_mode_when_no_file(self, dest):
        """_prepare_retry opens in write mode when dest file doesn't exist."""
        dl = Downloader("https://example.com/file.bin", dest)
        dl.file_pointer = None
        dl._prepare_retry()

        assert dl.file_pointer is not None
        assert dl.downloaded_size == 0
        dl.file_pointer.close()

    def test_downloaded_size_set_from_disk(self, dest):
        """_prepare_retry reads existing file size from disk."""
        data = b"Q" * 4096
        with open(dest, "wb") as f:
            f.write(data)

        dl = Downloader("https://example.com/file.bin", dest)
        dl.downloaded_size = 0  # Out of sync with disk
        dl.file_pointer = open(dest, "ab")
        dl._prepare_retry()

        assert dl.downloaded_size == 4096
        dl.file_pointer.close()

    def test_resets_stall_state(self, dest):
        """_prepare_retry resets stall detection."""
        import time

        dl = Downloader("https://example.com/file.bin", dest)
        dl._stall_start = time.monotonic()
        dl._stall_bytes_at_start = 100
        dl.file_pointer = None
        dl._prepare_retry()
        if dl.file_pointer:
            dl.file_pointer.close()

        assert dl._stall_start is None
        assert dl._stall_bytes_at_start == 0


# ------------------------------------------------------------------
# start() appends when partial file exists and overwrite=False
# ------------------------------------------------------------------


class TestStartAppendMode:
    def test_start_appends_to_existing_partial_file(self, dest):
        """start() with overwrite=False and existing file opens in append mode."""
        partial_data = b"P" * 512
        with open(dest, "wb") as f:
            f.write(partial_data)

        dl = Downloader("https://example.com/file.bin", dest, overwrite=False)
        with patch("lutris.util.downloader.jobs.AsyncCall") as mock_async:
            mock_thread = MagicMock()
            mock_thread.stop_request = MagicMock()
            mock_async.return_value = mock_thread
            dl.start()

        assert dl.downloaded_size == 512
        assert dl.file_pointer is not None
        dl.file_pointer.close()

    def test_start_overwrites_when_overwrite_true(self, dest):
        """start() with overwrite=True deletes existing file before downloading."""
        with open(dest, "wb") as f:
            f.write(b"OLD" * 100)

        dl = Downloader("https://example.com/file.bin", dest, overwrite=True)
        with patch("lutris.util.downloader.jobs.AsyncCall") as mock_async:
            mock_thread = MagicMock()
            mock_thread.stop_request = MagicMock()
            mock_async.return_value = mock_thread
            dl.start()

        assert dl.downloaded_size == 0
        if dl.file_pointer:
            dl.file_pointer.close()
