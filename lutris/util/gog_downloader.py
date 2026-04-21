"""Multi-connection parallel downloader for GOG game files.

Uses HTTP Range requests to download different byte ranges of a file
simultaneously across multiple threads, significantly improving download
speeds for large GOG installer files.

This downloader is a drop-in replacement for the standard Downloader class,
maintaining API compatibility with DownloadProgressBox and
DownloadCollectionProgressBox.
"""

import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from requests.adapters import HTTPAdapter

from lutris import __version__
from lutris.util import jobs
from lutris.util.download_progress import DownloadProgress
from lutris.util.downloader import DEFAULT_CHUNK_SIZE, Downloader, get_time
from lutris.util.log import logger


class GOGDownloader(Downloader):
    """Multi-connection parallel downloader optimized for GOG CDN downloads.

    Downloads large files using multiple simultaneous HTTP Range requests,
    each writing to a different region of the output file. Falls back to
    single-stream download if the server doesn't support Range requests
    or the file is too small to benefit from parallelism.

    Designed to be API-compatible with Downloader so it works seamlessly
    with DownloadProgressBox and DownloadCollectionProgressBox.
    """

    DEFAULT_WORKERS = 4
    MIN_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB minimum per worker
    RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2  # seconds between retries

    def __init__(
        self,
        url: str,
        dest: str,
        overwrite: bool = False,
        referer: str | None = None,
        cookies: Any = None,
        headers: dict[str, str] | None = None,
        session: requests.Session | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        num_workers: int = DEFAULT_WORKERS,
    ) -> None:
        super().__init__(
            url=url,
            dest=dest,
            overwrite=overwrite,
            referer=referer,
            cookies=cookies,
            headers=headers,
            session=session,
            chunk_size=chunk_size,
        )
        self.num_workers = max(1, num_workers)
        self._download_lock = threading.Lock()
        self._progress: DownloadProgress | None = None
        # Pipelining: bounded queue decouples download I/O from disk writes
        self._write_queue: queue.Queue = queue.Queue(maxsize=64)
        self._writer_error: Exception | None = None
        self._writer_error_event = threading.Event()
        # Create a dedicated session with connection pooling sized for our workers
        self._parallel_session = requests.Session()
        adapter = HTTPAdapter(pool_maxsize=self.num_workers + 2)
        self._parallel_session.mount("https://", adapter)
        self._parallel_session.mount("http://", adapter)
        self._parallel_session.headers["User-Agent"] = "Lutris/%s" % __version__

    def __repr__(self):
        return "GOG parallel downloader (%d workers) for %s" % (self.num_workers, self.url)

    def start(self):
        """Start parallel download job.

        If a previous download was interrupted (hibernate, crash, network
        error), the progress file and partial destination file are detected
        and the download resumes from the last completed byte ranges
        instead of starting over.
        """
        logger.debug("⬇ GOG parallel (%d workers): %s", self.num_workers, self.url)
        self.state = self.DOWNLOADING
        self.last_check_time = get_time()

        # Check for resumable progress before deleting anything
        can_resume = False
        if os.path.isfile(self.dest):
            progress = DownloadProgress(self.dest)
            if progress.load() and progress.get_remaining_ranges():
                can_resume = True
                logger.info(
                    "GOG download: found resumable progress for %s "
                    "(%d/%d ranges complete, %d bytes already downloaded)",
                    os.path.basename(self.dest),
                    len(progress.completed_ranges),
                    len(progress.total_ranges),
                    progress.get_completed_size(),
                )

        if not can_resume and self.overwrite and os.path.isfile(self.dest):
            os.remove(self.dest)
            # Also clean stale progress files
            progress_path = DownloadProgress.progress_path_for(self.dest)
            if os.path.isfile(progress_path):
                os.remove(progress_path)

        # Workers manage their own file I/O - no shared file_pointer needed
        self.file_pointer = None
        self.thread = jobs.AsyncCall(self.async_download, None)
        self.stop_request = self.thread.stop_request

    def cancel(self):
        """Request download stop and remove destination file.

        Explicit user cancellation removes both the partial file and
        the progress file so the next attempt starts fresh.
        """
        logger.debug("❌ GOG parallel: %s", self.url)
        self.state = self.CANCELLED
        if self.stop_request:
            self.stop_request.set()
        # No shared file_pointer to close - workers handle their own
        if os.path.isfile(self.dest):
            os.remove(self.dest)
        # Clean up progress file on explicit cancel
        if self._progress:
            self._progress.cleanup()
            self._progress = None

    def on_download_completed(self):
        """Mark download as complete and clean up progress file."""
        if self.state == self.CANCELLED:
            return
        logger.debug("✅ GOG parallel download finished: %s", self.url)
        if not self.downloaded_size:
            logger.warning("Downloaded file is empty")
        if not self.full_size:
            self.progress_fraction = 1.0
            self.progress_percentage = 100
        self.state = self.COMPLETED
        # No shared file_pointer to close
        # Remove progress file — download is complete
        if self._progress:
            self._progress.cleanup()
            self._progress = None

    def _build_request_headers(self) -> dict[str, str]:
        """Build HTTP headers for download requests."""
        headers: dict[str, str] = dict(requests.utils.default_headers())
        headers["User-Agent"] = "Lutris/%s" % __version__
        if self.referer:
            headers["Referer"] = self.referer
        if self.headers:
            headers.update(self.headers)
        return headers

    def _calculate_ranges(self, file_size: int) -> list[tuple[int, int]]:
        """Split file into byte ranges for parallel download.

        Returns a list of (start, end) tuples representing inclusive byte ranges.
        """
        chunk_size = file_size // self.num_workers
        ranges = []
        for i in range(self.num_workers):
            start = i * chunk_size
            end = file_size - 1 if i == self.num_workers - 1 else (i + 1) * chunk_size - 1
            ranges.append((start, end))
        return ranges

    # Checkpoint partial range progress every 64 MB of writes per range.
    # This caps the worst-case re-download after an interrupted range to 64 MB.
    _PARTIAL_CHECKPOINT_INTERVAL = 64 * 1024 * 1024  # 64 MB

    def _writer_loop(self) -> None:
        """Dedicated writer thread: dequeues chunks and writes to disk.

        Consumes (offset, data, range_start, range_end, is_last) tuples from
        the write queue. A None sentinel signals the writer to exit.

        All disk I/O and progress tracking happens here, keeping download
        workers free from disk latency.
        """
        # Last checkpointed byte count per range key (range_start, range_end).
        # Position-based: tracks bytes correctly even when a worker retries
        # and re-downloads from range_start.
        _range_last_saved: dict[tuple[int, int], int] = {}

        try:
            with open(self.dest, "r+b") as f:
                while True:
                    if self.stop_request and self.stop_request.is_set():
                        # Drain remaining items on cancel
                        break
                    try:
                        item = self._write_queue.get(timeout=0.5)
                    except queue.Empty:
                        continue

                    if item is None:
                        # Sentinel — all downloads complete
                        break

                    offset, data, range_start, range_end, is_last = item
                    f.seek(offset)
                    f.write(data)
                    with self._download_lock:
                        self.downloaded_size += len(data)
                    self.progress_event.set()

                    # Position-based byte tracking: bytes correctly on disk from range_start.
                    # offset + len(data) - range_start gives actual disk position, which
                    # resets naturally when a worker retries and starts over from range_start.
                    rk = (range_start, range_end)
                    written = offset + len(data) - range_start

                    if rk not in _range_last_saved:
                        # First chunk: seed from current offset so we don't immediately
                        # re-checkpoint bytes already recorded in a prior session.
                        _range_last_saved[rk] = offset - range_start
                    elif offset - range_start < _range_last_saved[rk]:
                        # Worker retried from range_start; its offset is BEHIND our last
                        # checkpoint. The previously saved checkpoint is now stale — clear it
                        # so a future resume doesn't skip past data that isn't on disk yet.
                        if self._progress:
                            try:
                                self._progress.mark_range_progress(range_start, range_end, 0)
                            except Exception as ex:
                                logger.warning(
                                    "Failed to reset stale checkpoint for %d-%d: %s", range_start, range_end, ex
                                )
                        _range_last_saved[rk] = 0

                    # If this write completes a range, mark it in progress file
                    if is_last:
                        if self._progress:
                            try:
                                self._progress.mark_range_complete(range_start, range_end)
                            except Exception as ex:
                                logger.warning("Failed to mark range %d-%d complete: %s", range_start, range_end, ex)
                        _range_last_saved.pop(rk, None)
                    elif self._progress:
                        # Checkpoint partial progress every _PARTIAL_CHECKPOINT_INTERVAL bytes
                        last_saved = _range_last_saved.get(rk, 0)
                        if written - last_saved >= self._PARTIAL_CHECKPOINT_INTERVAL:
                            try:
                                self._progress.mark_range_progress(range_start, range_end, written)
                                _range_last_saved[rk] = written
                            except Exception as ex:
                                logger.warning(
                                    "Failed to checkpoint range %d-%d at %d bytes: %s",
                                    range_start,
                                    range_end,
                                    written,
                                    ex,
                                )
        except Exception as ex:
            logger.error("Writer thread failed: %s", ex)
            self._writer_error = ex
            self._writer_error_event.set()

    def async_download(self):
        """Execute multi-connection parallel download with resume support.

        On each invocation the method:
        1. Probes the server for the final URL, file size, and Range support.
        2. Checks for an existing ``.progress`` file alongside the
           destination.  If one is found and the file size matches, the
           download resumes from only the remaining byte ranges.
        3. Pre-allocates (or reuses) the destination file and launches
           parallel workers for the outstanding ranges.
        4. On success the progress file is removed.  On failure or
           interruption (hibernate, crash) the progress file and partial
           destination are preserved for the next attempt.
        """
        try:
            headers = self._build_request_headers()

            # Step 1: Resolve URL (follow redirects) and check capabilities
            final_url, file_size, supports_range = self._probe_server(headers)

            # If the probe returned a suspiciously small or zero size, check whether a
            # previous session recorded a different (presumably correct) size. GOG CDN
            # signed URLs expire after ~1 hour; an expired URL often redirects to a small
            # HTML error page that has a tiny or absent Content-Length. Trusting that tiny
            # size would wipe the partially-downloaded file. Instead, fall back to the
            # stored progress file's known-good size so the download can continue cleanly
            # (and fail with a network error, not data loss, if the URL is truly dead).
            stored_progress = DownloadProgress(self.dest)
            if stored_progress.load() and stored_progress.file_size > self.MIN_CHUNK_SIZE:
                if file_size != stored_progress.file_size:
                    logger.warning(
                        "GOG probe returned file_size=%d which differs from stored %d bytes; "
                        "URL may be expired or redirected to an error page — "
                        "trusting stored size to protect partial download.",
                        file_size,
                        stored_progress.file_size,
                    )
                    file_size = stored_progress.file_size
                    # supports_range may be unreliable if probe hit wrong endpoint;
                    # force parallel path since we know the file is large enough.
                    supports_range = True

            self.full_size = file_size

            # Fall back to single-stream if Range not supported or file too small
            if not supports_range or file_size < self.MIN_CHUNK_SIZE * 2:
                logger.info(
                    "GOG download: falling back to single-stream (range=%s, size=%d bytes)",
                    supports_range,
                    file_size,
                )
                self._single_stream_download(final_url, headers)
                return

            self.progress_event.set()  # Signal that size is known

            # Step 2: Check for resumable progress
            self._progress = stored_progress  # reuse already-loaded progress object
            ranges_to_download = None

            if self._progress.load() and self._progress.is_compatible(file_size):
                remaining = self._progress.get_remaining_ranges()
                if remaining:
                    already_done = self._progress.get_completed_size()
                    partial_done = self._progress.get_partial_size()
                    logger.info(
                        "GOG download: resuming — %d/%d ranges done, "
                        "%d bytes complete + %d bytes checkpointed, %d bytes remaining",
                        len(self._progress.completed_ranges),
                        len(self._progress.total_ranges),
                        already_done,
                        partial_done,
                        file_size - already_done - partial_done,
                    )
                    # Credit completed and checkpointed bytes to progress display
                    with self._download_lock:
                        self.downloaded_size = already_done + partial_done
                    ranges_to_download = remaining
                else:
                    # All ranges already complete — verify file exists & size
                    if os.path.isfile(self.dest) and os.path.getsize(self.dest) == file_size:
                        logger.info(
                            "GOG download: all ranges already complete, skipping download of %s",
                            os.path.basename(self.dest),
                        )
                        with self._download_lock:
                            self.downloaded_size = file_size
                        self.on_download_completed()
                        return

            # Step 3: Compute ranges (fresh or from progress)
            if ranges_to_download is None:
                # Guard: refuse to wipe a large existing partial download. This can happen
                # when the GOG CDN URL expired and the probe returned wrong metadata. Wiping
                # a 30+ GB file and restarting from zero is never the right choice here.
                existing_size = os.path.getsize(self.dest) if os.path.isfile(self.dest) else 0
                if existing_size > self.MIN_CHUNK_SIZE:
                    raise RuntimeError(
                        "Refusing to overwrite existing %d-byte partial download "
                        "(probe returned file_size=%d; stored was %d). "
                        "The download URL may have expired — please cancel and restart "
                        "the download to obtain a fresh URL."
                        % (
                            existing_size,
                            file_size,
                            self._progress.file_size if self._progress._data else 0,
                        )
                    )
                # Genuinely fresh download — pre-allocate output file
                with open(self.dest, "wb") as f:
                    f.truncate(file_size)
                all_ranges = self._calculate_ranges(file_size)
                self._progress.create(final_url, file_size, all_ranges)
                ranges_to_download = all_ranges
            else:
                # Resuming — verify dest file exists and has correct size
                if not os.path.isfile(self.dest) or os.path.getsize(self.dest) != file_size:
                    logger.warning("GOG download: dest file missing or wrong size during resume, starting fresh")
                    with open(self.dest, "wb") as f:
                        f.truncate(file_size)
                    all_ranges = self._calculate_ranges(file_size)
                    self._progress.create(final_url, file_size, all_ranges)
                    ranges_to_download = all_ranges
                    with self._download_lock:
                        self.downloaded_size = 0

            total_remaining = sum(e - s + 1 for s, e in ranges_to_download)
            logger.info(
                "GOG parallel download: %d workers, %d ranges to download, %d MB remaining of %d MB total",
                self.num_workers,
                len(ranges_to_download),
                total_remaining // (1024 * 1024),
                file_size // (1024 * 1024),
            )

            # Step 4: Download chunks in parallel with pipelined writes
            # Reset writer error state
            self._writer_error = None
            self._writer_error_event.clear()

            # Start dedicated writer thread
            writer_thread = threading.Thread(target=self._writer_loop, name="GOGDownloader-writer", daemon=True)
            writer_thread.start()

            errors = []
            try:
                with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                    future_to_range = {}
                    for start, end in ranges_to_download:
                        future = executor.submit(self._download_range, final_url, headers, start, end)
                        future_to_range[future] = (start, end)

                    for future in as_completed(future_to_range):
                        try:
                            future.result()
                        except Exception as ex:
                            rng = future_to_range[future]
                            logger.error("Worker failed for range %d-%d: %s", rng[0], rng[1], ex)
                            errors.append(ex)
                            # Signal other workers to stop
                            if self.stop_request:
                                self.stop_request.set()
            finally:
                # Signal writer thread to exit and wait for it
                self._write_queue.put(None)
                writer_thread.join(timeout=30)

            # Check for writer errors
            if self._writer_error:
                raise self._writer_error

            if errors:
                raise errors[0]

            self.on_download_completed()
        except Exception as ex:
            logger.exception("GOG parallel download failed: %s", ex)
            self.on_download_failed(ex)

    def _probe_server(self, headers: dict) -> tuple[str, int, bool]:
        """Probe the server to determine final URL, file size, and Range support.

        Uses a HEAD request to follow redirects (e.g., GOG API → CDN URL),
        get Content-Length, and check Accept-Ranges header.

        Returns:
            Tuple of (final_url, file_size, supports_range)
        """
        resp = self._parallel_session.head(
            self.url, headers=headers, allow_redirects=True, timeout=30, cookies=self.cookies
        )
        resp.raise_for_status()

        final_url = resp.url
        file_size = int(resp.headers.get("Content-Length", 0))
        accept_ranges = resp.headers.get("Accept-Ranges", "")
        supports_range = "bytes" in accept_ranges.lower()

        # Some servers don't advertise Accept-Ranges but still support it.
        # If we got a Content-Length, try a small Range request to verify.
        if file_size and not supports_range:
            supports_range = self._test_range_support(final_url, headers)

        logger.debug(
            "GOG probe: url=%s, size=%d, range=%s",
            final_url[:80],
            file_size,
            supports_range,
        )
        return final_url, file_size, supports_range

    def _test_range_support(self, url: str, headers: dict) -> bool:
        """Test if server actually supports Range requests with a small probe."""
        try:
            test_headers = dict(headers)
            test_headers["Range"] = "bytes=0-0"
            resp = self._parallel_session.get(url, headers=test_headers, stream=True, timeout=10, cookies=self.cookies)
            resp.close()
            return resp.status_code == 206
        except Exception:
            return False

    def _download_range(self, url: str, headers: dict, start: int, end: int) -> None:
        """Download a specific byte range and enqueue data for the writer thread.

        Each worker downloads its assigned byte range and puts chunks into
        the write queue for the dedicated writer thread. Workers never
        perform file I/O directly, keeping them free from disk latency.

        Retries up to RETRY_ATTEMPTS times with exponential backoff.
        """
        # Check for a mid-range checkpoint from a previous interrupted session.
        # If bytes_checkpointed > 0, those bytes are already on disk; skip them.
        bytes_checkpointed = self._progress.get_range_progress(start, end) if self._progress else 0

        for attempt in range(self.RETRY_ATTEMPTS):
            try:
                range_headers = dict(headers)
                # On first attempt use checkpoint offset; on subsequent retries restart
                # the range from scratch (the checkpoint may have been from a stale session).
                resume_from = start + bytes_checkpointed if attempt == 0 else start
                range_headers["Range"] = "bytes=%d-%d" % (resume_from, end)

                response = self._parallel_session.get(
                    url,
                    headers=range_headers,
                    stream=True,
                    timeout=30,
                    cookies=self.cookies,
                )

                if response.status_code not in (200, 206):
                    raise requests.HTTPError(
                        "HTTP %d for range %d-%d" % (response.status_code, start, end),
                        response=response,
                    )

                # If server returned 200 (ignoring Range), only write our portion
                if response.status_code == 200:
                    logger.warning(
                        "Server ignored Range header, reading full response for range %d-%d",
                        start,
                        end,
                    )
                    self._write_from_full_response(response, start, end)
                    return

                # Normal 206 Partial Content response — enqueue for writer
                self._reset_stall_state()
                stream_bytes = 0
                current_offset = resume_from
                range_size = end - resume_from + 1

                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if self.stop_request and self.stop_request.is_set():
                        return
                    if self._writer_error_event.is_set():
                        return  # Writer failed, stop downloading
                    if chunk:
                        stream_bytes += len(chunk)
                        is_last_chunk = stream_bytes >= range_size
                        self._write_queue.put((current_offset, chunk, start, end, is_last_chunk))
                        current_offset += len(chunk)
                        self._check_stall(stream_bytes)

                return  # Success

            except Exception as ex:
                if self.stop_request and self.stop_request.is_set():
                    return  # Cancelled, don't retry
                if attempt < self.RETRY_ATTEMPTS - 1:
                    wait = self.RETRY_DELAY * (attempt + 1)
                    logger.warning(
                        "GOG range %d-%d attempt %d/%d failed: %s, retrying in %ds...",
                        start,
                        end,
                        attempt + 1,
                        self.RETRY_ATTEMPTS,
                        ex,
                        wait,
                    )
                    time.sleep(wait)
                else:
                    raise

    def _write_from_full_response(self, response: requests.Response, start: int, end: int) -> None:
        """Handle the case where server returns 200 instead of 206.

        Read the full response but only enqueue our byte range portion.
        This is a fallback for non-compliant servers.
        """
        bytes_read = 0
        current_offset = start
        range_size = end - start + 1
        enqueued_bytes = 0

        for chunk in response.iter_content(chunk_size=self.chunk_size):
            if self.stop_request and self.stop_request.is_set():
                return
            if self._writer_error_event.is_set():
                return
            if not chunk:
                continue

            # Only write the portion that falls within our range
            chunk_start = bytes_read
            chunk_end = bytes_read + len(chunk)

            if chunk_end <= start:
                # Before our range, skip
                bytes_read += len(chunk)
                continue
            elif chunk_start >= end + 1:
                # Past our range, done
                break
            else:
                # Calculate the slice of this chunk we need
                slice_start = max(0, start - chunk_start)
                slice_end = min(len(chunk), end + 1 - chunk_start)
                data = chunk[slice_start:slice_end]
                enqueued_bytes += len(data)
                is_last = enqueued_bytes >= range_size
                self._write_queue.put((current_offset, data, start, end, is_last))
                current_offset += len(data)

            bytes_read += len(chunk)
            if bytes_read >= end + 1:
                break

    def _single_stream_download(self, url: str, headers: dict) -> None:
        """Fallback single-stream download when Range requests aren't supported.

        Attempts to resume from an existing partial file via a Range header.
        If the server honours it (206) we append; otherwise we restart.
        """
        existing = os.path.getsize(self.dest) if os.path.isfile(self.dest) else 0
        req_headers = dict(headers)
        if existing > 0:
            req_headers["Range"] = "bytes=%d-" % existing

        response = self._parallel_session.get(url, headers=req_headers, stream=True, timeout=30, cookies=self.cookies)
        response.raise_for_status()

        if response.status_code == 206:
            content_range = response.headers.get("Content-Range", "")
            if content_range and "/" in content_range:
                self.full_size = int(content_range.split("/")[-1])
            else:
                self.full_size = existing + int(response.headers.get("Content-Length", "").strip() or 0)
            self.downloaded_size = existing
            file_mode = "ab"
        else:
            # Server returned 200 (ignored our Range request or URL redirected to error page).
            # Guard: if a large partial file already exists, refuse to overwrite it — this
            # would silently replace gigabytes of downloaded data with an HTML error page.
            if existing > self.MIN_CHUNK_SIZE:
                response.close()
                raise RuntimeError(
                    "Server returned %d instead of 206 for existing %d-byte partial file. "
                    "The download URL may have expired — please cancel and restart "
                    "to obtain a fresh URL." % (response.status_code, existing)
                )
            self.full_size = int(response.headers.get("Content-Length", "").strip() or 0)
            self.downloaded_size = 0
            file_mode = "wb"

        self.progress_event.set()

        with open(self.dest, file_mode) as f:
            for chunk in response.iter_content(chunk_size=self.chunk_size):
                if self.stop_request and self.stop_request.is_set():
                    break
                if chunk:
                    self.downloaded_size += len(chunk)
                    f.write(chunk)
                self.progress_event.set()

        self.on_download_completed()
