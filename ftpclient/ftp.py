import collections
import threading
import time
from typing import BinaryIO, Callable
from urllib.parse import unquote, urlparse

from fman import load_json

from .ftp_session import (
    DEFAULT_PORTS, FtpConnectionError, close_finished_children, close_host,
    format_base_url, is_connection_limit_error,
    ftputil, open_ftp_host,
)


# Bumped whenever this plugin changes something on a server. The connection
# pool is keyed by thread id, so an upload done on a task thread cannot reach
# the stat cache of the FTPHost the model thread reads listings from. Each
# pooled host therefore drops its stat cache the next time it is used.
_stat_cache_generation = 0


def invalidate_stat_caches():
    global _stat_cache_generation
    _stat_cache_generation += 1


class FtpWrapper():
    __conn_pool = {}
    __conn_timestamps = {}
    __conn_threads = {}
    __last_noop_check = {}
    __conn_base_urls = {}  # hash → base_url (e.g., "ftp://user@host:21")
    __last_visited_paths = {}  # base_url → last_full_url
    # hash → number of open `with` blocks. The pool is cleaned from whichever
    # thread connects next, so without this a transfer running longer than
    # __CONNECTION_TIMEOUT is closed under its own feet by a pane listing.
    __in_use = collections.Counter()
    __pool_lock = threading.Lock()
    # Connection timeout: close connections idle for more than 2 minutes
    __CONNECTION_TIMEOUT = 120
    # NOOP validation cache: only validate connection if last check was this long ago
    __NOOP_CHECK_INTERVAL = 5.0  # seconds

    def __init__(self, url):
        u = self._get_bookmark(url)
        self._url = u.geturl()
        self._scheme = '%s://' % (u.scheme,)
        self._path = u.path or '/'
        self._host = u.hostname or ''
        self._port = u.port or DEFAULT_PORTS.get(u.scheme, 21)
        self._user = unquote(u.username or '')
        self._passwd = unquote(u.password or '')

    def __enter__(self):
        with self.__pool_lock:
            self._cleanup_stale_connections()
            # Marked first, so no other thread's cleanup closes the connection
            # while _connect() below checks or opens it.
            self.__in_use[self.hash] += 1
        # Outside the lock: talking to the server can take long, and the
        # connection is this thread's own (see `hash`). Under the lock, one
        # server that does not answer froze every other thread's FTP access.
        try:
            self._connect()
        except BaseException:
            with self.__pool_lock:
                self._release()
            raise
        return self

    def _connect(self):
        """Make sure the pool holds a live connection for this wrapper."""
        conn = self.__conn_pool.get(self.hash)
        if conn is not None:
            current_time = time.time()
            if self._check_alive(conn):
                self.__conn_timestamps[self.hash] = current_time
                return
            else:
                with self.__pool_lock:
                    self._remove_connection(self.hash)

        try:
            host = self._open_host()
        except FtpConnectionError as error:
            if not is_connection_limit_error(error):
                raise
            with self.__pool_lock:
                stale_hashes = [
                    conn_hash
                    for conn_hash, base_url in self.__conn_base_urls.items()
                    if base_url == self._get_base_url()
                    and conn_hash not in self.__in_use
                ]
                for conn_hash in stale_hashes:
                    self._remove_connection(conn_hash)
            host = self._open_host()
        current_time = time.time()
        # Under the lock: other threads iterate these dicts while cleaning up.
        with self.__pool_lock:
            self.__conn_pool[self.hash] = host
            self.__conn_timestamps[self.hash] = current_time
            self.__conn_threads[self.hash] = threading.get_ident()
            self.__last_noop_check[self.hash] = current_time
            self.__conn_base_urls[self.hash] = self._get_base_url()

    def _check_alive(self, host):
        """Return whether a pooled FTP host is still usable."""
        if host.closed:
            return False
        current_time = time.time()
        last_check = self.__last_noop_check.get(self.hash, 0)
        if current_time - last_check <= self.__NOOP_CHECK_INTERVAL:
            return True
        try:
            host._session.voidcmd('NOOP')
        except Exception:
            return False
        self.__last_noop_check[self.hash] = current_time
        return True

    def _open_host(self):
        """Open an FTP host for this wrapper."""
        return open_ftp_host(
            self._scheme, self._host, self._port, self._user, self._passwd)

    def _release(self):
        # Call holding __pool_lock.
        self.__in_use[self.hash] -= 1
        if not self.__in_use[self.hash]:
            del self.__in_use[self.hash]

    def __exit__(self, exc_type, exc_value, exc_tb):
        with self.__pool_lock:
            self._release()
            if self.hash in self.__conn_pool:
                # The idle clock starts when the operation ends: a long
                # transfer must not look idle the moment it finishes.
                self.__conn_timestamps[self.hash] = time.time()
                # Clean up stale child connections after each operation
                self._after_use(self.__conn_pool[self.hash])
        return

    def drop_connection(self):
        """Close this thread's pooled session so the next `with` reconnects."""
        with self.__pool_lock:
            self._remove_connection(self.hash)

    def _after_use(self, host):
        """Clean up resources left by a completed FTP operation."""
        close_finished_children(host)

    @staticmethod
    def _get_bookmark(url):
        u = urlparse(url)
        url_without_path = u._replace(path='').geturl()

        bookmarks = \
            load_json('FTP Bookmarks.json', default={})
        # Replace base URL -if found in bookmarks-, keep the same path
        if url_without_path in bookmarks:
            u = urlparse(bookmarks[url_without_path][0])._replace(path=u.path)

        return u

    @property
    def hash(self):
        return hash((
            threading.get_ident(), self._scheme, self._host, self._port, self._user,
            self._passwd))

    def _get_base_url(self):
        """Return the base URL (without path) for this connection."""
        return format_base_url(
            self._scheme, self._user, self._host, self._port)

    @property
    def conn(self):
        if self.hash not in self.__conn_pool:
            raise Exception('Not connected')
        ftp_host = self.__conn_pool[self.hash]
        # ponytail: one global generation, so any write drops every host's whole
        # stat cache once. Per-path invalidate() if the extra LIST ever hurts.
        if getattr(ftp_host, '_fman_cache_gen', -1) != _stat_cache_generation:
            ftp_host._fman_cache_gen = _stat_cache_generation
            ftp_host.stat_cache.clear()
        return ftp_host

    @property
    def path(self):
        return self._path

    def upload(
        self, src_file: BinaryIO,
        on_chunk: Callable[[bytes], None] | None = None,
    ) -> None:
        """Upload a file through the pooled FTP session."""
        with ftputil.error.ftplib_error_to_ftp_os_error:
            if on_chunk is None:
                self.conn._session.storbinary(
                    'STOR ' + self.path, src_file)
            else:
                self.conn._session.storbinary(
                    'STOR ' + self.path, src_file, callback=on_chunk)

    def download(
        self, dst_file: BinaryIO, on_chunk: Callable[[bytes], None],
    ) -> None:
        """Download a file and report each received block."""
        def write_chunk(block):
            dst_file.write(block)
            on_chunk(block)

        with ftputil.error.ftplib_error_to_ftp_os_error:
            self.conn._session.retrbinary(
                'RETR ' + self.path, write_chunk)

    @classmethod
    def _remove_connection(cls, conn_hash):
        """Remove a connection from the pool and close it."""
        if conn_hash in cls.__conn_pool:
            close_host(cls.__conn_pool.pop(conn_hash))
            cls.__conn_timestamps.pop(conn_hash, None)
            cls.__conn_threads.pop(conn_hash, None)
            cls.__last_noop_check.pop(conn_hash, None)
            cls.__conn_base_urls.pop(conn_hash, None)

    def _cleanup_stale_connections(self):
        """Remove dead-thread connections and connections idle for too long."""
        current_time = time.time()
        live_threads = {thread.ident for thread in threading.enumerate()}
        # A connection in use is never idle, however old its timestamp.
        stale_hashes = [
            conn_hash
            for conn_hash, timestamp in self.__conn_timestamps.items()
            if conn_hash not in self.__in_use
            and (
                (
                    self.__conn_threads.get(conn_hash) is not None
                    and self.__conn_threads[conn_hash] not in live_threads
                )
                or current_time - timestamp > self.__CONNECTION_TIMEOUT
            )
        ]

        for conn_hash in stale_hashes:
            self._remove_connection(conn_hash)

    @classmethod
    def close_all_connections(cls):
        """Close all connections in the pool. Useful for cleanup."""
        with cls.__pool_lock:
            for conn_hash in list(cls.__conn_pool.keys()):
                close_host(cls.__conn_pool[conn_hash])
            cls.__conn_pool.clear()
            cls.__conn_timestamps.clear()
            cls.__conn_threads.clear()
            cls.__last_noop_check.clear()
            cls.__conn_base_urls.clear()
            cls.__last_visited_paths.clear()

    @classmethod
    def record_visited_path(cls, url):
        """Record the last visited path for a connection's base URL."""
        u = cls._get_bookmark(url)

        host = u.hostname or ''
        port = u.port or DEFAULT_PORTS.get(u.scheme, 21)
        user = unquote(u.username or '')
        base_url = format_base_url(
            '%s://' % u.scheme, user, host, port)

        with cls.__pool_lock:
            cls.__last_visited_paths[base_url] = url

    @classmethod
    def get_open_connections(cls):
        """Return list of (base_url, last_visited_url) for active connections."""
        with cls.__pool_lock:
            active_base_urls = set(cls.__conn_base_urls.values())
            result = []
            for base_url in active_base_urls:
                last_url = cls.__last_visited_paths.get(base_url, base_url + '/')
                result.append((base_url, last_url))
            return result

    @classmethod
    def close_connection_by_url(cls, base_url):
        """Close a specific connection by its base URL."""
        with cls.__pool_lock:
            hashes_to_remove = [
                h for h, url in cls.__conn_base_urls.items()
                if url == base_url
            ]
            for conn_hash in hashes_to_remove:
                cls._remove_connection(conn_hash)
            if base_url in cls.__last_visited_paths:
                del cls.__last_visited_paths[base_url]
