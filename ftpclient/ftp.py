import ftplib
import threading
import time
from urllib.parse import unquote, urlparse

from fman import load_json

try:
    import ftputil
except ImportError:
    import os
    import sys
    sys.path.append(
        os.path.join(os.path.dirname(__file__), 'ftputil-3.4'))
    import ftputil


class FtpSession(ftplib.FTP):
    def __init__(self, host, port, user, password):
        super().__init__()
        self.connect(host, port)
        # FIXME ftplib.error_temp: 421 Too many connections from the
        #       same IP address.
        self.login(user, password)


class FtpTlsSession(ftplib.FTP_TLS):
    def __init__(self, host, port, user, password):
        super().__init__()
        self.connect(host, port)
        self.login(user, password)
        self.prot_p()


class FtpWrapper():
    __conn_pool = {}
    __conn_timestamps = {}
    __last_noop_check = {}
    __conn_base_urls = {}  # hash → base_url (e.g., "ftp://user@host:21")
    __last_visited_paths = {}  # base_url → last_full_url
    __pool_lock = threading.Lock()
    # Connection timeout: close connections idle for more than 2 minutes
    __CONNECTION_TIMEOUT = 120
    # Max pool size: limit to 3 FTPHost objects per server
    # (each FTPHost can spawn multiple child connections)
    __MAX_POOL_SIZE = 3
    # NOOP validation cache: only validate connection if last check was this long ago
    __NOOP_CHECK_INTERVAL = 5.0  # seconds

    def __init__(self, url):
        u = self._get_bookmark(url)
        self._url = u.geturl()
        self._scheme = '%s://' % (u.scheme,)
        self._path = u.path or '/'
        self._host = u.hostname or ''
        self._port = u.port or 21
        self._user = unquote(u.username or '')
        self._passwd = unquote(u.password or '')

    def __enter__(self):
        with self.__pool_lock:
            # Clean up stale connections periodically
            self._cleanup_stale_connections()

            if self.hash in self.__conn_pool:
                conn = self.__conn_pool[self.hash]
                current_time = time.time()

                # Check if connection is still valid and not closed
                if not conn.closed:
                    # Only validate with NOOP if we haven't checked recently
                    last_check = self.__last_noop_check.get(self.hash, 0)
                    needs_validation = (current_time - last_check) > self.__NOOP_CHECK_INTERVAL

                    if needs_validation:
                        try:
                            conn._session.voidcmd('NOOP')
                            self.__last_noop_check[self.hash] = current_time
                        except:
                            # Connection is stale, remove it from pool
                            self._remove_connection(self.hash)
                            # Fall through to create new connection
                        else:
                            # NOOP succeeded, connection is valid
                            self.__conn_timestamps[self.hash] = current_time
                            return self
                    else:
                        # Skip NOOP, connection was validated recently
                        self.__conn_timestamps[self.hash] = current_time
                        return self
                else:
                    # Connection is closed, remove it from pool
                    self._remove_connection(self.hash)

            # Create new connection
            session_factory = \
                FtpTlsSession if self._scheme == 'ftps://' else FtpSession
            ftp_host = ftputil.FTPHost(
                self._host, self._port, self._user, self._passwd,
                session_factory=session_factory)

            # Increase stat cache size for large directories
            # Default is 5000, which causes cache eviction in large dirs
            ftp_host.stat_cache.resize(20000)

            current_time = time.time()
            self.__conn_pool[self.hash] = ftp_host
            self.__conn_timestamps[self.hash] = current_time
            self.__last_noop_check[self.hash] = current_time
            # Track base URL for this connection
            base_url = self._get_base_url()
            self.__conn_base_urls[self.hash] = base_url
            return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        # Clean up stale child connections after each operation
        if self.hash in self.__conn_pool:
            self._cleanup_children(self.__conn_pool[self.hash])
        return

    def _cleanup_children(self, ftp_host):
        """Close stale child connections that have finished their file transfers."""
        stale_indices = []
        for i, host in enumerate(ftp_host._children):
            if host._file.closed:
                try:
                    host._session.close()
                except:
                    pass
                stale_indices.append(i)

        # Remove stale children in reverse order to preserve indices
        for i in reversed(stale_indices):
            del ftp_host._children[i]

    def _get_bookmark(self, url):
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
            threading.get_ident(), self._host, self._port, self._user,
            self._passwd))

    def _get_base_url(self):
        """Return the base URL (without path) for this connection."""
        if self._user:
            return '%s%s@%s:%d' % (self._scheme, self._user, self._host, self._port)
        else:
            return '%s%s:%d' % (self._scheme, self._host, self._port)

    @property
    def conn(self):
        if self.hash not in self.__conn_pool:
            raise Exception('Not connected')
        return self.__conn_pool[self.hash]

    @property
    def path(self):
        return self._path

    def _remove_connection(self, conn_hash):
        """Remove a connection from the pool and close it."""
        if conn_hash in self.__conn_pool:
            try:
                ftp_host = self.__conn_pool[conn_hash]
                # Close all child connections first
                for child in ftp_host._children[:]:
                    try:
                        child.close()
                    except:
                        pass
                # Close the main connection
                ftp_host.close()
            except:
                pass
            del self.__conn_pool[conn_hash]
            if conn_hash in self.__conn_timestamps:
                del self.__conn_timestamps[conn_hash]
            if conn_hash in self.__last_noop_check:
                del self.__last_noop_check[conn_hash]
            if conn_hash in self.__conn_base_urls:
                del self.__conn_base_urls[conn_hash]

    def _cleanup_stale_connections(self):
        """Remove connections that have been idle for too long."""
        current_time = time.time()
        stale_hashes = []

        for conn_hash, timestamp in self.__conn_timestamps.items():
            if current_time - timestamp > self.__CONNECTION_TIMEOUT:
                stale_hashes.append(conn_hash)

        for conn_hash in stale_hashes:
            self._remove_connection(conn_hash)

        # Enforce max pool size (remove oldest connections)
        if len(self.__conn_pool) > self.__MAX_POOL_SIZE:
            sorted_conns = sorted(
                self.__conn_timestamps.items(),
                key=lambda x: x[1]
            )
            excess_count = len(self.__conn_pool) - self.__MAX_POOL_SIZE
            for conn_hash, _ in sorted_conns[:excess_count]:
                self._remove_connection(conn_hash)

    @classmethod
    def close_all_connections(cls):
        """Close all connections in the pool. Useful for cleanup."""
        with cls.__pool_lock:
            for conn_hash in list(cls.__conn_pool.keys()):
                try:
                    ftp_host = cls.__conn_pool[conn_hash]
                    # Close all child connections first
                    for child in ftp_host._children[:]:
                        try:
                            child.close()
                        except:
                            pass
                    # Close the main connection
                    ftp_host.close()
                except:
                    pass
            cls.__conn_pool.clear()
            cls.__conn_timestamps.clear()
            cls.__last_noop_check.clear()
            cls.__conn_base_urls.clear()
            cls.__last_visited_paths.clear()

    @classmethod
    def record_visited_path(cls, url):
        """Record the last visited path for a connection's base URL."""
        u = urlparse(url)
        url_without_path = u._replace(path='').geturl()

        # Resolve bookmark alias to actual URL (same as _get_bookmark)
        bookmarks = load_json('FTP Bookmarks.json', default={})
        if url_without_path in bookmarks:
            u = urlparse(bookmarks[url_without_path][0])._replace(path=u.path)

        # Build base URL matching _get_base_url format
        scheme = '%s://' % u.scheme
        host = u.hostname or ''
        port = u.port or 21
        user = unquote(u.username or '')

        if user:
            base_url = '%s%s@%s:%d' % (scheme, user, host, port)
        else:
            base_url = '%s%s:%d' % (scheme, host, port)

        with cls.__pool_lock:
            cls.__last_visited_paths[base_url] = url

    @classmethod
    def get_open_connections(cls):
        """Return list of (base_url, last_visited_url) for active connections."""
        with cls.__pool_lock:
            # Get all unique base URLs from active connections
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
            # Find all hashes that match this base_url
            hashes_to_remove = [
                h for h, url in cls.__conn_base_urls.items()
                if url == base_url
            ]
            for conn_hash in hashes_to_remove:
                if conn_hash in cls.__conn_pool:
                    try:
                        ftp_host = cls.__conn_pool[conn_hash]
                        # Close all child connections first
                        for child in ftp_host._children[:]:
                            try:
                                child.close()
                            except:
                                pass
                        # Close the main connection
                        ftp_host.close()
                    except:
                        pass
                    del cls.__conn_pool[conn_hash]
                    if conn_hash in cls.__conn_timestamps:
                        del cls.__conn_timestamps[conn_hash]
                    if conn_hash in cls.__last_noop_check:
                        del cls.__last_noop_check[conn_hash]
                    if conn_hash in cls.__conn_base_urls:
                        del cls.__conn_base_urls[conn_hash]
            # Also clear the last visited path for this connection
            if base_url in cls.__last_visited_paths:
                del cls.__last_visited_paths[base_url]
