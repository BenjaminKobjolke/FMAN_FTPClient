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
    __pool_lock = threading.Lock()
    # Connection timeout: close connections idle for more than 2 minutes
    __CONNECTION_TIMEOUT = 120
    # Max pool size: limit to 3 FTPHost objects per server
    # (each FTPHost can spawn multiple child connections)
    __MAX_POOL_SIZE = 3

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
                # Check if connection is still valid and not closed
                if not conn.closed:
                    try:
                        conn._session.voidcmd('NOOP')
                        # Update timestamp on successful reuse
                        self.__conn_timestamps[self.hash] = time.time()
                        return self
                    except:
                        pass
                # Connection is stale or closed, remove it from pool
                self._remove_connection(self.hash)

            session_factory = \
                FtpTlsSession if self._scheme == 'ftps://' else FtpSession
            ftp_host = ftputil.FTPHost(
                self._host, self._port, self._user, self._passwd,
                session_factory=session_factory)

            self.__conn_pool[self.hash] = ftp_host
            self.__conn_timestamps[self.hash] = time.time()
            return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        # try:
        #     self.__conn.quit()
        # except:
        #     pass
        # finally:
        #     self.__conn = None
        return

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
