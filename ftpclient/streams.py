"""Binary streams that keep a pooled FTP connection alive while in use."""
import errno
import io
import sys
from types import TracebackType
from typing import BinaryIO, Callable
from urllib.parse import urlsplit

from .connections import connection_for
from .ftp import FtpWrapper
from .ftp_session import ftputil

READ_MODE = 'rb'
WRITE_MODE = 'wb'


def open_remote(
    url: str,
    mode: str,
    on_written: Callable[[str], None],
    on_removed: Callable[[str], None],
) -> 'RemoteStream':
    """Open one remote file as a binary stream."""
    if mode not in (READ_MODE, WRITE_MODE):
        raise ValueError('Unsupported mode: %s' % mode)

    # The pool reaps idle connections, and its in-use marker belongs to this thread.
    wrapper = connection_for(url)
    wrapper.__enter__()
    try:
        if wrapper.conn.path.isdir(wrapper.path):
            raise IsADirectoryError(
                errno.EISDIR, 'Is a directory', wrapper.path)
        if mode == READ_MODE and not wrapper.conn.path.exists(wrapper.path):
            raise FileNotFoundError(errno.ENOENT, 'No such file', wrapper.path)
        stream = wrapper.conn.open(wrapper.path, mode)
        parsed_url = urlsplit(url)
        return RemoteStream(
            wrapper, stream, mode, on_written, on_removed,
            parsed_url.netloc + parsed_url.path, url)
    except BaseException:
        wrapper.__exit__(*sys.exc_info())
        raise


class RemoteStream:
    """A remote binary stream that owns its pooled connection until close."""

    def __init__(
        self,
        wrapper: FtpWrapper,
        stream: BinaryIO,
        mode: str,
        on_written: Callable[[str], None],
        on_removed: Callable[[str], None],
        path: str,
        url: str,
    ) -> None:
        self._wrapper = wrapper
        self._stream = stream
        self._mode = mode
        self._on_written = on_written
        self._on_removed = on_removed
        self._path = path
        self._url = url
        self._closed = False

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def write(self, data: bytes) -> int:
        self._stream.write(data)
        return len(data)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._close_inner()
        finally:
            try:
                self._wrapper.__exit__(None, None, None)
            finally:
                if self._mode == WRITE_MODE:
                    self._on_written(self._path)

    def __enter__(self) -> 'RemoteStream':
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if exc_type is None:
            self.close()
        elif self._mode == WRITE_MODE and not self._closed:
            self._closed = True
            try:
                self._close_inner()
            except Exception:
                pass
            self._wrapper.__exit__(exc_type, exc_value, traceback)
            try:
                with connection_for(self._url) as ftp:
                    ftp.conn.remove(ftp.path)
            except (OSError, ftputil.error.FTPError):
                # fman++ retries deleting a failed destination; remove partial
                # data now too.
                pass
            self._on_removed(self._path)
        else:
            self.close()
        return False

    def _close_inner(self) -> None:
        if self._mode == WRITE_MODE and urlsplit(self._url).scheme == 'ftps':
            # ftputil's FTPFile.close closes TLS sockets without unwrap(); FTPS
            # servers wait for TLS shutdown before returning the STOR response.
            self._stream._fobj.close()
            self._stream._fobj = io.BytesIO()
            self._stream._conn = self._stream._conn.unwrap()
        self._stream.close()
