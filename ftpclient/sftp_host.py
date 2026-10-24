"""Paramiko-backed file host with the subset of ftputil used by this plugin."""
from __future__ import annotations

import os
import posixpath
import stat
from typing import Any, BinaryIO, Callable

import paramiko


class _StatCache:
    """Small per-host cache compatible with the ftputil cache calls we use."""
    def __init__(self, host: SftpHost) -> None:
        self._host = host
        self._values = {}

    def get(self, key: object) -> object | None:
        return self._values.get(key)

    def put(self, key: object, value: object) -> None:
        self._values[key] = value

    def clear(self) -> None:
        self._values.clear()
        self._host._stats.clear()
        self._host._listings.clear()


class _Path:
    def __init__(self, host: SftpHost) -> None:
        self._host = host

    def getsize(self, path: str) -> int:
        return self._host.stat(path).st_size

    def getmtime(self, path: str) -> float:
        return self._host.stat(path).st_mtime

    def exists(self, path: str) -> bool:
        try:
            self._host.stat(path)
            return True
        except OSError:
            return False

    def isdir(self, path: str) -> bool:
        try:
            return stat.S_ISDIR(self._host.stat(path).st_mode)
        except OSError:
            return False


class _TrackedFile:
    def __init__(
        self, stream: BinaryIO, host: SftpHost, path: str, writable: bool,
    ) -> None:
        self._stream = stream
        self._host = host
        self._path = path
        self._writable = writable

    def __getattr__(self, name: str) -> Any:
        return getattr(self._stream, name)

    def __enter__(self) -> _TrackedFile:
        self._stream.__enter__()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None,
        exc_value: BaseException | None, traceback: Any,
    ) -> bool | None:
        try:
            return self._stream.__exit__(exc_type, exc_value, traceback)
        finally:
            if self._writable:
                self._host._invalidate(self._path)

    def close(self) -> None:
        try:
            return self._stream.close()
        finally:
            if self._writable:
                self._host._invalidate(self._path)


class SftpHost:
    """Expose a Paramiko SFTP client through the FTPHost operations in use."""
    def __init__(self, ssh_client: paramiko.SSHClient) -> None:
        self._ssh_client = ssh_client
        self._session = ssh_client.open_sftp()
        self._children = []
        self._closed = False
        self._stats = {}
        self._listings = {}
        self.stat_cache = _StatCache(self)
        self.path = _Path(self)

    @property
    def closed(self) -> bool:
        return self._closed or self._session.get_channel() is None or \
            self._session.get_channel().closed

    def _normalize(self, path: str) -> str:
        return posixpath.normpath('/' + path.lstrip('/'))

    def _invalidate(self, path: str) -> None:
        path = self._normalize(path)
        parent = posixpath.dirname(path)
        self._stats.pop(path, None)
        self._listings.pop(parent, None)

    @staticmethod
    def _to_stat(attributes: paramiko.SFTPAttributes) -> os.stat_result:
        mode = getattr(attributes, 'st_mode', None)
        if mode is None:
            mode = stat.S_IFREG | 0o644
        return os.stat_result((
            mode, getattr(attributes, 'st_ino', 0) or 0,
            getattr(attributes, 'st_dev', 0) or 0,
            getattr(attributes, 'st_nlink', 1) or 1,
            getattr(attributes, 'st_uid', 0) or 0,
            getattr(attributes, 'st_gid', 0) or 0,
            getattr(attributes, 'st_size', 0) or 0,
            getattr(attributes, 'st_atime', 0) or 0,
            getattr(attributes, 'st_mtime', 0) or 0,
            getattr(attributes, 'st_ctime', 0) or 0))

    def stat(self, path: str) -> os.stat_result:
        path = self._normalize(path)
        if path not in self._stats:
            self._stats[path] = self._to_stat(self._session.stat(path))
        return self._stats[path]

    def lstat(self, path: str) -> os.stat_result:
        path = self._normalize(path)
        key = ('lstat', path)
        if key not in self._stats:
            self._stats[key] = self._to_stat(self._session.lstat(path))
        return self._stats[key]

    def listdir(self, path: str) -> list[str]:
        path = self._normalize(path)
        if path not in self._listings:
            attributes = self._session.listdir_attr(path)
            self._listings[path] = [entry.filename for entry in attributes]
            for entry in attributes:
                self._stats[posixpath.join(path, entry.filename)] = \
                    self._to_stat(entry)
        return list(self._listings[path])

    def open(self, path: str, mode: str = 'r') -> BinaryIO:
        path = self._normalize(path)
        writable = any(flag in mode for flag in 'wax+')
        return _TrackedFile(self._session.open(path, mode), self, path, writable)

    def copyfileobj(
        self, source: BinaryIO, destination: BinaryIO,
        callback: Callable[[bytes], None] | None = None,
    ) -> None:
        while True:
            chunk = source.read(32768)
            if not chunk:
                return
            destination.write(chunk)
            if callback is not None:
                callback(chunk)

    def remove(self, path: str) -> None:
        path = self._normalize(path)
        self._session.remove(path)
        self._invalidate(path)

    def rename(self, source: str, destination: str) -> None:
        source = self._normalize(source)
        destination = self._normalize(destination)
        self._session.rename(source, destination)
        self._invalidate(source)
        self._invalidate(destination)

    def makedirs(self, path: str) -> None:
        path = self._normalize(path)
        if path == '/':
            return
        parent = posixpath.dirname(path)
        if parent != path and not self.path.isdir(parent):
            self.makedirs(parent)
        try:
            self._session.mkdir(path)
        except OSError:
            if not self.path.isdir(path):
                raise
        self._invalidate(path)

    def rmtree(self, path: str) -> None:
        path = self._normalize(path)
        for name in self.listdir(path):
            child = posixpath.join(path, name)
            if stat.S_ISDIR(self.lstat(child).st_mode):
                self.rmtree(child)
            else:
                self.remove(child)
        self._session.rmdir(path)
        self._invalidate(path)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._session.close()
        finally:
            self._ssh_client.close()
