"""Load FTP code with the fman API stub used by offline self-checks."""
import importlib.util
import os
import sys
import types
from types import ModuleType
from typing import BinaryIO, Callable, Optional, Type
from types import TracebackType


def load_ftp_module(test_directory: str) -> ModuleType:
    fake_fman = types.ModuleType('fman')
    fake_fman.__path__ = []
    fake_fman.load_json = lambda name, default=None, save_on_quit=False: \
        {} if default is None else default
    sys.modules['fman'] = fake_fman

    package = types.ModuleType('ftpclient')
    package.__path__ = [os.path.join(test_directory, 'ftpclient')]
    sys.modules['ftpclient'] = package
    spec = importlib.util.spec_from_file_location(
        'ftpclient.ftp', os.path.join(test_directory, 'ftpclient', 'ftp.py'))
    ftp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ftp)
    return ftp


def make_fman_url() -> ModuleType:
    """Build the URL module stub used by FTP self-checks."""
    url = types.ModuleType('fman.url')
    url.basename = lambda value: value.rsplit('/', 1)[-1]
    url.dirname = lambda value: value.rsplit('/', 1)[0]
    url.join = lambda *parts: '/'.join(parts)
    url.splitscheme = lambda value: (
        value[:value.index('://') + 3], value[value.index('://') + 3:])
    return url


class FakeWrapper:
    """Provide the shared FTP wrapper behavior used by offline tests."""

    def __init__(self, value: str, host: object) -> None:
        self.path = '/' + value.split('://', 1)[1].split('/', 1)[1]
        self.conn = host

    def __enter__(self) -> 'FakeWrapper':
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        return False

    def upload(
        self,
        src_file: BinaryIO,
        on_chunk: Optional[Callable[[bytes], None]] = None,
    ) -> None:
        kwargs = {'callback': on_chunk} if on_chunk is not None else {}
        self.conn._session.storbinary('STOR ' + self.path, src_file, **kwargs)

    def download(
        self,
        dst_file: BinaryIO,
        on_chunk: Callable[[bytes], None],
    ) -> None:
        def write_chunk(block: bytes) -> None:
            dst_file.write(block)
            on_chunk(block)

        self.conn._session.retrbinary('RETR ' + self.path, write_chunk)
