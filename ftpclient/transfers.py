"""Copy and move tasks between FTP servers and this machine."""
import os
from io import UnsupportedOperation
from typing import Callable

from fman import Task, fs, show_status_message
from fman.url import basename, join as urljoin, splitscheme

from .connections import connection_for, is_file, is_ftp
from .ftp_session import ftputil

# Each worker holds a connection, and some servers limit them per client IP.
PARALLEL_TRANSFERS = 3


class _Transfer(Task):
    """One file transfer with progress based on its known size."""
    def __init__(self, verb, name, size, transfer, on_done):
        super().__init__('%s %s' % (verb, name), size=size)
        self._verb = verb
        self._transfer = transfer
        self._on_done = on_done

    def __call__(self):
        size = self.get_size()
        done = 0

        def on_chunk(chunk):
            nonlocal done
            self.check_canceled()
            done += len(chunk)
            if size:
                # A file that grew since measurement must not overrun progress.
                self.set_progress(min(done, size))
                show_status_message('%s... %d%% (%d KB / %d KB)' % (
                    self._verb, done * 100 // size, done // 1024, size // 1024))

        self._transfer(on_chunk)
        show_status_message('Ready.', timeout_secs=0)
        self._on_done()


class TransferSequence(Task):
    """Run transfer tasks in order and stop after a failure or cancellation."""
    def __init__(self, title, tasks):
        super().__init__(title, size=sum(task.get_size() for task in tasks))
        self._tasks = tasks

    def __call__(self):
        for task in self._tasks:
            self.check_canceled()
            self.run(task)


def _remote_size(ftp_wrapper):
    try:
        with ftp_wrapper as ftp:
            return ftp.conn.path.getsize(ftp.path)
    except (OSError, ftputil.error.FTPError):
        return 0


def _local_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def check_supported(src_url, dst_url):
    """Reject copy pairs this file system does not own, before iteration."""
    if not all(is_ftp(url) or is_file(url) for url in (src_url, dst_url)):
        raise UnsupportedOperation


def parallel_transfers(src_url, dst_url) -> int:
    """Return the worker count while avoiding two server connections each."""
    if bool(is_ftp(src_url)) != bool(is_ftp(dst_url)):
        return PARALLEL_TRANSFERS
    return 1


def prepare_copy(src_url, dst_url, on_written: Callable[[str], None]):
    check_supported(src_url, dst_url)
    return _prepare_copy(src_url, dst_url, on_written)


def _prepare_copy(src_url, dst_url, on_written):
    if not fs.is_dir(src_url):
        yield prepare_file_copy(src_url, dst_url, on_written)
        return
    yield _prepare_mkdir(dst_url)
    for fname in fs.iterdir(src_url):
        yield from _prepare_copy(
            urljoin(src_url, fname), urljoin(dst_url, fname), on_written)


def _prepare_mkdir(dst_url):
    result = Task(
        'Creating ' + basename(dst_url), fn=fs.makedirs, args=(dst_url,),
        kwargs={'exist_ok': True})
    # Parallel workers must not start files in the folder before it exists.
    result.blocks_later_tasks = True
    return result


def prepare_file_copy(src_url, dst_url, on_written):
    name = basename(src_url)
    dst_path = splitscheme(dst_url)[1]

    def written():
        on_written(dst_path)

    if is_file(src_url):
        src_path = splitscheme(src_url)[1]
        ftp = connection_for(dst_url)

        def upload(on_chunk):
            with ftp:
                with open(src_path, 'rb') as src:
                    ftp.upload(src, on_chunk=on_chunk)

        return _Transfer('Uploading', name, _local_size(src_path), upload, written)

    src_ftp = connection_for(src_url)
    if is_file(dst_url):
        def download(on_chunk):
            with src_ftp:
                with open(dst_path, 'wb') as dst:
                    src_ftp.download(dst, on_chunk)

        return _Transfer(
            'Downloading', name, _remote_size(src_ftp), download,
            lambda: fs.notify_file_added(dst_url))

    dst_ftp = connection_for(dst_url)

    def ftp_to_ftp(on_chunk):
        with src_ftp, dst_ftp:
            with src_ftp.conn.open(src_ftp.path, 'rb') as src, \
                    dst_ftp.conn.open(dst_ftp.path, 'wb') as dst:
                dst_ftp.conn.copyfileobj(src, dst, callback=on_chunk)

    return _Transfer('Copying', name, _remote_size(src_ftp), ftp_to_ftp, written)


def prepare_move(src_url, dst_url, on_written: Callable[[str], None]):
    check_supported(src_url, dst_url)
    return _prepare_move(src_url, dst_url, on_written)


def _prepare_move(src_url, dst_url, on_written):
    name = basename(src_url)
    if not fs.is_dir(src_url):
        # A failed copy must leave its source in place.
        yield TransferSequence('Moving ' + name, [
            prepare_file_copy(src_url, dst_url, on_written),
            Task('Deleting ' + name, fn=_delete_source, args=(src_url,))])
        return
    yield _prepare_mkdir(dst_url)
    for fname in fs.iterdir(src_url):
        yield from _prepare_move(
            urljoin(src_url, fname), urljoin(dst_url, fname), on_written)
    delete = Task(
        'Deleting ' + name, fn=_delete_if_empty, args=(src_url,))
    # The folder is only empty once the moves out of it are done.
    delete.waits_for_earlier_tasks = True
    yield delete


def _delete_source(src_url):
    if fs.exists(src_url):
        fs.delete(src_url)


def _delete_if_empty(src_url):
    # Files that failed or were skipped keep their folder.
    if next(iter(fs.iterdir(src_url)), None) is None:
        _delete_source(src_url)
