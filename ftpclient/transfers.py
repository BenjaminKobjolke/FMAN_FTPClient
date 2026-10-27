"""Copy and move tasks between FTP servers and this machine."""
import os
import threading
from io import UnsupportedOperation
from typing import Callable

from fman import Task, fs, show_status_message, submit_task
from fman.url import basename, dirname, join as urljoin, splitscheme

from .connections import connection_for, is_file, is_ftp
from .ftp_session import ftputil
from .transfer_retry import (
    TRANSIENT_ERRORS, TrackedUnit, current_batch, transfer_retries,
    with_retry_check,
)

# Each worker holds a connection, and some servers limit them per client IP.
PARALLEL_TRANSFERS = 3


class _Folders:
    """Create each destination folder once across a transfer's workers."""
    def __init__(self):
        self._locks_lock = threading.Lock()
        self._locks = {}
        self._done = set()

    def ensure(self, url):
        with self._locks_lock:
            lock = self._locks.setdefault(url, threading.Lock())
        with lock:
            if url in self._done:
                return
            fs.makedirs(url, exist_ok=True)
            self._done.add(url)


class _Transfer(Task):
    """One file transfer with progress based on its known size."""
    def __init__(self, verb, name, size, transfer, on_done, connections):
        super().__init__('%s %s' % (verb, name), size=size)
        self._verb = verb
        self._name = name
        self._transfer = transfer
        self._on_done = on_done
        self._connections = connections

    def __call__(self):
        retries = transfer_retries()
        for attempt in range(1, retries + 2):
            try:
                self._attempt()
            except TRANSIENT_ERRORS:
                if attempt > retries:
                    raise
                self._prepare_retry(attempt, retries)
            else:
                break
        show_status_message('Ready.', timeout_secs=0)
        self._on_done()

    def _attempt(self):
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

    def _prepare_retry(self, attempt, retries):
        self.check_canceled()
        self.set_progress(0)
        show_status_message(
            'Retrying %s (%d/%d)...' % (self._name, attempt, retries))
        # The pool hands a dead session back for a few seconds after its
        # last check, so the retry would fail on it again.
        for connection in self._connections:
            connection.drop_connection()



class TransferSequence(Task):
    """Run transfer tasks in order and stop after a failure or cancellation."""
    minimizable = True
    icon = os.path.join(os.path.dirname(__file__), 'icons', 'ftp.svg')

    def __init__(self, title, tasks):
        super().__init__(title, size=sum(task.get_size() for task in tasks))
        self._tasks = tasks

    def __call__(self):
        for task in self._tasks:
            self.check_canceled()
            self.run(task)


class EditUpload(TransferSequence):
    """Upload of a file being edited from the server."""
    # The edit alert is modal; only a modal window opens in front of it.
    keeps_focus = True


def upload_edited_file(src_url: str, dst_url: str) -> None:
    submit_task(EditUpload(
        'Uploading ' + basename(src_url), list(fs.prepare_copy(src_url, dst_url))))


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
    batch = current_batch()
    folders = _Folders()
    return with_retry_check(
        batch, _prepare_copy(src_url, dst_url, on_written, batch, folders))


def _prepare_copy(src_url, dst_url, on_written, batch, folders):
    if not fs.is_dir(src_url):
        yield TrackedUnit(batch, (src_url, dst_url), lambda: prepare_file_copy(
            src_url, dst_url, on_written,
            make_parent=lambda: folders.ensure(dirname(dst_url))))
        return
    yield _prepare_mkdir(dst_url, folders)
    for fname in fs.iterdir(src_url):
        yield from _prepare_copy(
            urljoin(src_url, fname), urljoin(dst_url, fname), on_written,
            batch, folders)


def _prepare_mkdir(dst_url, folders):
    result = Task(
        'Creating ' + basename(dst_url), fn=folders.ensure, args=(dst_url,))
    return result


def prepare_file_copy(src_url, dst_url, on_written, make_parent=lambda: None):
    name = basename(src_url)
    dst_path = splitscheme(dst_url)[1]

    def written():
        on_written(dst_path)

    if is_file(src_url):
        src_path = splitscheme(src_url)[1]
        ftp = connection_for(dst_url)

        def upload(on_chunk):
            make_parent()
            with ftp:
                with open(src_path, 'rb') as src:
                    ftp.upload(src, on_chunk=on_chunk)

        return _Transfer(
            'Uploading', name, _local_size(src_path), upload, written, (ftp,))

    src_ftp = connection_for(src_url)
    if is_file(dst_url):
        def download(on_chunk):
            make_parent()
            with src_ftp:
                with open(dst_path, 'wb') as dst:
                    src_ftp.download(dst, on_chunk)

        return _Transfer(
            'Downloading', name, _remote_size(src_ftp), download,
            lambda: fs.notify_file_added(dst_url), (src_ftp,))

    dst_ftp = connection_for(dst_url)

    def ftp_to_ftp(on_chunk):
        make_parent()
        with src_ftp, dst_ftp:
            with src_ftp.conn.open(src_ftp.path, 'rb') as src, \
                    dst_ftp.conn.open(dst_ftp.path, 'wb') as dst:
                dst_ftp.conn.copyfileobj(src, dst, callback=on_chunk)

    return _Transfer(
        'Copying', name, _remote_size(src_ftp), ftp_to_ftp, written,
        (src_ftp, dst_ftp))


def prepare_move(src_url, dst_url, on_written: Callable[[str], None]):
    check_supported(src_url, dst_url)
    batch = current_batch()
    folders = _Folders()
    return with_retry_check(
        batch, _prepare_move(src_url, dst_url, on_written, batch, folders))


def _prepare_file_move(src_url, dst_url, on_written, folders):
    name = basename(src_url)
    # A failed copy must leave its source in place.
    return TransferSequence('Moving ' + name, [
        prepare_file_copy(
            src_url, dst_url, on_written,
            make_parent=lambda: folders.ensure(dirname(dst_url))),
        Task('Deleting ' + name, fn=_delete_source, args=(src_url,))])


def _prepare_move(src_url, dst_url, on_written, batch, folders):
    name = basename(src_url)
    if not fs.is_dir(src_url):
        yield TrackedUnit(batch, (src_url, dst_url), lambda: _prepare_file_move(
            src_url, dst_url, on_written, folders))
        return
    yield _prepare_mkdir(dst_url, folders)
    for fname in fs.iterdir(src_url):
        yield from _prepare_move(
            urljoin(src_url, fname), urljoin(dst_url, fname), on_written,
            batch, folders)
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
