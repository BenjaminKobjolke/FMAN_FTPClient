import errno
import re
import stat
from datetime import datetime
from io import UnsupportedOperation
from os.path import commonprefix, dirname, join as pathjoin
from tempfile import NamedTemporaryFile

import os

from fman import fs, load_json, show_status_message, Task, submit_task
from fman.fs import FileSystem, cached
from fman.url import basename, join as urljoin, splitscheme

from .ftp import FtpConnectionError, FtpWrapper, invalidate_stat_caches

try:
    import ftputil.error
except ImportError:
    import os
    import sys
    sys.path.append(
        os.path.join(os.path.dirname(__file__), 'ftputil-3.4'))
    import ftputil.error

is_ftp = re.compile('^ftps?://').match
is_file = re.compile('^file://').match


class _Transfer(Task):
    """
    One file going to or from an FTP server.

    The size is fixed up front: fman runs these as subtasks of its own copy or
    move task, and a subtask cannot change its size once it has started.
    """
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
                # min(): a file that grew since it was measured must not push
                # the shared progress bar past the end.
                self.set_progress(min(done, size))
                show_status_message('%s... %d%% (%d KB / %d KB)' % (
                    self._verb, done * 100 // size, done // 1024, size // 1024))

        self._transfer(on_chunk)
        show_status_message('Ready.', timeout_secs=0)
        self._on_done()


class _Sequence(Task):
    """
    Runs tasks in order under one progress dialog. A failure or a cancel stops
    the rest, which keeps a move from deleting a source it did not copy.
    """
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
        # Only costs the progress bar; the transfer reports the real error.
        return 0


def _local_size(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


class FtpFs(FileSystem):
    scheme = 'ftp://'

    def get_default_columns(self, path):
        settings = load_json('FTP Settings.json', default={})

        # Check if detailed stats are disabled
        if settings.get('disable_detailed_stats', False):
            # Only show filename for faster listings
            return ('core.Name',)
        else:
            # Show full file information
            return (
                'core.Name', 'core.Size', 'core.Modified',
                'ftpclient.columns.Permissions', 'ftpclient.columns.Owner',
                'ftpclient.columns.Group')

    def _notify_written(self, path):
        """
        Tell fman that `path` now exists (or has new contents), so the pane
        showing its directory updates by itself instead of waiting for Ctrl+R.

        We drop our own cache entry directly instead of firing
        notify_file_removed first: a removal event for a directory a pane is
        showing sends that pane to its parent (SortedModel#_on_file_removed).
        notify_file_changed is no use here either, its callbacks are only ever
        registered for a pane's own directory, never for single files.
        """
        invalidate_stat_caches()
        self.cache.clear(path)
        fs.notify_file_added(self.scheme + path)

    def _notify_removed(self, path):
        invalidate_stat_caches()
        fs.notify_file_removed(self.scheme + path)

    @cached
    def size_bytes(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            return ftp.conn.path.getsize(ftp.path)

    @cached
    def modified_datetime(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            return datetime.utcfromtimestamp(ftp.conn.path.getmtime(ftp.path))

    @cached
    def get_permissions(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            return stat.filemode(ftp.conn.lstat(ftp.path).st_mode)

    @cached
    def get_owner(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            return ftp.conn.lstat(ftp.path).st_uid

    @cached
    def get_group(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            return ftp.conn.lstat(ftp.path).st_gid

    @cached
    def exists(self, path):
        # XXX avoid errors on URLs without connection details: fman probes
        # ancestor URLs up to the bare scheme root, which has no host.
        if not path:
            return False
        try:
            with FtpWrapper(self.scheme + path) as ftp:
                return ftp.conn.path.exists(ftp.path)
        except FtpConnectionError:
            # Connection/login problems are real errors: let them through so
            # fman shows the reason instead of a misleading "not found".
            raise
        except ftputil.error.FTPError:
            # The server answered, but the path is not there
            return False

    @cached
    def is_dir(self, path):
        # XXX avoid errors on URLs without connection details: fman probes
        # ancestor URLs up to the bare scheme root, which has no host.
        if not path:
            return False
        try:
            with FtpWrapper(self.scheme + path) as ftp:
                return ftp.conn.path.isdir(ftp.path)
        except FtpConnectionError:
            raise
        except ftputil.error.FTPError:
            # The server answered, but the path is not a directory
            return False

    def iterdir(self, path):
        # XXX avoid errors on URLs without connection details
        if not path:
            return
        show_status_message('Loading %s...' % (path,))
        with FtpWrapper(self.scheme + path) as ftp:
            # ftputil's listdir() automatically populates its internal
            # _lstat_cache with all file stats. No need to manually
            # pre-fetch stats - they're already cached!
            for name in ftp.conn.listdir(ftp.path):
                yield name
        show_status_message('Ready.', timeout_secs=0)

    def delete(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            if self.is_dir(path):
                ftp.conn.rmtree(ftp.path)
            else:
                ftp.conn.remove(ftp.path)
        self._notify_removed(path)

    def move_to_trash(self, path):
        # ENOSYS: Function not implemented
        raise OSError(errno.ENOSYS, "FTP has no Trash support")

    def mkdir(self, path):
        # fman's makedirs(exist_ok=True) relies on this; ftputil's makedirs()
        # would silently succeed, and the resulting notification would send a
        # pane sitting in `path` to its parent.
        if self.exists(path):
            raise FileExistsError(errno.EEXIST, "File exists", path)
        with FtpWrapper(self.scheme + path) as ftp:
            ftp.conn.makedirs(ftp.path)
        self._notify_written(path)

    def touch(self, path):
        if self.exists(path):
            raise OSError(errno.EEXIST, "File exists")
        with FtpWrapper(self.scheme + path) as ftp:
            with NamedTemporaryFile(delete=True) as tmp:
                ftp.conn.upload(tmp.name, ftp.path)
        self._notify_written(path)

    def samefile(self, path1, path2):
        return path1 == path2

    def copy(self, src_url, dst_url):
        submit_task(_Sequence(
            'Copying ' + basename(src_url),
            self.prepare_copy(src_url, dst_url)))

    def prepare_copy(self, src_url, dst_url):
        # Returns a list, not a generator: fman only tries the other file
        # system when UnsupportedOperation is raised by this call itself.
        if not all(is_ftp(url) or is_file(url) for url in (src_url, dst_url)):
            raise UnsupportedOperation
        if not fs.is_dir(src_url):
            return [self._prepare_file_copy(src_url, dst_url)]
        tasks = [Task(
            'Creating ' + basename(dst_url), fn=fs.makedirs, args=(dst_url,),
            kwargs={'exist_ok': True})]
        for fname in fs.iterdir(src_url):
            tasks += self.prepare_copy(
                urljoin(src_url, fname), urljoin(dst_url, fname))
        return tasks

    def _prepare_file_copy(self, src_url, dst_url):
        name = basename(src_url)
        dst_path = splitscheme(dst_url)[1]

        def written():
            self._notify_written(dst_path)

        if is_file(src_url):
            src_path = splitscheme(src_url)[1]
            ftp = FtpWrapper(dst_url)

            def upload(on_chunk):
                with ftp:
                    ftp.conn.upload(src_path, ftp.path, callback=on_chunk)

            return _Transfer(
                'Uploading', name, _local_size(src_path), upload, written)

        src_ftp = FtpWrapper(src_url)
        if is_file(dst_url):
            def download(on_chunk):
                with src_ftp:
                    src_ftp.conn.download(
                        src_ftp.path, dst_path, callback=on_chunk)

            # Local destination: nothing of ours to invalidate.
            return _Transfer(
                'Downloading', name, _remote_size(src_ftp), download,
                lambda: fs.notify_file_added(dst_url))

        dst_ftp = FtpWrapper(dst_url)

        def ftp_to_ftp(on_chunk):
            with src_ftp, dst_ftp:
                with src_ftp.conn.open(src_ftp.path, 'rb') as src, \
                        dst_ftp.conn.open(dst_ftp.path, 'wb') as dst:
                    dst_ftp.conn.copyfileobj(src, dst, callback=on_chunk)

        return _Transfer(
            'Copying', name, _remote_size(src_ftp), ftp_to_ftp, written)

    @staticmethod
    def _is_rename(src_url, dst_url):
        src_scheme, src_path = splitscheme(src_url)
        dst_scheme, dst_path = splitscheme(dst_url)
        return src_scheme == dst_scheme and commonprefix([src_path, dst_path])

    def move(self, src_url, dst_url):
        if self._is_rename(src_url, dst_url):
            # Use single connection for same-server renames
            with FtpWrapper(src_url) as ftp:
                ftp.conn.rename(ftp.path, FtpWrapper(dst_url).path)
            self._notify_removed(splitscheme(src_url)[1])
            self._notify_written(splitscheme(dst_url)[1])
            return
        for task in self.prepare_move(src_url, dst_url):
            submit_task(task)

    def prepare_move(self, src_url, dst_url):
        if self._is_rename(src_url, dst_url):
            # A rename is instant: fman's default task, which calls move(), is
            # all the progress it needs.
            return super().prepare_move(src_url, dst_url)
        name = basename(src_url)
        # The delete goes last in the same sequence, so a canceled or failed
        # copy never reaches it.
        return [_Sequence('Moving ' + name, [
            *self.prepare_copy(src_url, dst_url),
            Task('Deleting ' + name, fn=self._delete_source, args=(src_url,))
        ])]

    @staticmethod
    def _delete_source(src_url):
        if fs.exists(src_url):
            fs.delete(src_url)

    def get_stats(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            lstat = ftp.conn.lstat(ftp.path)
            dt_mtime = datetime.utcfromtimestamp(lstat.st_mtime)
            st_mode = stat.filemode(lstat.st_mode)
            self.cache.put(path, 'size_bytes', lstat.st_size)
            self.cache.put(path, 'modified_datetime', dt_mtime)
            self.cache.put(path, 'get_permissions', st_mode)
            self.cache.put(path, 'get_owner', lstat.st_uid)
            self.cache.put(path, 'get_group', lstat.st_gid)


class FtpsFs(FtpFs):
    scheme = 'ftps://'
