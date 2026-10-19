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

# Files fman transfers at once between FTP and this machine. Each worker holds
# its own connection, so servers that limit connections per IP may refuse more.
PARALLEL_TRANSFERS = 3


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
            list(self.prepare_copy(src_url, dst_url))))

    def prepare_copy(self, src_url, dst_url):
        self._check_supported(src_url, dst_url)
        return self._prepare_copy(src_url, dst_url)

    @staticmethod
    def _check_supported(src_url, dst_url):
        # Checked eagerly: fman only tries the other file system when
        # UnsupportedOperation is raised by prepare_copy/move itself. The walk
        # is a generator, so fman starts transferring while it still runs.
        if not all(is_ftp(url) or is_file(url) for url in (src_url, dst_url)):
            raise UnsupportedOperation

    def get_parallel_transfers(self, src_url, dst_url):
        # FTP to FTP stays sequential: each worker would hold a connection to
        # both servers.
        if bool(is_ftp(src_url)) != bool(is_ftp(dst_url)):
            return PARALLEL_TRANSFERS
        return 1

    def _prepare_copy(self, src_url, dst_url):
        if not fs.is_dir(src_url):
            yield self._prepare_file_copy(src_url, dst_url)
            return
        yield self._prepare_mkdir(dst_url)
        for fname in fs.iterdir(src_url):
            yield from self._prepare_copy(
                urljoin(src_url, fname), urljoin(dst_url, fname))

    @staticmethod
    def _prepare_mkdir(dst_url):
        result = Task(
            'Creating ' + basename(dst_url), fn=fs.makedirs, args=(dst_url,),
            kwargs={'exist_ok': True})
        # fman's parallel workers must not start the files that go into the
        # folder before it exists.
        result.blocks_later_tasks = True
        return result

    def _prepare_file_copy(self, src_url, dst_url):
        name = basename(src_url)
        dst_path = splitscheme(dst_url)[1]

        def written():
            self._notify_written(dst_path)

        if is_file(src_url):
            src_path = splitscheme(src_url)[1]
            ftp = FtpWrapper(dst_url)

            def upload(on_chunk):
                # ftputil opens a second login per file; hosts limit logins per IP.
                with ftp:
                    with open(src_path, 'rb') as src, \
                            ftputil.error.ftplib_error_to_ftp_os_error:
                        ftp.conn._session.storbinary(
                            'STOR ' + ftp.path, src, callback=on_chunk)

            return _Transfer(
                'Uploading', name, _local_size(src_path), upload, written)

        src_ftp = FtpWrapper(src_url)
        if is_file(dst_url):
            def download(on_chunk):
                # ftputil opens a second login per file; hosts limit logins per IP.
                with src_ftp:
                    with open(dst_path, 'wb') as dst, \
                            ftputil.error.ftplib_error_to_ftp_os_error:
                        def write_chunk(block):
                            dst.write(block)
                            on_chunk(block)

                        src_ftp.conn._session.retrbinary(
                            'RETR ' + src_ftp.path, write_chunk)

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
        submit_task(_Sequence(
            'Moving ' + basename(src_url),
            list(self.prepare_move(src_url, dst_url))))

    def prepare_move(self, src_url, dst_url):
        if self._is_rename(src_url, dst_url):
            # A rename is instant: fman's default task, which calls move(), is
            # all the progress it needs.
            return super().prepare_move(src_url, dst_url)
        self._check_supported(src_url, dst_url)
        return self._prepare_move(src_url, dst_url)

    def _prepare_move(self, src_url, dst_url):
        # File by file, so fman's workers can share a folder. Each source file
        # goes right after its own copy; a failed copy keeps it.
        name = basename(src_url)
        if not fs.is_dir(src_url):
            yield _Sequence('Moving ' + name, [
                self._prepare_file_copy(src_url, dst_url),
                Task('Deleting ' + name, fn=self._delete_source,
                     args=(src_url,))])
            return
        yield self._prepare_mkdir(dst_url)
        for fname in fs.iterdir(src_url):
            yield from self._prepare_move(
                urljoin(src_url, fname), urljoin(dst_url, fname))
        delete = Task(
            'Deleting ' + name, fn=self._delete_if_empty, args=(src_url,))
        # The folder is only empty once the moves out of it are done.
        delete.waits_for_earlier_tasks = True
        yield delete

    @staticmethod
    def _delete_source(src_url):
        if fs.exists(src_url):
            fs.delete(src_url)

    @classmethod
    def _delete_if_empty(cls, src_url):
        # Files that failed or were skipped keep their folder.
        if next(iter(fs.iterdir(src_url)), None) is None:
            cls._delete_source(src_url)

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
