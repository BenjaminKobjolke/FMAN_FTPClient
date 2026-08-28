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
from fman.url import join as urljoin, splitscheme

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
        # Recursive copy
        if fs.is_dir(src_url):
            fs.makedirs(dst_url, exist_ok=True)
            for fname in fs.iterdir(src_url):
                fs.copy(urljoin(src_url, fname), urljoin(dst_url, fname))
            return

        if is_ftp(src_url) and is_ftp(dst_url):
            # FTP to FTP copy
            src_ftp_wrapper = FtpWrapper(src_url)
            dst_ftp_wrapper = FtpWrapper(dst_url)
            filename = src_ftp_wrapper.path.split('/')[-1]

            class FtpToFtpCopyTask(Task):
                def __init__(task_self):
                    super().__init__(f'Copying {filename}...')

                def __call__(task_self):
                    with src_ftp_wrapper as src_ftp, dst_ftp_wrapper as dst_ftp:
                        # Get file size for progress
                        try:
                            file_size = src_ftp.conn.path.getsize(src_ftp.path)
                        except:
                            file_size = 0

                        if file_size:
                            task_self.set_size(file_size)

                        transferred = [0]
                        def progress_callback(chunk):
                            task_self.check_canceled()
                            transferred[0] += len(chunk)
                            if file_size:
                                task_self.set_progress(transferred[0])
                                percent = int((transferred[0] / file_size) * 100)
                                show_status_message(f'Copying... {percent}% ({transferred[0] // 1024} KB / {file_size // 1024} KB)')

                        with src_ftp.conn.open(src_ftp.path, 'rb') as src, \
                                dst_ftp.conn.open(dst_ftp.path, 'wb') as dst:
                            dst_ftp.conn.copyfileobj(src, dst, callback=progress_callback)
                        show_status_message('Ready.', timeout_secs=0)
                    self._notify_written(splitscheme(dst_url)[1])

            task = FtpToFtpCopyTask()
            submit_task(task)

        elif is_ftp(src_url) and is_file(dst_url):
            # FTP download
            _, dst_path = splitscheme(dst_url)
            ftp_wrapper = FtpWrapper(src_url)
            filename = ftp_wrapper.path.split('/')[-1]

            class FtpDownloadTask(Task):
                def __init__(task_self):
                    super().__init__(f'Downloading {filename}...')

                def __call__(task_self):
                    with ftp_wrapper as ftp:
                        # Get file size for progress
                        try:
                            file_size = ftp.conn.path.getsize(ftp.path)
                        except:
                            file_size = 0

                        if file_size:
                            task_self.set_size(file_size)

                        transferred = [0]
                        def progress_callback(chunk):
                            task_self.check_canceled()
                            transferred[0] += len(chunk)
                            if file_size:
                                task_self.set_progress(transferred[0])
                                percent = int((transferred[0] / file_size) * 100)
                                show_status_message(f'Downloading... {percent}% ({transferred[0] // 1024} KB / {file_size // 1024} KB)')

                        ftp.conn.download(ftp.path, dst_path, callback=progress_callback)
                        show_status_message('Ready.', timeout_secs=0)
                    # Local destination: nothing of ours to invalidate.
                    fs.notify_file_added(dst_url)

            task = FtpDownloadTask()
            submit_task(task)

        elif is_file(src_url) and is_ftp(dst_url):
            # FTP upload
            _, src_path = splitscheme(src_url)
            ftp_wrapper = FtpWrapper(dst_url)
            filename = os.path.basename(src_path)

            class FtpUploadTask(Task):
                def __init__(task_self):
                    super().__init__(f'Uploading {filename}...')

                def __call__(task_self):
                    with ftp_wrapper as ftp:
                        # Get local file size for progress
                        try:
                            file_size = os.path.getsize(src_path)
                        except:
                            file_size = 0

                        if file_size:
                            task_self.set_size(file_size)

                        transferred = [0]
                        def progress_callback(chunk):
                            task_self.check_canceled()
                            transferred[0] += len(chunk)
                            if file_size:
                                task_self.set_progress(transferred[0])
                                percent = int((transferred[0] / file_size) * 100)
                                show_status_message(f'Uploading... {percent}% ({transferred[0] // 1024} KB / {file_size // 1024} KB)')

                        ftp.conn.upload(src_path, ftp.path, callback=progress_callback)
                        show_status_message('Ready.', timeout_secs=0)
                    self._notify_written(splitscheme(dst_url)[1])

            task = FtpUploadTask()
            submit_task(task)
        else:
            raise UnsupportedOperation

    def move(self, src_url, dst_url):
        # Rename on same server
        src_scheme, src_path = splitscheme(src_url)
        dst_scheme, dst_path = splitscheme(dst_url)
        if src_scheme == dst_scheme and commonprefix([src_path, dst_path]):
            # Use single connection for same-server renames
            with FtpWrapper(src_url) as ftp:
                # Get destination path from dst_url
                dst_ftp = FtpWrapper(dst_url)
                ftp.conn.rename(ftp.path, dst_ftp.path)
            self._notify_removed(src_path)
            self._notify_written(dst_path)
            return

        fs.copy(src_url, dst_url)
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
