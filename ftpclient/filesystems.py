import errno
import io
import stat
from datetime import datetime
from urllib.parse import unquote, urlparse

from fman import fs, load_json, show_status_message, submit_task
from fman.fs import FileSystem, cached
from fman.url import \
    basename, dirname as urldirname, splitscheme

from .connections import connection_for
from .ftp import invalidate_stat_caches
from .ftp_session import DEFAULT_PORTS, FtpConnectionError, ftputil
from . import transfers


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
        notify_file_changed is not useful for a single file, but _reload_panes
        uses it for the parent directory so panes showing that directory reload.
        """
        invalidate_stat_caches()
        self.cache.clear(path)
        fs.notify_file_added(self.scheme + path)

    def _reload_panes(self, *paths):
        # Same as Ctrl+R in every pane showing the folder of each path.
        # ponytail: one LIST per call, so deleting or moving N files lists the
        # folder N times; coalesce per folder if that ever gets slow.
        for folder in {urldirname(self.scheme + path) for path in paths}:
            fs.notify_file_changed(folder)

    def _notify_removed(self, path):
        invalidate_stat_caches()
        fs.notify_file_removed(self.scheme + path)

    @cached
    def size_bytes(self, path):
        with connection_for(self.scheme + path) as ftp:
            return ftp.conn.path.getsize(ftp.path)

    @cached
    def modified_datetime(self, path):
        with connection_for(self.scheme + path) as ftp:
            return datetime.utcfromtimestamp(ftp.conn.path.getmtime(ftp.path))

    @cached
    def get_permissions(self, path):
        with connection_for(self.scheme + path) as ftp:
            return stat.filemode(ftp.conn.lstat(ftp.path).st_mode)

    @cached
    def get_owner(self, path):
        with connection_for(self.scheme + path) as ftp:
            return ftp.conn.lstat(ftp.path).st_uid

    @cached
    def get_group(self, path):
        with connection_for(self.scheme + path) as ftp:
            return ftp.conn.lstat(ftp.path).st_gid

    @cached
    def exists(self, path):
        # XXX avoid errors on URLs without connection details: fman probes
        # ancestor URLs up to the bare scheme root, which has no host.
        if not path:
            return False
        try:
            with connection_for(self.scheme + path) as ftp:
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
            with connection_for(self.scheme + path) as ftp:
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
        with connection_for(self.scheme + path) as ftp:
            # ftputil's listdir() automatically populates its internal
            # _lstat_cache with all file stats. No need to manually
            # pre-fetch stats - they're already cached!
            for name in ftp.conn.listdir(ftp.path):
                yield name
        show_status_message('Ready.', timeout_secs=0)

    def delete(self, path):
        with connection_for(self.scheme + path) as ftp:
            if self.is_dir(path):
                ftp.conn.rmtree(ftp.path)
            else:
                ftp.conn.remove(ftp.path)
        self._notify_removed(path)
        self._reload_panes(path)

    def move_to_trash(self, path):
        # FTP has no trash, so deleting is the closest thing.
        self.delete(path)

    def mkdir(self, path):
        # fman's makedirs(exist_ok=True) relies on this; ftputil's makedirs()
        # would silently succeed, and the resulting notification would send a
        # pane sitting in `path` to its parent.
        if self.exists(path):
            raise FileExistsError(errno.EEXIST, "File exists", path)
        with connection_for(self.scheme + path) as ftp:
            ftp.conn.makedirs(ftp.path)
        self._notify_written(path)
        self._reload_panes(path)

    def touch(self, path):
        if self.exists(path):
            raise OSError(errno.EEXIST, "File exists")
        with connection_for(self.scheme + path) as ftp:
            ftp.upload(io.BytesIO())
        self._notify_written(path)
        self._reload_panes(path)

    def samefile(self, path1, path2):
        return path1 == path2

    def copy(self, src_url, dst_url):
        submit_task(transfers.TransferSequence(
            'Copying ' + basename(src_url),
            list(self.prepare_copy(src_url, dst_url))))

    def prepare_copy(self, src_url, dst_url):
        return transfers.prepare_copy(src_url, dst_url, self._notify_written)

    def get_parallel_transfers(self, src_url, dst_url):
        return transfers.parallel_transfers(src_url, dst_url)

    @staticmethod
    def _is_rename(src_url, dst_url):
        src = urlparse(src_url)
        dst = urlparse(dst_url)
        src_identity = (
            src.scheme, src.hostname, src.port or DEFAULT_PORTS.get(src.scheme),
            unquote(src.username or ''), unquote(src.password or ''))
        dst_identity = (
            dst.scheme, dst.hostname, dst.port or DEFAULT_PORTS.get(dst.scheme),
            unquote(dst.username or ''), unquote(dst.password or ''))
        return src_identity == dst_identity and src.path and dst.path

    def move(self, src_url, dst_url):
        if self._is_rename(src_url, dst_url):
            # Use single connection for same-server renames
            with connection_for(src_url) as ftp:
                ftp.conn.rename(ftp.path, connection_for(dst_url).path)
            self._notify_removed(splitscheme(src_url)[1])
            self._notify_written(splitscheme(dst_url)[1])
            self._reload_panes(
                splitscheme(src_url)[1], splitscheme(dst_url)[1])
            return
        submit_task(transfers.TransferSequence(
            'Moving ' + basename(src_url),
            list(self.prepare_move(src_url, dst_url))))

    def prepare_move(self, src_url, dst_url):
        if self._is_rename(src_url, dst_url):
            # A rename is instant: fman's default task, which calls move(), is
            # all the progress it needs.
            return super().prepare_move(src_url, dst_url)
        return transfers.prepare_move(src_url, dst_url, self._notify_written)

    def get_stats(self, path):
        with connection_for(self.scheme + path) as ftp:
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


class SftpFs(FtpFs):
    scheme = 'sftp://'
