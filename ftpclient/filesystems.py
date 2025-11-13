import errno
import re
import stat
from datetime import datetime
from io import UnsupportedOperation
from os.path import commonprefix, dirname, join as pathjoin
from tempfile import NamedTemporaryFile

from fman import fs, load_json, show_status_message
from fman.fs import FileSystem, cached
from fman.url import join as urljoin, splitscheme

from .ftp import FtpWrapper

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
        except (OSError, IOError, ConnectionError, ftputil.error.FTPError):
            # If we can't connect, the path doesn't exist from our perspective
            return False

    @cached
    def is_dir(self, path):
        try:
            with FtpWrapper(self.scheme + path) as ftp:
                return ftp.conn.path.isdir(ftp.path)
        except (OSError, IOError, ConnectionError, ftputil.error.FTPError):
            # If we can't connect, assume it's not a directory
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

    def move_to_trash(self, path):
        # ENOSYS: Function not implemented
        raise OSError(errno.ENOSYS, "FTP has no Trash support")

    def mkdir(self, path):
        with FtpWrapper(self.scheme + path) as ftp:
            ftp.conn.makedirs(ftp.path)

    def touch(self, path):
        if self.exists(path):
            raise OSError(errno.EEXIST, "File exists")
        with FtpWrapper(self.scheme + path) as ftp:
            with NamedTemporaryFile(delete=True) as tmp:
                ftp.conn.upload(tmp.name, ftp.path)

    def samefile(self, path1, path2):
        return path1 == path2

    def copy(self, src_url, dst_url):
        # Recursive copy
        if fs.is_dir(src_url):
            fs.mkdir(dst_url)
            for fname in fs.iterdir(src_url):
                fs.copy(urljoin(src_url, fname), urljoin(dst_url, fname))
            return

        if is_ftp(src_url) and is_ftp(dst_url):
            with FtpWrapper(src_url) as src_ftp, \
                    FtpWrapper(dst_url) as dst_ftp:
                with src_ftp.conn.open(src_ftp.path, 'rb') as src, \
                        dst_ftp.conn.open(dst_ftp.path, 'wb') as dst:
                    dst_ftp.conn.copyfileobj(src, dst)
        elif is_ftp(src_url) and is_file(dst_url):
            _, dst_path = splitscheme(dst_url)
            with FtpWrapper(src_url) as src_ftp:
                src_ftp.conn.download(src_ftp.path, dst_path)
        elif is_file(src_url) and is_ftp(dst_url):
            _, src_path = splitscheme(src_url)
            with FtpWrapper(dst_url) as dst_ftp:
                dst_ftp.conn.upload(src_path, dst_ftp.path)
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
