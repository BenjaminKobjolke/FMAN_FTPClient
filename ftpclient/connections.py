"""Choose the pooled wrapper that matches a remote URL's protocol."""
import re
from urllib.parse import urlparse

from .ftp import FtpWrapper


is_ftp = re.compile('^(?:ftps?|sftp)://').match
is_file = re.compile('^file://').match


def connection_for(url: str) -> FtpWrapper:
    """Return the FTP or SFTP wrapper selected by ``url``."""
    if urlparse(url).scheme == 'sftp':
        from .sftp import SftpWrapper
        return SftpWrapper(url)
    return FtpWrapper(url)
