"""FTP sessions and host helpers the connection pool builds on."""
import ftplib
import os
import sys

from . import credentials

try:
    import ftputil
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), 'ftputil-3.4'))
    import ftputil


class FtpConnectionError(OSError):
    """Raised when the FTP server cannot be reached or refuses the login."""


def is_connection_limit_error(error):
    cause = error.__cause__
    while cause is not None:
        if (
            isinstance(cause, ftputil.error.PermanentError)
            and str(cause).startswith('530')
            and 'connection' in str(cause).lower()
        ):
            return True
        cause = cause.__cause__
    return False


# A stuck server must not block the worker forever; this also bounds transfers.
SOCKET_TIMEOUT_SECS = 30
DEFAULT_PORTS: dict[str, int] = {'ftp': 21, 'ftps': 21, 'sftp': 22}


class FtpSession(ftplib.FTP):
    def __init__(self, host, port, user, password):
        super().__init__(timeout=SOCKET_TIMEOUT_SECS)
        self.connect(host, port)
        self.login(user, password)


class FtpTlsSession(ftplib.FTP_TLS):
    def __init__(self, host, port, user, password):
        super().__init__(timeout=SOCKET_TIMEOUT_SECS)
        self.connect(host, port)
        self.login(user, password)
        self.prot_p()


def format_base_url(scheme: str, user: str, host: str, port: int) -> str:
    """Format a connection URL without its path or password."""
    user_info = '%s@' % user if user else ''
    return '%s%s%s:%d' % (scheme, user_info, host, port)


def open_ftp_host(scheme, host, port, user, url_password):
    """Create an FTPHost and configure its listing and cache behavior."""
    session_factory = FtpTlsSession if scheme == 'ftps://' else FtpSession
    try:
        password = url_password or credentials.read_password(
            credentials.credential_target(
                format_base_url(scheme, user, host, port))) or ''
        ftp_host = ftputil.FTPHost(
            host, port, user, password, session_factory=session_factory)
    except Exception as e:
        reason = str(e).splitlines()[0] if str(e) else repr(e)
        raise FtpConnectionError(
            'Could not connect to %s%s@%s:%d\n\n%s'
            % (scheme, user, host, port, reason)
        ) from e

    # Some servers reject LIST -a; hidden files then stay hidden.
    ftp_host.use_list_a_option = False
    ftp_host.stat_cache.resize(20000)
    return ftp_host


def close_finished_children(ftp_host) -> None:
    """Close child sessions whose file transfers have finished."""
    stale_indices = []
    for i, host in enumerate(ftp_host._children):
        if host._file.closed:
            try:
                host._session.close()
            except Exception:
                pass
            stale_indices.append(i)
    for i in reversed(stale_indices):
        del ftp_host._children[i]


def close_host(host) -> None:
    """Close child sessions and the pooled host, tolerating partial failure."""
    for child in getattr(host, '_children', ())[:]:
        try:
            child.close()
        except Exception:
            pass
    try:
        host.close()
    except Exception:
        pass
