"""Read WinSCP sessions from the current user's registry."""

from typing import Any
from urllib.parse import unquote

from ftp_account_readers import constants as c
from ftp_account_readers import winscp_password
from ftp_account_readers.app_logger import AppLogger
from ftp_account_readers.helpers import parse_port
from ftp_account_readers.models import FtpAccount, FtpProtocol


def is_winscp_available() -> bool:
    try:
        import winreg
    except ImportError:
        return False
    try:
        winreg.CloseKey(winreg.OpenKey(winreg.HKEY_CURRENT_USER, c.WINSCP_SESSIONS_KEY))
        return True
    except OSError:
        return False


def read_winscp() -> list[FtpAccount]:
    """Read WinSCP sessions from the current user's registry."""
    import winreg

    accounts = []
    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, c.WINSCP_SESSIONS_KEY) as root:
        for raw_name in _session_names(root):
            if raw_name == c.WINSCP_DEFAULT_SETTINGS_SESSION:
                continue
            with winreg.OpenKey(root, raw_name) as session:
                account = _parse(unquote(raw_name), session)
            if account is not None:
                accounts.append(account)
    return accounts


def _session_names(root: Any) -> list[str]:
    import winreg

    return [winreg.EnumKey(root, i) for i in range(winreg.QueryInfoKey(root)[0])]


def _read(session: Any, name: str) -> str | int | None:
    import winreg

    try:
        value: str | int = winreg.QueryValueEx(session, name)[0]
        return value
    except OSError:
        return None


def _parse(full_name: str, session: object) -> FtpAccount | None:
    host, user = _read(session, c.WINSCP_HOST), _read(session, c.WINSCP_USER) or ""
    if not isinstance(host, str) or not host:
        return None
    protocol = _protocol(_read(session, c.WINSCP_FS_PROTOCOL), _read(session, c.WINSCP_FTPS))
    password = ""
    encrypted = _read(session, c.WINSCP_PASSWORD)
    if isinstance(encrypted, str) and encrypted:
        try:
            password = winscp_password.decrypt(host, str(user), encrypted)
        except (ValueError, IndexError, UnicodeDecodeError):
            AppLogger.warning(f"WinSCP: cannot decrypt password of session {full_name}")
    folder, _, name = full_name.rpartition(c.FOLDER_SEPARATOR)
    port = _read(session, c.WINSCP_PORT)
    return FtpAccount(
        name=name,
        host=host,
        user=str(user),
        password=password,
        port=parse_port(port if isinstance(port, int) else None, protocol),
        protocol=protocol,
        folder=folder,
        remote_dir=str(_read(session, c.WINSCP_REMOTE_DIR) or ""),
    )


def _protocol(fs_protocol: str | int | None, ftps: str | int | None) -> FtpProtocol:
    if fs_protocol != c.WINSCP_FS_FTP:
        return FtpProtocol.SFTP
    return FtpProtocol.FTP if ftps in (None, c.WINSCP_FTPS_NONE) else FtpProtocol.FTPS
