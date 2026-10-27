"""Read saved Notepad++ NppFTP profiles from NppFTP.xml."""

import os
import xml.etree.ElementTree as ET
from pathlib import Path

from ftp_account_readers import constants as c
from ftp_account_readers import nppftp_password
from ftp_account_readers.app_logger import AppLogger
from ftp_account_readers.helpers import parse_port
from ftp_account_readers.models import FtpAccount, FtpProtocol


def _default_path() -> Path:
    appdata = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    return appdata / c.NPPFTP_RELATIVE_PATH


def is_nppftp_available() -> bool:
    try:
        from Crypto.Cipher import DES  # noqa: F401
    except ImportError:
        return False
    return _default_path().is_file()


def read_nppftp(path: Path | None = None) -> list[FtpAccount]:
    """Read NppFTP profiles from `path` or the user's default configuration file."""
    source = path if path is not None else _default_path()
    profiles = ET.parse(source).getroot().find(c.NPP_PROFILES)
    if profiles is None:
        return []
    return [
        _parse(profile) for profile in profiles.findall(c.NPP_PROFILE) if profile.get("hostname")
    ]


def _parse(profile: ET.Element) -> FtpAccount:
    protocol = _protocol(profile.get("securityMode", ""))
    host = profile.get("hostname", "")
    password = ""
    try:
        password = nppftp_password.decrypt(profile.get("password", ""))
    except (ValueError, UnicodeDecodeError):
        AppLogger.warning(f"NppFTP: cannot decrypt password of profile {profile.get('name')}")
    return FtpAccount(
        name=profile.get("name") or host,
        host=host,
        user=profile.get("username", ""),
        password=password,
        port=parse_port(profile.get("port"), protocol),
        protocol=protocol,
        folder=profile.get("parent", ""),
        remote_dir=profile.get("initialDir", ""),
    )


def _protocol(value: str) -> FtpProtocol:
    if value == str(c.NPP_SECURITY_SFTP):
        return FtpProtocol.SFTP
    if value in (str(c.NPP_SECURITY_FTPS), str(c.NPP_SECURITY_FTPES)):
        return FtpProtocol.FTPS
    return FtpProtocol.FTP
