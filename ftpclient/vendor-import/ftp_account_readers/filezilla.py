"""Read saved FileZilla sites from sitemanager.xml."""

import base64
import os
import xml.etree.ElementTree as ET
from pathlib import Path

from ftp_account_readers import constants as c
from ftp_account_readers.app_logger import AppLogger
from ftp_account_readers.helpers import join_folder, parse_port
from ftp_account_readers.models import FtpAccount, FtpProtocol


def _default_path() -> Path:
    appdata = Path(os.environ.get("APPDATA", Path.home() / "AppData" / "Roaming"))
    return appdata / c.FILEZILLA_RELATIVE_PATH


def is_filezilla_available() -> bool:
    return _default_path().is_file()


def read_filezilla(path: Path | None = None) -> list[FtpAccount]:
    """Read FileZilla sites from `path` or the user's default site manager file."""
    source = path if path is not None else _default_path()
    servers = ET.parse(source).getroot().find(c.FZ_SERVERS)
    return [] if servers is None else list(_walk(servers, ""))


def _walk(element: ET.Element, folder: str) -> list[FtpAccount]:
    accounts: list[FtpAccount] = []
    for node in element:
        if node.tag == c.FZ_FOLDER:
            accounts += _walk(node, join_folder(folder, (node.text or "").strip()))
        elif node.tag == c.FZ_SERVER and node.findtext(c.FZ_HOST):
            accounts.append(_parse_server(node, folder))
    return accounts


def _parse_server(node: ET.Element, folder: str) -> FtpAccount:
    protocol = _protocol(node.findtext(c.FZ_PROTOCOL, ""))
    host = node.findtext(c.FZ_HOST, "")
    pass_node = node.find(c.FZ_PASS)
    password = ""
    if pass_node is not None and pass_node.text:
        if pass_node.get("encoding") == "base64":
            password = base64.b64decode(pass_node.text).decode("utf-8")
        elif pass_node.get("encoding") == "crypt":
            AppLogger.warning(
                f"FileZilla: cannot read master-password protected password for {host}"
            )
        else:
            password = pass_node.text
    return FtpAccount(
        name=node.findtext(c.FZ_NAME) or host,
        host=host,
        user=node.findtext(c.FZ_USER, ""),
        password=password,
        port=parse_port(node.findtext(c.FZ_PORT), protocol),
        protocol=protocol,
        folder=folder,
        remote_dir=decode_remote_dir(node.findtext(c.FZ_REMOTE_DIR, "")),
    )


def _protocol(value: str) -> FtpProtocol:
    if value == str(c.FZ_PROTOCOL_SFTP):
        return FtpProtocol.SFTP
    if value in (str(c.FZ_PROTOCOL_FTPS_IMPLICIT), str(c.FZ_PROTOCOL_FTPS_EXPLICIT)):
        return FtpProtocol.FTPS
    return FtpProtocol.FTP


def decode_remote_dir(value: str) -> str:
    """FileZilla CServerPath: '<type> <prefix-len> [<len> <segment>]...'"""
    parts = value.split(" ", 2)
    if len(parts) < 3:
        return ""
    rest, segments = parts[2], []
    try:
        while rest:
            length_text, rest = rest.split(" ", 1)
            length = int(length_text)
            segments.append(rest[:length])
            rest = rest[length + 1 :]
    except ValueError:
        return ""
    return "/" + "/".join(segments)
