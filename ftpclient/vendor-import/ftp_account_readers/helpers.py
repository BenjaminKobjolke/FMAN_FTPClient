"""Small shared helpers used by the readers."""

import xml.etree.ElementTree as ET
from collections.abc import Collection

from ftp_account_readers import constants
from ftp_account_readers.models import FtpProtocol


def unique_name(name: str, taken: Collection[str]) -> str:
    """Return `name`, or `name (2)`, `name (3)`, ... if already taken."""
    candidate, counter = name, 2
    while candidate in taken:
        candidate = f"{name} ({counter})"
        counter += 1
    return candidate


def join_folder(*parts: str) -> str:
    return constants.FOLDER_SEPARATOR.join(p for p in parts if p)


def parse_port(value: str | int | None, protocol: FtpProtocol) -> int:
    if value is None or value == "":
        return protocol.default_port
    try:
        return int(value)
    except ValueError:
        return protocol.default_port


def child(parent: ET.Element, tag: str) -> ET.Element:
    """Return the first `tag` child, creating it if absent."""
    found = parent.find(tag)
    return found if found is not None else ET.SubElement(parent, tag)
