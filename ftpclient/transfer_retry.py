"""Configuration for retries of one failed transfer."""

from fman import load_json

from .ftp_session import ftputil

DEFAULT_TRANSFER_RETRIES = 2
MAX_TRANSFER_RETRIES = 10
# Errors a reconnect can fix; cancellation and bugs still propagate.
TRANSIENT_ERRORS = (OSError, ftputil.error.FTPError)


def parse_transfer_retries(value) -> int | None:
    """Return `value` as a retry count, or None when it is not one."""
    try:
        retries = int(value)
    except (TypeError, ValueError):
        return None
    return retries if 0 <= retries <= MAX_TRANSFER_RETRIES else None


def transfer_retries() -> int:
    """Return the retries per failed transfer from `FTP Settings.json`."""
    settings = load_json('FTP Settings.json', default={})
    retries = parse_transfer_retries(
        settings.get('transfer_retries', DEFAULT_TRANSFER_RETRIES))
    return DEFAULT_TRANSFER_RETRIES if retries is None else retries
