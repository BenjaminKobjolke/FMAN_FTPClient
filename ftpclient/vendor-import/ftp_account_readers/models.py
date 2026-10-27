"""Domain types shared by the account readers."""

from dataclasses import dataclass
from enum import StrEnum

from ftp_account_readers import constants


class FtpProtocol(StrEnum):
    FTP = "ftp"
    FTPS = "ftps"
    SFTP = "sftp"

    @property
    def default_port(self) -> int:
        return (
            constants.SFTP_DEFAULT_PORT if self is FtpProtocol.SFTP else constants.FTP_DEFAULT_PORT
        )


@dataclass(frozen=True)
class FtpAccount:
    """One stored site. `folder` uses '/' between nesting levels, '' for top level."""

    name: str
    host: str
    user: str
    password: str
    port: int
    protocol: FtpProtocol
    folder: str = ""
    remote_dir: str = ""

    def match_key(self) -> tuple[str, str]:
        # Port and protocol are ignored on purpose: the same login is often saved
        # as FTP in one app and FTPS in another.
        return (self.host.strip().lower(), self.user.strip().lower())

    def describe(self) -> str:
        """Display text without the password."""
        location = f"{self.folder}/{self.name}" if self.folder else self.name
        return f"{location}  ({self.protocol.value}://{self.user}@{self.host}:{self.port})"

    def matches(self, query: str) -> bool:
        """Every whitespace-separated term is a case-insensitive substring of describe()."""
        text = self.describe().lower()
        return all(term in text for term in query.lower().split())
