"""Read saved FTP accounts from WinSCP, FileZilla, and Notepad++ NppFTP."""

from ftp_account_readers.filezilla import is_filezilla_available, read_filezilla
from ftp_account_readers.models import FtpAccount, FtpProtocol
from ftp_account_readers.nppftp import is_nppftp_available, read_nppftp
from ftp_account_readers.winscp import is_winscp_available, read_winscp

__all__ = [
    "FtpAccount",
    "FtpProtocol",
    "is_filezilla_available",
    "is_nppftp_available",
    "is_winscp_available",
    "read_filezilla",
    "read_nppftp",
    "read_winscp",
]
