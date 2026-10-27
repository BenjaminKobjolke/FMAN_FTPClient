"""Reader-related string and numeric constants."""

FTP_DEFAULT_PORT = 21
SFTP_DEFAULT_PORT = 22
FOLDER_SEPARATOR = "/"

# Default store locations, relative to %APPDATA% (registry key relative to HKCU).
FILEZILLA_RELATIVE_PATH = r"FileZilla\sitemanager.xml"
NPPFTP_RELATIVE_PATH = r"Notepad++\plugins\config\NppFTP\NppFTP.xml"
WINSCP_SESSIONS_KEY = r"Software\Martin Prikryl\WinSCP 2\Sessions"

# WinSCP registry values
WINSCP_HOST = "HostName"
WINSCP_PORT = "PortNumber"
WINSCP_USER = "UserName"
WINSCP_PASSWORD = "Password"
WINSCP_FS_PROTOCOL = "FSProtocol"
WINSCP_FTPS = "Ftps"
WINSCP_REMOTE_DIR = "RemoteDirectory"
WINSCP_FS_SFTP = 2
WINSCP_FS_FTP = 5
WINSCP_FTPS_NONE = 0
WINSCP_FTPS_EXPLICIT = 3
WINSCP_DEFAULT_SETTINGS_SESSION = "Default Settings"

# FileZilla sitemanager.xml
FZ_SERVERS = "Servers"
FZ_FOLDER = "Folder"
FZ_SERVER = "Server"
FZ_HOST = "Host"
FZ_PORT = "Port"
FZ_PROTOCOL = "Protocol"
FZ_TYPE = "Type"
FZ_USER = "User"
FZ_PASS = "Pass"
FZ_LOGONTYPE = "Logontype"
FZ_NAME = "Name"
FZ_REMOTE_DIR = "RemoteDir"
FZ_PROTOCOL_FTP = 0
FZ_PROTOCOL_SFTP = 1
FZ_PROTOCOL_FTPS_IMPLICIT = 3
FZ_PROTOCOL_FTPS_EXPLICIT = 4
FZ_LOGONTYPE_NORMAL = 1

# NppFTP.xml
NPP_PROFILES = "Profiles"
NPP_PROFILE = "Profile"
NPP_SECURITY_FTP = 0
NPP_SECURITY_FTPS = 1
NPP_SECURITY_FTPES = 2
NPP_SECURITY_SFTP = 3
NPP_DEFAULT_KEY = b"NppFTP00"
