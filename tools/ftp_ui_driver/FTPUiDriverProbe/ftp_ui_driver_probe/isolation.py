"""Session-local replacements for plugin credentials and external sources."""

import json
from pathlib import Path
from typing import Any, Mapping

try:
    from . import constants
except ImportError:
    import constants


def install_isolation(session_root: str, modules: Mapping[str, object]) -> None:
    """Redirect plugin credentials, SSH files and account imports to the session."""
    root = Path(session_root)
    credentials = modules['credentials']
    sftp = modules['sftp']
    account_readers = modules['ftp_account_readers']
    credential_path = root / constants.CREDENTIALS_FILE

    def read_credentials() -> dict[str, str]:
        try:
            data = json.loads(credential_path.read_text(encoding='utf-8'))
        except FileNotFoundError:
            return {}
        if not isinstance(data, dict):
            raise ValueError('Session credentials must be a JSON object')
        return data

    def write_credentials(data: dict[str, str]) -> None:
        root.mkdir(parents=True, exist_ok=True)
        temporary_path = credential_path.with_suffix('.json.tmp')
        temporary_path.write_text(
            json.dumps(data, indent=2) + '\n', encoding='utf-8')
        temporary_path.replace(credential_path)

    def read_password(target: str) -> str | None:
        return read_credentials().get(target)

    def save_password(target: str, password: str) -> None:
        data = read_credentials()
        data[target] = password
        write_credentials(data)

    def delete_password(target: str) -> None:
        data = read_credentials()
        data.pop(target, None)
        write_credentials(data)

    credentials.read_password = read_password
    credentials.save_password = save_password
    credentials.delete_password = delete_password
    sftp._known_hosts_path = lambda: str(root / constants.KNOWN_HOSTS_FILE)

    ssh_client = sftp.paramiko.SSHClient
    original_connect = ssh_client.connect

    def connect_without_user_keys(client: object, *args: Any, **kwargs: Any):
        kwargs['allow_agent'] = False
        kwargs['look_for_keys'] = False
        return original_connect(client, *args, **kwargs)

    ssh_client.connect = connect_without_user_keys
    for source in ('winscp', 'filezilla', 'nppftp'):
        setattr(account_readers, 'is_%s_available' % source, lambda: False)
