"""Keep CLI driver state in throwaway files and start one local server per call."""
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
import atexit
import importlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Iterator


REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_VENDOR = REPO_ROOT / 'test-vendor'
REMOTE_SOURCE = b'hello from the CLI driver'
LOCAL_UPLOAD = b'upload test data'

sys.path.insert(0, str(REPO_ROOT))
import sftp_test_support  # noqa: E402

# This module creates its own seeded root on import for the SFTP integration test.
# The CLI always supplies a separate state root, so discard that unused test root.
atexit.register(shutil.rmtree, sftp_test_support.temp_root, ignore_errors=True)


class DriverProtocol(Enum):
    FTP = 'ftp'
    FTPS = 'ftps'
    SFTP = 'sftp'

    def filesystem_type(self) -> type:
        filesystems = importlib.import_module('ftpclient.filesystems')
        return {
            DriverProtocol.FTP: filesystems.FtpFs,
            DriverProtocol.FTPS: filesystems.FtpsFs,
            DriverProtocol.SFTP: filesystems.SftpFs,
        }[self]

    def start_server(self, root: str) -> Any:
        return {
            DriverProtocol.FTP: _start_ftp_server,
            DriverProtocol.FTPS: _start_ftps_server,
            DriverProtocol.SFTP: lambda server_root: (
                sftp_test_support.SftpTestServer(root=server_root)),
        }[self](root)


def _start_ftp_server(root: str) -> Any:
    return _start_ftp_server_with_tls(root, False)


def _start_ftps_server(root: str) -> Any:
    return _start_ftp_server_with_tls(root, True)


def _start_ftp_server_with_tls(root: str, tls: bool) -> Any:
    if not ftp_vendor_available():
        raise RuntimeError(
            r'test-vendor/ is missing; run tools\vendor_test_deps.bat')
    server_module = importlib.import_module('ftp_server_support')
    return server_module.FtpTestServer(tls=tls, root=root)


@dataclass(frozen=True)
class DriverState:
    root: str

    @property
    def remote(self) -> str:
        return os.path.join(self.root, 'remote')

    @property
    def local(self) -> str:
        return os.path.join(self.root, 'local')

    @property
    def fman_json(self) -> str:
        return os.path.join(self.root, 'fman_json')

    @property
    def credentials_file(self) -> str:
        return os.path.join(self.root, 'credentials.json')

    @property
    def known_hosts(self) -> str:
        return os.path.join(self.root, 'known_hosts')


def default_state_dir() -> str:
    return os.path.join(tempfile.gettempdir(), 'fman-ftp-cli-driver')


def ftp_vendor_available() -> bool:
    return (TEST_VENDOR / 'pyftpdlib').is_dir() and \
        (TEST_VENDOR / 'OpenSSL').is_dir()


def reset_state(state: DriverState) -> list[str]:
    shutil.rmtree(state.root, ignore_errors=True)
    os.makedirs(os.path.join(state.remote, 'folder'))
    os.makedirs(state.local)
    with open(os.path.join(state.remote, 'folder', 'source.txt'), 'wb') as stream:
        stream.write(REMOTE_SOURCE)
    with open(os.path.join(state.local, 'upload.txt'), 'wb') as stream:
        stream.write(LOCAL_UPLOAD)
    return ['/folder/source.txt', 'local/upload.txt']


def ensure_state(state: DriverState) -> None:
    if not os.path.isdir(state.root):
        reset_state(state)


def install_stubs(state: DriverState) -> None:
    os.makedirs(state.fman_json, exist_ok=True)
    fman = sys.modules['fman']
    json_cache = {}

    def load_json(name, default=None, save_on_quit=False):
        if name not in json_cache:
            path = os.path.join(state.fman_json, name)
            try:
                with open(path, encoding='utf-8') as stream:
                    json_cache[name] = json.load(stream)
            except FileNotFoundError:
                json_cache[name] = {} if default is None else default
        return json_cache[name]

    def save_json(name):
        path = os.path.join(state.fman_json, name)
        with open(path, 'w', encoding='utf-8') as stream:
            json.dump(json_cache[name], stream, indent=2)

    fman.load_json = load_json
    fman.save_json = save_json
    fman.show_alert = lambda message, **kwargs: print('alert: ' + message)
    fman.DirectoryPaneCommand = type('DirectoryPaneCommand', (), {})
    fman.QuicksearchItem = type(
        'QuicksearchItem', (),
        {'__init__': lambda self, label, **kwargs: setattr(self, 'label', label)})

    credentials = importlib.import_module('ftpclient.credentials')

    def read_credentials():
        try:
            with open(state.credentials_file, encoding='utf-8') as stream:
                return json.load(stream)
        except FileNotFoundError:
            return {}

    def write_credentials(values):
        with open(state.credentials_file, 'w', encoding='utf-8') as stream:
            json.dump(values, stream, indent=2)

    def save_password(target, password):
        values = read_credentials()
        values[target] = password
        write_credentials(values)

    def delete_password(target):
        values = read_credentials()
        values.pop(target, None)
        write_credentials(values)

    credentials.read_password = lambda target: read_credentials().get(target)
    credentials.save_password = save_password
    credentials.delete_password = delete_password

    sftp = importlib.import_module('ftpclient.sftp')
    original_connect = sftp.paramiko.SSHClient.connect

    def connect_without_local_keys(client, *args, **kwargs):
        kwargs['allow_agent'] = False
        kwargs['look_for_keys'] = False
        return original_connect(client, *args, **kwargs)

    sftp.paramiko.SSHClient.connect = connect_without_local_keys
    Path(state.known_hosts).parent.mkdir(parents=True, exist_ok=True)
    Path(state.known_hosts).write_text('', encoding='utf-8')
    sftp._confirm_host_key = lambda *args: True
    sftp._known_hosts_path = lambda: state.known_hosts


@contextmanager
def running_server(protocol: DriverProtocol, state: DriverState) -> Iterator[int]:
    server = protocol.start_server(state.remote)
    try:
        yield server.port
    finally:
        server.close()
        ftp = importlib.import_module('ftpclient.ftp')
        ftp.FtpWrapper.close_all_connections()
