"""Run one isolated FTP, FTPS, SFTP or account-import check per invocation.

Commands: reset, close, connect, list, upload, download, delete, import-accounts.
Every remote command starts a local test server and uses only the selected state folder.
"""
from dataclasses import dataclass
import argparse
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import types
from urllib.parse import quote

from ftp_cli_driver_env import (
    DriverProtocol, DriverState, default_state_dir, ensure_state,
    install_stubs, reset_state, running_server,
)


DEFAULT_USER = 'user'
DEFAULT_PASSWORD = 'secret'
DEFAULT_HOST = '127.0.0.1'
DEFAULT_FIXTURE = Path(__file__).with_name('ftp_cli_driver_accounts.json')
ACCOUNT_FIELDS = {'name', 'host', 'user', 'password', 'port', 'protocol'}
REMOTE_PATH_ERROR = 'remote path must start with / and stay inside the fixture root'
LOCAL_NAME_ERROR = 'local name must be a plain file name inside local/'


@dataclass
class FixtureAccount:
    name: str
    host: str
    user: str
    password: str
    port: int
    protocol: str
    folder: str = ''
    remote_dir: str = '/'

    def describe(self) -> str:
        return '%s (%s://%s@%s:%d)' % (
            self.name, self.protocol, self.user, self.host, self.port)

    def matches(self, query: str) -> bool:
        return query.lower() in self.describe().lower()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', default=default_state_dir())
    commands = parser.add_subparsers(dest='command', required=True)

    commands.add_parser('reset')
    commands.add_parser('close')
    connect = commands.add_parser('connect')
    connect.add_argument('protocol', choices=[item.value for item in DriverProtocol])
    connect.add_argument('--password', default=DEFAULT_PASSWORD)
    listing = commands.add_parser('list')
    listing.add_argument('protocol', choices=[item.value for item in DriverProtocol])
    listing.add_argument('remote_path', nargs='?', default='/')
    upload = commands.add_parser('upload')
    upload.add_argument('protocol', choices=[item.value for item in DriverProtocol])
    upload.add_argument('local_name')
    upload.add_argument('remote_path')
    download = commands.add_parser('download')
    download.add_argument('protocol', choices=[item.value for item in DriverProtocol])
    download.add_argument('remote_path')
    download.add_argument('local_name')
    delete = commands.add_parser('delete')
    delete.add_argument('protocol', choices=[item.value for item in DriverProtocol])
    delete.add_argument('remote_path')
    accounts = commands.add_parser('import-accounts')
    accounts.add_argument('fixture', nargs='?', default=str(DEFAULT_FIXTURE))
    return parser


def state_from_argument(path: str) -> DriverState:
    temp_root = Path(tempfile.gettempdir()).resolve()
    state_root = Path(path).expanduser().resolve()
    try:
        if os.path.commonpath((str(temp_root), str(state_root))) != str(temp_root) \
                or state_root == temp_root:
            raise ValueError(
                'state directory must be a child of the system temp folder')
    except ValueError:
        raise ValueError('state directory must be a child of the system temp folder')
    return DriverState(str(state_root))


def validate_remote_path(path: str) -> None:
    if not path.startswith('/') or '\x00' in path or \
            any(part == '..' for part in path.replace('\\', '/').split('/')):
        raise ValueError(REMOTE_PATH_ERROR)


def validate_local_name(name: str) -> None:
    drive, _ = os.path.splitdrive(name)
    if not name or name in ('.', '..') or drive or \
            '/' in name or '\\' in name or os.path.basename(name) != name:
        raise ValueError(LOCAL_NAME_ERROR)


def remote_url(protocol: DriverProtocol, port: int, path: str,
               password: str = DEFAULT_PASSWORD) -> str:
    return '%s://%s:%s@%s:%d%s' % (
        protocol.value, quote(DEFAULT_USER, safe=''), quote(password, safe=''),
        DEFAULT_HOST, port, quote(path, safe='/'))


def remote_fs_path(port: int, path: str) -> str:
    return '%s:%s@%s:%d%s' % (
        quote(DEFAULT_USER, safe=''), quote(DEFAULT_PASSWORD, safe=''),
        DEFAULT_HOST, port, quote(path, safe='/'))


def run_remote(args: argparse.Namespace, state: DriverState) -> None:
    protocol = DriverProtocol(args.protocol)
    if args.command in ('list', 'delete', 'download'):
        validate_remote_path(args.remote_path)
    if args.command in ('upload', 'download'):
        validate_local_name(args.local_name)

    ensure_state(state)
    install_stubs(state)
    with running_server(protocol, state) as port:
        if args.command == 'connect':
            connection_for = importlib.import_module(
                'ftpclient.connections').connection_for
            url = remote_url(protocol, port, '/', args.password)
            with connection_for(url):
                print('connected %s://%s@%s:%d' % (
                    protocol.value, DEFAULT_USER, DEFAULT_HOST, port))
        else:
            filesystems = importlib.import_module('ftpclient.filesystems')
            filesystem = protocol.filesystem_type()()
            if args.command == 'list':
                remote_path = remote_fs_path(port, args.remote_path)
                for name in sorted(filesystem.iterdir(remote_path)):
                    entry_path = remote_fs_path(
                        port, args.remote_path.rstrip('/') + '/' + name)
                    if filesystem.is_dir(entry_path):
                        print('dir  ' + name)
                    else:
                        print('file %s %d' % (
                            name, filesystem.size_bytes(entry_path)))
            elif args.command == 'upload':
                local_path = os.path.join(state.local, args.local_name)
                url = remote_url(protocol, port, args.remote_path)
                task = filesystems.transfers.prepare_file_copy(
                    'file://' + local_path, url, filesystem._notify_written)
                task()
                print('uploaded ' + args.remote_path)
            elif args.command == 'download':
                local_path = os.path.join(state.local, args.local_name)
                url = remote_url(protocol, port, args.remote_path)
                task = filesystems.transfers.prepare_file_copy(
                    url, 'file://' + local_path, filesystem._notify_written)
                task()
                size = os.path.getsize(local_path)
                print('downloaded %s (%d bytes)' % (args.local_name, size))
            elif args.command == 'delete':
                filesystem.delete(remote_fs_path(port, args.remote_path))
                print('deleted ' + args.remote_path)


def read_accounts(path: str) -> list[FixtureAccount]:
    with open(path, encoding='utf-8') as stream:
        values = json.load(stream)
    if not isinstance(values, list):
        raise ValueError('account fixture must be a JSON list')
    accounts = []
    for index, value in enumerate(values):
        if not isinstance(value, dict) or not ACCOUNT_FIELDS <= value.keys():
            raise ValueError('account fixture item %d has missing fields' % index)
        string_fields = ACCOUNT_FIELDS - {'port'}
        if any(not isinstance(value[field], str) for field in string_fields):
            raise ValueError('account fixture item %d has invalid text fields' % index)
        if not isinstance(value['port'], int) or isinstance(value['port'], bool) \
                or not 1 <= value['port'] <= 65535:
            raise ValueError('account fixture item %d has an invalid port' % index)
        if any(field in value and not isinstance(value[field], str)
               for field in ('folder', 'remote_dir')):
            raise ValueError(
                'account fixture item %d has invalid optional fields' % index)
        accounts.append(FixtureAccount(**value))
    return accounts


def run_import_accounts(fixture: str, state: DriverState) -> None:
    ensure_state(state)
    install_stubs(state)
    accounts = read_accounts(fixture)
    fake_reader = types.ModuleType('ftp_account_readers')
    fake_reader.__path__ = [str(
        Path(__file__).resolve().parents[1] / 'ftpclient' / 'vendor-import' /
        'ftp_account_readers')]
    fake_reader.is_winscp_available = lambda: True
    fake_reader.is_filezilla_available = lambda: False
    fake_reader.is_nppftp_available = lambda: False
    fake_reader.read_winscp = lambda: accounts
    fake_reader.read_filezilla = lambda: []
    fake_reader.read_nppftp = lambda: []
    sys.modules['ftp_account_readers'] = fake_reader

    import fman
    selections = ['WinSCP', 'Import all (%d)' % sum(
        bool(account.host and account.host.strip()) and
        account.protocol in ('ftp', 'ftps', 'sftp') for account in accounts)]

    def choose_item(callback):
        items = list(callback(''))
        selected = selections.pop(0)
        if not any(item.label == selected for item in items):
            raise ValueError('fixture import did not offer %s' % selected)
        return 0, selected

    fman.show_quicksearch = choose_item
    module = importlib.import_module('ftpclient.import_accounts')
    module.ImportFtpAccounts()()

    bookmarks_path = os.path.join(state.fman_json, 'FTP Bookmarks.json')
    try:
        with open(bookmarks_path, encoding='utf-8') as stream:
            bookmarks = json.load(stream)
    except FileNotFoundError:
        bookmarks = {}
    for alias, value in sorted(bookmarks.items()):
        print('bookmark %s -> %s' % (alias, value[0]))
    try:
        with open(state.credentials_file, encoding='utf-8') as stream:
            credentials = json.load(stream)
    except FileNotFoundError:
        credentials = {}
    for target in sorted(credentials):
        print('credential ' + target)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        state = state_from_argument(args.state_dir)
        if args.command == 'reset':
            for item in reset_state(state):
                print(item)
        elif args.command == 'close':
            import shutil
            shutil.rmtree(state.root, ignore_errors=True)
            print('closed')
        elif args.command == 'import-accounts':
            run_import_accounts(args.fixture, state)
        else:
            run_remote(args, state)
        return 0
    except Exception as error:
        message = str(error).splitlines()[0] if str(error) else type(error).__name__
        password = getattr(args, 'password', '')
        if password:
            message = message.replace(password, '<redacted>')
        print('error: ' + message)
        return 1


if __name__ == '__main__':
    sys.exit(main())
