"""Focused tests for fixture-server pacing used by the desktop UI driver."""
import ftplib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from ftp_ui_driver_test_support import (
    _test_driver_commands, _test_profile_staging,
    _test_session_close_order, _test_session_launch_and_cleanup,
)
from types import SimpleNamespace
from unittest import mock
from io import StringIO


HERE = os.path.dirname(os.path.abspath(__file__))
FTP_CASES_AVAILABLE = all(os.path.isdir(os.path.join(HERE, 'test-vendor', name))
                          for name in ('pyftpdlib', 'OpenSSL'))
PROBE_MODULES = os.path.join(
    HERE, 'tools', 'ftp_ui_driver', 'FTPUiDriverProbe',
    'ftp_ui_driver_probe')
sys.path.insert(0, PROBE_MODULES)


def _remove_sftp_file(delay_seconds: float) -> float:
    from sftp_test_support import SftpTestServer, paramiko

    with tempfile.TemporaryDirectory(prefix='ftp-ui-sftp-test-') as root:
        path = os.path.join(root, 'remove.txt')
        with open(path, 'wb') as stream:
            stream.write(b'test')
        server = SftpTestServer(root=root, delay_seconds=delay_seconds)
        assert server.delay_seconds == delay_seconds
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect('127.0.0.1', port=server.port, username='user',
                           password='secret', allow_agent=False,
                           look_for_keys=False, timeout=5)
            sftp = client.open_sftp()
            try:
                started = time.monotonic()
                sftp.remove('/remove.txt')
                elapsed = time.monotonic() - started
            finally:
                sftp.close()
        finally:
            client.close()
            server.close()
        assert not os.path.exists(path)
        return elapsed


def _remove_ftp_file(delay_seconds: float) -> float:
    from ftp_server_support import FtpTestServer

    with tempfile.TemporaryDirectory(prefix='ftp-ui-ftp-test-') as root:
        path = os.path.join(root, 'remove.txt')
        with open(path, 'wb') as stream:
            stream.write(b'test')
        server = FtpTestServer(root=root, delay_seconds=delay_seconds)
        expected_server = 'FTPServer' if delay_seconds == 0 else 'ThreadedFTPServer'
        assert type(server.server).__name__ == expected_server
        client = ftplib.FTP()
        try:
            client.connect('127.0.0.1', server.port, timeout=5)
            client.login('user', 'secret')
            started = time.monotonic()
            client.delete('remove.txt')
            elapsed = time.monotonic() - started
        finally:
            try:
                client.quit()
            except (OSError, ftplib.Error):
                pass
            server.close()
        assert not os.path.exists(path)
        return elapsed


def _fixture_paths() -> list[str]:
    paths = [
        'left/single.txt',
        'left/upload/file01.txt',
        'left/upload/file02.txt',
        'left/upload/file03.txt',
        'left/upload/file04.txt',
        'left/upload/file05.txt',
        'left/upload/file06.txt',
        'left/upload/file07.txt',
        'left/upload/file08.txt',
        'left/upload/file09.txt',
        'left/upload/file10.txt',
        'left/upload/nested/a.txt',
        'left/upload/nested/b.txt',
    ]
    for protocol in ('ftp', 'sftp'):
        paths.extend([
            f'remote/{protocol}/folder/source.txt',
            *(f'remote/{protocol}/delete-me/d{i:02}.txt'
              for i in range(1, 7)),
            *(f'remote/{protocol}/delete-me/sub/s{i}.txt'
              for i in range(1, 4)),
            f'remote/{protocol}/single.txt',
        ])
    return sorted(paths)


def _test_fixtures() -> None:
    from tools.ftp_ui_driver import fixtures

    with tempfile.TemporaryDirectory(prefix='ftp-ui-fixtures-test-') as root:
        paths = fixtures.seed(root)
        assert paths == _fixture_paths()
        assert os.path.isdir(os.path.join(root, 'right'))
        assert not os.listdir(os.path.join(root, 'right'))
        for relative in paths:
            path = os.path.join(root, *relative.split('/'))
            assert os.path.isfile(path), relative
            if '/upload/file' in relative:
                assert os.path.getsize(path) == 32 * 1024
        assert fixtures.server_files(root, 'ftp') == sorted(
            path.removeprefix('remote/ftp/') for path in paths
            if path.startswith('remote/ftp/'))
        assert fixtures.server_files(root, 'sftp') == sorted(
            path.removeprefix('remote/sftp/') for path in paths
            if path.startswith('remote/sftp/'))

        with open(os.path.join(root, 'left', 'upload', 'file01.txt'), 'wb') as stream:
            stream.write(b'deleted')
        os.remove(os.path.join(root, 'remote', 'ftp', 'single.txt'))
        assert fixtures.seed(root) == paths
        assert os.path.getsize(os.path.join(
            root, 'left', 'upload', 'file01.txt')) == 32 * 1024
        assert 'single.txt' in fixtures.server_files(root, 'ftp')

        try:
            fixtures.server_files(root, 'http')
        except ValueError:
            pass
        else:
            raise AssertionError('unknown protocol should fail')


def _test_server_readiness() -> None:
    from tools.ftp_ui_driver import fixtures

    with tempfile.TemporaryDirectory(prefix='ftp-ui-server-test-') as root:
        fixtures.seed(root)
        script = os.path.join(HERE, 'tools', 'ftp_ui_driver', 'servers.py')
        process = subprocess.Popen(
            [sys.executable, script, root, '0'],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            marker = os.path.join(root, 'servers.json')
            deadline = time.monotonic() + 15
            while not os.path.isfile(marker) and time.monotonic() < deadline:
                if process.poll() is not None:
                    error = process.stderr.read() if process.stderr else ''
                    raise AssertionError(f'server process exited early: {error}')
                time.sleep(0.05)
            assert os.path.isfile(marker), 'server readiness marker timed out'

            import json
            from sftp_test_support import paramiko

            with open(marker, encoding='utf-8') as stream:
                ready = json.load(stream)
            assert ready['pid'] == process.pid
            assert isinstance(ready['sftp_port'], int)
            assert os.path.isfile(os.path.join(root, 'known_hosts'))
            with open(os.path.join(root, 'known_hosts'), encoding='utf-8') as stream:
                host_entry = stream.read().strip().split()
            assert host_entry[0] == f"[127.0.0.1]:{ready['sftp_port']}"
            assert len(host_entry) == 3

            client = paramiko.SSHClient()
            client.load_host_keys(os.path.join(root, 'known_hosts'))
            client.connect(
                '127.0.0.1', port=ready['sftp_port'], username='user',
                password='secret', allow_agent=False, look_for_keys=False,
                timeout=5)
            try:
                assert sorted(client.open_sftp().listdir('/')) == [
                    'delete-me', 'folder', 'single.txt']
            finally:
                client.close()

            if not FTP_CASES_AVAILABLE:
                assert ready['ftp_port'] is None
                assert ready['ftp_error'] == (
                    r'test-vendor/ is missing; run tools\vendor_test_deps.bat')
        finally:
            process.terminate()
            process.wait(timeout=5)


class _FakeWidget:
    def __init__(self, *, visible: bool = True, children=None) -> None:
        self.visible = visible
        self.children = list(children or [])

    def isVisible(self) -> bool:
        return self.visible

    def findChildren(self, widget_type):
        matches = []
        for child in self.children:
            if isinstance(child, widget_type):
                matches.append(child)
            matches.extend(child.findChildren(widget_type))
        return matches


class _FakeLabel(_FakeWidget):
    def __init__(self, text: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._text = text

    def text(self) -> str:
        return self._text


class _FakeButton(_FakeWidget):
    def __init__(self, text: str, enabled: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self._text = text
        self._enabled = enabled

    def text(self) -> str:
        return self._text

    def isEnabled(self) -> bool:
        return self._enabled


class _FakeProgressBar(_FakeWidget):
    def __init__(self, value: int, maximum: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self._value = value
        self._maximum = maximum

    def value(self) -> int:
        return self._value

    def maximum(self) -> int:
        return self._maximum


class _FakeWindow(_FakeWidget):
    def __init__(self, title: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._title = title

    def windowTitle(self) -> str:
        return self._title


def _test_window_helpers() -> None:
    import windows

    window = _FakeWindow('Copying upload', children=[
        _FakeLabel('Preparing files'),
        _FakeLabel('hidden', visible=False),
        _FakeLabel('  '),
        _FakeProgressBar(2, 5),
        _FakeButton('Cancel', enabled=True),
        _FakeButton('Pause', enabled=False, visible=False),
    ])
    assert windows.describe_window(
        window, _FakeLabel, _FakeButton, _FakeProgressBar) == {
            'title': 'Copying upload',
            'texts': ['Preparing files'],
            'value': 2,
            'maximum': 5,
            'buttons': [{'text': 'Cancel', 'enabled': True}],
        }

    match = _FakeWindow('Deleting folder')
    assert windows.find_window([window, match], 'DELET') is match
    for candidates, title, expected in (
        ([window], 'missing', 'No visible window'),
        ([window, _FakeWindow('COPYING another')], 'copying', 'Multiple'),
    ):
        try:
            windows.find_window(candidates, title)
        except ValueError as error:
            assert expected in str(error), str(error)
        else:
            raise AssertionError('window lookup should fail: ' + title)


def _test_probe_isolation() -> None:
    import constants
    from isolation import install_isolation

    class FakeSshClient:
        def connect(self, host, **kwargs):
            self.connection = (host, kwargs)

    client_type = FakeSshClient
    credentials = SimpleNamespace(
        read_password=lambda _target: 'real password',
        save_password=lambda _target, _password: None,
        delete_password=lambda _target: None,
    )
    sftp = SimpleNamespace(
        _known_hosts_path=lambda: 'user known_hosts',
        paramiko=SimpleNamespace(SSHClient=client_type),
    )
    readers = SimpleNamespace(
        is_winscp_available=lambda: True,
        is_filezilla_available=lambda: True,
        is_nppftp_available=lambda: True,
    )

    with tempfile.TemporaryDirectory(prefix='ftp-ui-isolation-test-') as root:
        install_isolation(root, {
            'credentials': credentials,
            'sftp': sftp,
            'ftp_account_readers': readers,
        })
        assert sftp._known_hosts_path() == os.path.join(root, 'known_hosts')
        target = 'FMAN_FTPClient:SFTP:sftp://user@127.0.0.1:22'
        assert credentials.read_password(target) is None
        credentials.save_password(target, 'secret')
        assert credentials.read_password(target) == 'secret'
        with open(os.path.join(root, constants.CREDENTIALS_FILE),
                  encoding='utf-8') as stream:
            assert json.load(stream) == {target: 'secret'}
        credentials.delete_password(target)
        assert credentials.read_password(target) is None

        client = client_type()
        client.connect('127.0.0.1', allow_agent=True, look_for_keys=True)
        assert client.connection[1]['allow_agent'] is False
        assert client.connection[1]['look_for_keys'] is False
        assert readers.is_winscp_available() is False
        assert readers.is_filezilla_available() is False
        assert readers.is_nppftp_available() is False


def _test_batch_launcher_errors() -> None:
    launcher = Path(HERE, 'tools', 'ftp_ui_driver.bat')
    with tempfile.TemporaryDirectory(prefix='ftp-ui-launcher-test-') as root:
        root_path = Path(root)
        fixture_launcher = root_path / 'ftp_ui_driver.bat'
        if launcher.is_file():
            shutil.copy2(launcher, fixture_launcher)

        completed = subprocess.run(
            ['cmd', '/c', str(fixture_launcher)], capture_output=True,
            text=True, timeout=10)
        result = json.loads(completed.stdout.strip())
        assert completed.returncode != 0
        assert 'ftp_ui_driver_config.bat' in result['error']

        config = root_path / 'ftp_ui_driver_config.bat'
        config.write_text('set "FMAN_SOURCE_DIR=Z:\\missing-fman"\n',
                          encoding='utf-8')
        completed = subprocess.run(
            ['cmd', '/c', str(fixture_launcher)], capture_output=True,
            text=True, timeout=10)
        result = json.loads(completed.stdout.strip())
        assert completed.returncode != 0
        assert 'Driver script is missing' in result['error']

        driver_dir = root_path / 'ftp_ui_driver'
        driver_dir.mkdir()
        (driver_dir / 'ftp_ui_driver.py').write_text('', encoding='utf-8')
        completed = subprocess.run(
            ['cmd', '/c', str(fixture_launcher)], capture_output=True,
            text=True, timeout=10)
        result = json.loads(completed.stdout.strip())
        assert completed.returncode != 0
        assert 'FMAN_SOURCE_DIR' in result['error']
        assert 'ftp_ui_driver_config.bat' in result['error']


def _test_json_output_escapes_console_glyphs() -> None:
    from tools.ftp_ui_driver import ftp_ui_driver

    output = StringIO()
    with mock.patch('sys.stdout', output):
        ftp_ui_driver._json_result({'button': '−'})
    assert r'\u2212' in output.getvalue()
    assert json.loads(output.getvalue()) == {'button': '−'}


def main() -> None:
    _test_fixtures()
    _test_server_readiness()
    _test_window_helpers()
    _test_probe_isolation()
    _test_profile_staging()
    _test_session_launch_and_cleanup()
    _test_session_close_order()
    _test_driver_commands()
    _test_batch_launcher_errors()
    _test_json_output_escapes_console_glyphs()
    assert _remove_sftp_file(0.0) < 0.3
    assert _remove_sftp_file(0.35) >= 0.3

    if FTP_CASES_AVAILABLE:
        assert _remove_ftp_file(0.0) < 0.3
        assert _remove_ftp_file(0.35) >= 0.3
    else:
        print('SKIP: FTP pacing cases, test-vendor/ is missing')

    print('ok: fixture-server pacing')


if __name__ == '__main__':
    main()
