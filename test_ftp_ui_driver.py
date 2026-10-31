"""Focused tests for fixture-server pacing used by the desktop UI driver."""
import ftplib
import os
import subprocess
import sys
import tempfile
import time
from ftp_ui_driver_cli_test_support import _test_driver_commands
from ftp_ui_driver_probe_test_support import (
    _test_batch_launcher_errors,
    _test_json_output_escapes_console_glyphs,
    _test_probe_isolation,
    _test_window_helpers,
)
from ftp_ui_driver_test_support import (
    HERE,
    _test_profile_staging,
    _test_session_close_order,
    _test_session_launch_and_cleanup,
)


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
