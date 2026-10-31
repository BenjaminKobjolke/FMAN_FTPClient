"""Session and CLI test cases for the isolated fman UI driver."""
import json
import os
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))


def _test_profile_staging() -> None:
    from tools.ftp_ui_driver import session

    with tempfile.TemporaryDirectory(prefix='ftp-ui-stage-test-') as root:
        root_path = os.path.abspath(root)
        plugin_source = os.path.join(root_path, 'plugin-source')
        os.makedirs(os.path.join(plugin_source, 'ftpclient'))
        with open(os.path.join(plugin_source, 'ftpclient', 'marker.py'), 'w') as stream:
            stream.write('plugin')
        credentials_path = os.path.join(plugin_source, 'ftpclient', 'credentials.py')
        with open(credentials_path, 'w') as stream:
            stream.write(
                'from urllib.parse import urlparse\n'
                'def credential_target(url):\n'
                '    scheme = urlparse(url).scheme.upper()\n'
                '    return "FMAN_FTPClient:" + scheme + ":" + url\n')
        for name in ('.git', 'tickets', 'tmp', 'graphify-out',
                     'code_analysis_results', 'test-vendor', 'claude-plans',
                     '__pycache__'):
            os.makedirs(os.path.join(plugin_source, name))
            with open(os.path.join(plugin_source, name, 'excluded.txt'), 'w') as stream:
                stream.write('excluded')
        probe_source = os.path.join(root_path, 'probe-source')
        os.makedirs(os.path.join(probe_source, 'ftp_ui_driver_probe'))
        with open(os.path.join(
                probe_source, 'ftp_ui_driver_probe', '__init__.py'), 'w') as stream:
            stream.write('probe')

        session_root = os.path.join(root_path, 'session')
        staged = session.stage_profile(
            session_root, plugin_source, probe_source, 43210, 'token-value',
            ftp_port=2121, sftp_port=2222)
        profile = staged['profile']
        staged_plugin = os.path.join(
            profile, 'Plugins', 'User', 'FMAN_FTPClient')
        assert os.path.isfile(os.path.join(staged_plugin, 'ftpclient', 'marker.py'))
        for name in ('.git', 'tickets', 'tmp', 'graphify-out',
                     'code_analysis_results', 'test-vendor', 'claude-plans',
                     '__pycache__'):
            assert not os.path.exists(os.path.join(staged_plugin, name)), name
        assert os.path.isfile(os.path.join(
            profile, 'Plugins', 'User', 'FTPUiDriverProbe',
            'ftp_ui_driver_probe', '__init__.py'))

        with open(os.path.join(profile, 'Local', 'Settings.json')) as stream:
            settings = json.load(stream)
        assert settings['single_instance'] is False
        assert settings['scripting_api']['enabled'] is True
        assert settings['scripting_api']['command_access'] == 'all'
        with open(os.path.join(profile, 'Local', 'Session.json')) as stream:
            assert json.load(stream) == {'panes': [{}, {}]}
        with open(staged['token_path']) as stream:
            assert stream.read() == 'token-value'

        ftp_url = 'ftp://user@127.0.0.1:2121'
        sftp_url = 'sftp://user@127.0.0.1:2222'
        bookmarks_path = os.path.join(
            profile, 'Plugins', 'User', 'Settings', 'FTP Bookmarks.json')
        with open(bookmarks_path) as stream:
            bookmarks = json.load(stream)
        assert bookmarks == {
            'ftp://ftp-fixture': [ftp_url, '/'],
            'sftp://sftp-fixture': [sftp_url, '/'],
        }
        with open(os.path.join(session_root, 'credentials.json')) as stream:
            credentials = json.load(stream)
        assert credentials == {
            'FMAN_FTPClient:FTP:' + ftp_url: 'secret',
            'FMAN_FTPClient:SFTP:' + sftp_url: 'secret',
        }


def _test_session_launch_and_cleanup() -> None:
    from tools.ftp_ui_driver import session

    with tempfile.TemporaryDirectory(prefix='ftp-ui-launch-test-') as root:
        root_path = os.path.abspath(root)
        fman_source = os.path.join(root_path, 'fman')
        os.makedirs(os.path.join(fman_source, 'tools', 'fman_ui'))
        os.makedirs(os.path.join(fman_source, 'src', 'main', 'python', 'fman'))
        with open(os.path.join(
                fman_source, 'tools', 'fman_ui', 'fman_ui.py'), 'w') as stream:
            stream.write('# driver')
        fman_main = os.path.join(
            fman_source, 'src', 'main', 'python', 'fman', 'main.py')
        with open(fman_main, 'w') as stream:
            stream.write('# fman')

        monkeypatch = mock.patch
        driver_root = Path(root_path, 'driver')
        with monkeypatch.object(session, 'driver_root', return_value=driver_root):
            with monkeypatch.object(
                    session, 'fman_source_dir', return_value=Path(fman_source)):
                with monkeypatch.object(session, '_wait_for_servers', return_value={
                        'pid': 101, 'ftp_port': 2121, 'sftp_port': 2222,
                        'ftp_error': None}):
                    with monkeypatch.object(
                            session, '_wait_for_state', return_value={'ok': True}):
                        with monkeypatch.object(
                                session,
                                '_probe_windows',
                                return_value={'windows': []},
                        ):
                            class FakeProcess:
                                next_pid = 100

                                def __init__(self, command, **kwargs):
                                    self.pid = FakeProcess.next_pid
                                    FakeProcess.next_pid += 1
                                    self.command = command
                                    self.kwargs = kwargs
                                    self.killed = False

                                def poll(self):
                                    return None

                                def kill(self):
                                    self.killed = True

                                def wait(self, timeout=None):
                                    return 0

                            processes = []

                            def fake_popen(command, **kwargs):
                                process = FakeProcess(command, **kwargs)
                                processes.append(process)
                                return process

                            with monkeypatch.object(
                                    session.subprocess,
                                    'Popen',
                                    side_effect=fake_popen,
                            ):
                                result = session.launch(delay_seconds=0.25)

        current_path = os.path.join(root_path, 'driver', 'fman-ui', 'current.json')
        with open(current_path) as stream:
            current = json.load(stream)
        assert set(('session_id', 'pid', 'port', 'token_path', 'profile',
                    'left', 'right', 'servers_pid', 'ftp_url', 'sftp_url',
                    'remote_paths', 'ftp_error', 'delay_seconds')) <= set(current)
        assert result['session_id'] == current['session_id']
        assert current['pid'] == processes[1].pid
        assert current['servers_pid'] == 101
        assert current['ftp_url'] == 'ftp://ftp-fixture/'
        assert current['sftp_url'] == 'sftp://sftp-fixture/'
        fman_environment = processes[1].kwargs['env']
        assert fman_environment['FMAN_DATA_DIRECTORY'] == current['profile']
        session_root = os.path.dirname(current['profile'])
        assert fman_environment['FTP_UI_DRIVER_SESSION'] == session_root
        python_path_suffix = os.path.join('src', 'main', 'python')
        assert fman_environment['PYTHONPATH'].endswith(python_path_suffix)
        assert processes[1].kwargs['cwd'] == fman_source

    with tempfile.TemporaryDirectory(prefix='ftp-ui-launch-failure-') as root:
        driver_path = Path(root, 'driver')
        current_path = os.path.join(driver_path, 'fman-ui', 'current.json')
        os.makedirs(os.path.dirname(current_path))
        with open(current_path, 'w') as stream:
            stream.write('{}')
        with mock.patch.object(session, 'driver_root', return_value=driver_path):
            try:
                session.launch()
            except RuntimeError as error:
                assert 'already active' in str(error).lower()
            else:
                raise AssertionError('launch should refuse an existing session')

    with tempfile.TemporaryDirectory(prefix='ftp-ui-launch-cleanup-') as root:
        driver_path = Path(root, 'driver')
        fman_source = Path(root, 'fman')
        (fman_source / 'tools' / 'fman_ui').mkdir(parents=True)
        (fman_source / 'src' / 'main' / 'python' / 'fman').mkdir(parents=True)
        (fman_source / 'tools' / 'fman_ui' / 'fman_ui.py').write_text('# driver')
        fman_main = fman_source / 'src' / 'main' / 'python' / 'fman' / 'main.py'
        fman_main.write_text('# fman')

        class FailedProcess:
            next_pid = 200

            def __init__(self, command, **kwargs):
                self.pid = FailedProcess.next_pid
                FailedProcess.next_pid += 1
                self.command = command
                self.kwargs = kwargs
                self.killed = False

            def poll(self):
                return None

            def kill(self):
                self.killed = True

            def wait(self, timeout=None):
                return 0

        processes = []
        killed = []

        def failed_popen(command, **kwargs):
            process = FailedProcess(command, **kwargs)
            processes.append(process)
            return process

        def mark_killed(process):
            process.killed = True
            killed.append(process.pid)

        with mock.patch.object(
                session, 'driver_root', return_value=driver_path), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=fman_source), \
                mock.patch.object(session, '_wait_for_servers', return_value={
                    'pid': 200, 'ftp_port': None, 'sftp_port': 2222,
                    'ftp_error': 'FTP unavailable'}), \
                mock.patch.object(
                    session, '_wait_for_state',
                    side_effect=RuntimeError('startup failure')), \
                mock.patch.object(
                    session, '_kill_process', side_effect=mark_killed), \
                mock.patch.object(
                    session.subprocess, 'Popen', side_effect=failed_popen):
            try:
                session.launch(delay_seconds=0)
            except RuntimeError as error:
                assert 'startup failure' in str(error)
            else:
                raise AssertionError('failed startup should be reported')

        failed_session_root = Path(processes[0].command[2])
        assert len(killed) == 2
        assert all(process.killed for process in processes)
        assert not failed_session_root.exists()
        assert not (driver_path / 'fman-ui' / 'current.json').exists()


def _test_session_close_order() -> None:
    from tools.ftp_ui_driver import session

    with tempfile.TemporaryDirectory(prefix='ftp-ui-close-test-') as root:
        driver_path = Path(root, 'driver')
        marker = driver_path / 'fman-ui' / 'current.json'
        session_root = marker.parent / 'fixture-session'
        profile = session_root / 'profile'
        session_root.mkdir(parents=True)
        profile.mkdir()
        current = {
            'session_id': 'fixture-session', 'pid': 555, 'servers_pid': 444,
            'port': 1234, 'token_path': str(profile / 'Local' / 'token'),
            'profile': str(profile), 'left': str(session_root / 'left'),
            'right': str(session_root / 'right'),
        }
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(json.dumps(current), encoding='utf-8')
        events = []
        taskkill = mock.Mock(return_value=SimpleNamespace(returncode=128))

        def delegated(_fman_source, *arguments):
            events.append(('delegate', arguments))
            shutil.rmtree(session_root, ignore_errors=True)
            marker.unlink(missing_ok=True)
            return {'ok': True}

        with mock.patch.object(session, 'current_path', return_value=marker), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=Path(root)), \
                mock.patch.object(
                    session.subprocess,
                    'run',
                    side_effect=lambda *args, **kwargs: (
                        events.append(('taskkill', args[0]))
                        or taskkill.return_value),
                ), \
                mock.patch.object(session, '_run_driver', side_effect=delegated):
            result = session.close()

        assert result == {'ok': True}
        assert [event[0] for event in events] == ['taskkill', 'delegate']
        assert events[0][1] == ['taskkill', '/PID', '444', '/T', '/F']
        assert events[1][1] == ('close',)
        assert not session_root.exists()
        assert not marker.exists()
