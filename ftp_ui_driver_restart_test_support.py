"""Focused test cases for restarting the isolated fman UI driver."""
import contextlib
import io
import json
import os
import tempfile
from pathlib import Path
from unittest import mock


def _test_session_restart() -> None:
    from tools.ftp_ui_driver import restart, session

    with tempfile.TemporaryDirectory(prefix='ftp-ui-restart-test-') as root:
        root_path = Path(root)
        marker_path = root_path / 'driver' / 'fman-ui' / 'current.json'
        session_root = marker_path.parent / 'fixture-session'
        profile = session_root / 'profile'
        session_root.mkdir(parents=True)
        profile.mkdir()
        fman_source = root_path / 'fman'
        fman_main = fman_source / 'src' / 'main' / 'python' / 'fman' / 'main.py'
        fman_main.parent.mkdir(parents=True)
        fman_main.write_text('# fman', encoding='utf-8')
        current = {
            'session_id': 'fixture-session', 'pid': 555, 'servers_pid': 444,
            'port': 1234, 'token_path': str(profile / 'Local' / 'token'),
            'profile': str(profile), 'left': str(session_root / 'left'),
            'right': str(session_root / 'right'), 'ftp_url': 'ftp://ftp-fixture/',
            'sftp_url': 'sftp://sftp-fixture/', 'ftp_error': None,
            'delay_seconds': 0.0,
        }
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_path.write_text(json.dumps(current), encoding='utf-8')
        events = []

        class FakeProcess:
            pid = 777

        def delegated(_fman_source, *arguments):
            events.append(('driver', arguments))
            return {}

        def fake_popen(command, **kwargs):
            events.append(('popen', command, kwargs))
            return FakeProcess()

        tasklist = mock.Mock(return_value=None)
        with mock.patch.object(session, 'current_path', return_value=marker_path), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=fman_source), \
                mock.patch.object(session, '_run_driver', side_effect=delegated), \
                mock.patch.object(
                    restart, '_wait_for_exit',
                    side_effect=lambda pid: events.append(('wait', pid))), \
                mock.patch.object(
                    session.subprocess, 'Popen', side_effect=fake_popen), \
                mock.patch.object(session, '_wait_for_state', return_value={}), \
                mock.patch.object(session, '_probe_windows', return_value={}), \
                mock.patch.object(session.subprocess, 'run', tasklist):
            result = restart.restart()

        assert [event[0] for event in events[:3]] == ['driver', 'wait', 'popen']
        assert events[0] == ('driver', ('command', 'quit', '{}'))
        assert events[1] == ('wait', 555)
        assert not any(event[0] == 'taskkill' and '444' in event[1]
                       for event in events)
        _command, command, options = events[2]
        assert command[-2:] == [current['left'], current['right']]
        assert options['env']['FMAN_DATA_DIRECTORY'] == str(profile)
        assert options['env']['FTP_UI_DRIVER_SESSION'] == str(session_root)
        assert options['env']['PYTHONPATH'].endswith(
            os.path.join('src', 'main', 'python'))
        saved = json.loads(marker_path.read_text(encoding='utf-8'))
        assert saved['pid'] == 777
        assert {key: value for key, value in saved.items() if key != 'pid'} == {
            key: value for key, value in current.items() if key != 'pid'
        }
        assert session_root.is_dir()
        assert result == {
            'restarted': True, **session.session_result(saved), 'pid': 777}
        assert tasklist.call_count == 0

    with tempfile.TemporaryDirectory(prefix='ftp-ui-restart-quit-test-') as root:
        marker = Path(root) / 'current.json'
        session_root = Path(root) / 'session'
        profile = session_root / 'profile'
        profile.mkdir(parents=True)
        marker.write_text(json.dumps({
            'session_id': 's', 'pid': 555, 'profile': str(profile),
            'servers_pid': 444, 'left': str(session_root / 'left'),
            'right': str(session_root / 'right'), 'ftp_url': 'ftp://ftp-fixture/',
            'sftp_url': 'sftp://sftp-fixture/', 'ftp_error': None,
            'delay_seconds': 0.0,
        }), encoding='utf-8')
        class FakeProcess:
            pid = 777
        with mock.patch.object(session, 'current_path', return_value=marker), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=Path(root)), \
                mock.patch.object(session, '_run_driver',
                                  side_effect=RuntimeError('connection reset')), \
                mock.patch.object(restart, '_wait_for_exit'), \
                mock.patch.object(session, '_start_fman', return_value=FakeProcess()), \
                mock.patch.object(session, '_wait_for_state', return_value={}), \
                mock.patch.object(session, '_probe_windows', return_value={}):
            assert restart.restart()['restarted'] is True


def _test_restart_startup_failure() -> None:
    from tools.ftp_ui_driver import restart, session

    with tempfile.TemporaryDirectory(prefix='ftp-ui-restart-fail-test-') as root:
        root_path = Path(root)
        marker = root_path / 'current.json'
        session_root = root_path / 'session'
        profile = session_root / 'profile'
        profile.mkdir(parents=True)
        current = {'session_id': 's', 'pid': 555, 'servers_pid': 444,
                   'profile': str(profile)}
        marker.write_text(json.dumps(current), encoding='utf-8')

        class FakeProcess:
            pid = 777

        killed = []
        with mock.patch.object(session, 'current_path', return_value=marker), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=root_path), \
                mock.patch.object(session, '_run_driver', return_value={}), \
                mock.patch.object(restart, '_wait_for_exit'), \
                mock.patch.object(session, '_start_fman', return_value=FakeProcess()), \
                mock.patch.object(session, '_wait_for_state',
                                  side_effect=RuntimeError('startup failure')), \
                mock.patch.object(session, '_kill_process',
                                  side_effect=lambda process: killed.append(
                                      process.pid)):
            try:
                restart.restart()
            except RuntimeError as error:
                assert 'startup failure' in str(error) and 'close' in str(error)
            else:
                raise AssertionError('startup failure should be reported')

        assert killed == [777]
        assert json.loads(marker.read_text(encoding='utf-8')) == current
        assert session_root.is_dir()

        with mock.patch.object(
                session, 'current_path', return_value=root_path / 'missing.json'):
            try:
                restart.restart()
            except RuntimeError as error:
                assert str(error) == 'No active fman FTP UI session'
            else:
                raise AssertionError('missing session marker should be reported')


def _test_wait_for_exit() -> None:
    from tools.ftp_ui_driver import restart, session

    with mock.patch.object(restart.subprocess, 'run', side_effect=[
            mock.Mock(stdout='"fman.exe","555"'),
            mock.Mock(stdout=(
                'INFO: No tasks are running which match the specified criteria.')),
    ]) as run, mock.patch.object(restart.time, 'sleep') as sleep:
        restart._wait_for_exit(555)
    assert run.call_count == 2
    assert run.call_args_list[0].args[0] == [
        'tasklist', '/FI', 'PID eq 555', '/NH', '/FO', 'CSV']
    assert sleep.call_count == 1

    with mock.patch.object(restart, '_process_running', return_value=True), \
            mock.patch.object(session, '_taskkill') as taskkill, \
            mock.patch.object(restart.time, 'monotonic', side_effect=[0.0, 1.0]):
        restart._wait_for_exit(555, timeout=0.1)
    taskkill.assert_called_once_with(555)


def _test_restart_command() -> None:
    from tools.ftp_ui_driver import ftp_ui_driver, restart

    output = io.StringIO()
    with mock.patch.object(restart, 'restart', return_value={'restarted': True}), \
            contextlib.redirect_stdout(output):
        assert ftp_ui_driver.main(['restart']) == 0
    assert output.getvalue() == '{"restarted": true}\n'

    output = io.StringIO()
    with mock.patch.object(restart, 'restart', side_effect=RuntimeError('boom')), \
            contextlib.redirect_stdout(output):
        assert ftp_ui_driver.main(['restart']) == 1
    assert output.getvalue() == '{"error": "boom"}\n'
