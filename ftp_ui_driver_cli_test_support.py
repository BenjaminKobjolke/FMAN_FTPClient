"""CLI tests for the isolated fman UI driver."""
import json
import tempfile
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
def _test_driver_commands() -> None:
    from tools.ftp_ui_driver import ftp_ui_driver as driver
    from tools.ftp_ui_driver import session

    with tempfile.TemporaryDirectory(prefix='ftp-ui-cli-test-') as root:
        root_path = Path(root)
        fman_source = root_path / 'fman'
        fman_source.mkdir()
        driver_root = root_path / 'driver'
        output = StringIO()
        error_output = StringIO()
        completed = SimpleNamespace(
            returncode=7, stdout='delegated stdout\n', stderr='delegated stderr\n')

        with mock.patch.object(session, 'driver_root', return_value=driver_root), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=fman_source), \
                mock.patch.object(
                    session.subprocess, 'run', return_value=completed) as run, \
                mock.patch('sys.stdout', output), \
                mock.patch('sys.stderr', error_output):
            code = driver.main(['state'])
            assert code == 7
            assert output.getvalue() == completed.stdout
            assert error_output.getvalue() == completed.stderr
            command, = run.call_args.args
            assert command[1] == str(fman_source / 'tools' / 'fman_ui' / 'fman_ui.py')
            assert command[2:] == ['state']
            assert run.call_args.kwargs['env']['TEMP'] == str(driver_root)
            assert run.call_args.kwargs['env']['TMP'] == str(driver_root)

        for arguments, expected in (
                (['keys', 'F5'], ['keys', 'F5']),
                (['command', 'ftp_ui_driver_windows', '{"verbose":true}'],
                 ['command', 'ftp_ui_driver_windows', '{"verbose":true}'])):
            with mock.patch.object(
                    session, 'fman_source_dir', return_value=fman_source), \
                    mock.patch.object(
                        session, 'driver_root', return_value=driver_root), \
                    mock.patch.object(
                        session.subprocess, 'run', return_value=completed) as run, \
                    mock.patch('sys.stdout', StringIO()), \
                    mock.patch('sys.stderr', StringIO()):
                assert driver.main(arguments) == 7
                assert run.call_args.args[0][2:] == expected

        for arguments in (['launch', '--left', 'C:/tmp'],
                          ['launch', '--right', 'C:/tmp']):
            with mock.patch('sys.stdout', StringIO()), \
                    mock.patch('sys.stderr', StringIO()):
                code = driver.main(arguments)
            assert code == 1, 'launch pane overrides must be rejected'

        invalid_output = StringIO()
        with mock.patch('sys.stdout', invalid_output), \
                mock.patch('sys.stderr', StringIO()):
            assert driver.main(['launch', '--delay', '-1']) == 1
        assert json.loads(invalid_output.getvalue()) == {
            'error': 'delay must be non-negative'}

        marker = driver_root / 'fman-ui' / 'current.json'
        session_root = root_path / 'fixture-session'
        profile = session_root / 'profile'
        profile.mkdir(parents=True)
        marker.parent.mkdir(parents=True)
        marker.write_text(json.dumps({'profile': str(profile)}), encoding='utf-8')

        def write_probe_result(_fman_source, arguments):
            assert arguments == [
                'command', 'ftp_ui_driver_window_click',
                '{"title":"Deleting folder","button":"Cancel"}']
            (session_root / 'probe-result.json').write_text(
                '{"ok":true,"clicked":"Cancel"}\n', encoding='utf-8')
            return SimpleNamespace(returncode=0, stdout='{}', stderr='')

        probe_output = StringIO()
        with mock.patch.object(session, 'current_path', return_value=marker), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=fman_source), \
                mock.patch.object(
                    session, '_delegate', side_effect=write_probe_result), \
                mock.patch('sys.stdout', probe_output), \
                mock.patch('sys.stderr', StringIO()):
            assert driver.main([
                'window-click', '--title', 'Deleting folder',
                '--button', 'Cancel']) == 0
        assert json.loads(probe_output.getvalue()) == {
            'ok': True, 'clicked': 'Cancel'}

        with mock.patch.object(session, 'current_path', return_value=marker), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=fman_source), \
                mock.patch.object(
                    driver, '_probe', return_value={'ok': True}) as probe, \
                mock.patch('sys.stdout', StringIO()), \
                mock.patch('sys.stderr', StringIO()):
            for arguments, command_name, expected in (
                    (['windows'], 'ftp_ui_driver_windows', {}),
                    (['minimized-click', '--title', 'Copying upload'],
                     'ftp_ui_driver_minimized_click', {'title': 'Copying upload'}),
                    (['window-screenshot', '--path', 'C:/tmp/window.png'],
                     'ftp_ui_driver_window_screenshot',
                     {'path': 'C:/tmp/window.png', 'title': None})):
                assert driver.main(arguments) == 0
                assert probe.call_args.args[:2] == (command_name, expected)

        with mock.patch.object(session, 'current_path', return_value=marker), \
                mock.patch.object(
                    session, 'fman_source_dir', return_value=fman_source), \
                mock.patch.object(
                    session, 'close', return_value={'ok': True}) as close, \
                mock.patch('sys.stdout', StringIO()), \
                mock.patch('sys.stderr', StringIO()):
            assert driver.main(['close']) == 0
            close.assert_called_once_with()

        remote_root = session_root / 'remote' / 'ftp'
        (remote_root / 'z-folder').mkdir(parents=True)
        (remote_root / 'z-folder' / 'last.txt').write_text('last')
        (remote_root / 'first.txt').write_text('first')
        with mock.patch.object(session, 'current_path', return_value=marker), \
                mock.patch('sys.stdout', StringIO()) as output, \
                mock.patch('sys.stderr', StringIO()):
            assert driver.main(['server-files', 'ftp']) == 0
        assert json.loads(output.getvalue()) == {
            'protocol': 'ftp', 'files': ['first.txt', 'z-folder/last.txt']}

        with mock.patch.object(session, 'current_path', return_value=marker), \
                mock.patch('sys.stdout', StringIO()) as output, \
                mock.patch('sys.stderr', StringIO()):
            assert driver.main(['reset']) == 0
        reset = json.loads(output.getvalue())
        assert reset['ok'] is True
        assert 'reload the pane' in reset['message'].lower()
        assert 'left/upload/file01.txt' in reset['paths']
