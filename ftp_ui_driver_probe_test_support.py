"""Probe, window, and launcher tests for the isolated fman UI driver."""
import json
import os
import shutil
import subprocess
import tempfile
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from ftp_ui_driver_test_support import HERE

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
