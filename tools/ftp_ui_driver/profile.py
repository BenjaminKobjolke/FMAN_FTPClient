"""Stage a private fman profile with this plugin and its test probe."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
from typing import Any


PLUGIN_DIRECTORY = 'FMAN_FTPClient'
PROBE_DIRECTORY = 'FTPUiDriverProbe'
CREDENTIALS_FILE = 'credentials.json'
EXCLUDED_PLUGIN_DIRECTORIES = frozenset({
    '.git', 'tickets', 'tmp', 'graphify-out', 'code_analysis_results',
    'test-vendor', 'claude-plans', '__pycache__',
})


def _json_file(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')


def _copy_ignore(_directory: str, names: list[str]) -> set[str]:
    return set(names) & EXCLUDED_PLUGIN_DIRECTORIES


def _credential_target(plugin_source: str | os.PathLike[str], base_url: str) -> str:
    credentials_path = Path(plugin_source) / 'ftpclient' / 'credentials.py'
    spec = importlib.util.spec_from_file_location(
        '_ftp_ui_driver_credentials', credentials_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'Cannot load plugin credentials helper: {credentials_path}')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.credential_target(base_url)


def stage_profile(
    session_root: str | os.PathLike[str],
    plugin_source: str | os.PathLike[str],
    probe_source: str | os.PathLike[str],
    scripting_port: int,
    token: str,
    *,
    ftp_port: int,
    sftp_port: int,
) -> dict[str, Any]:
    """Stage the plugin, probe, pane fixtures, bookmarks and session secrets."""
    root = Path(session_root).resolve()
    profile = root / 'profile'
    local = profile / 'Local'
    user_plugins = profile / 'Plugins' / 'User'
    local.mkdir(parents=True, exist_ok=True)
    user_plugins.mkdir(parents=True, exist_ok=True)

    settings = {
        'single_instance': False,
        'scripting_api': {
            'enabled': True,
            'automation': True,
            'port': scripting_port,
            'command_access': 'all',
            'allowed_hosts': [],
        },
    }
    _json_file(local / 'Settings.json', settings)
    _json_file(local / 'Session.json', {'panes': [{}, {}]})
    token_path = local / 'ScriptingApiToken.txt'
    token_path.write_text(token, encoding='utf-8')

    shutil.copytree(
        plugin_source, user_plugins / PLUGIN_DIRECTORY,
        ignore=_copy_ignore,
    )
    shutil.copytree(
        probe_source, user_plugins / PROBE_DIRECTORY,
        ignore=shutil.ignore_patterns('__pycache__', '*.pyc'),
    )

    ftp_url = f'ftp://user@127.0.0.1:{ftp_port}'
    sftp_url = f'sftp://user@127.0.0.1:{sftp_port}'
    settings_directory = user_plugins / 'Settings'
    _json_file(settings_directory / 'FTP Bookmarks.json', {
        'ftp://ftp-fixture': [ftp_url, '/'],
        'sftp://sftp-fixture': [sftp_url, '/'],
    })
    credentials = {
        _credential_target(plugin_source, ftp_url): 'secret',
        _credential_target(plugin_source, sftp_url): 'secret',
    }
    _json_file(root / CREDENTIALS_FILE, credentials)
    return {
        'profile': str(profile),
        'token_path': str(token_path),
        'left': str(root / 'left'),
        'right': str(root / 'right'),
        'ftp_url': 'ftp://ftp-fixture/',
        'sftp_url': 'sftp://sftp-fixture/',
        'remote_paths': {
            'ftp': 'ftp://ftp-fixture/',
            'sftp': 'sftp://sftp-fixture/',
        },
    }
