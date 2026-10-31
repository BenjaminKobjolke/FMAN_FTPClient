"""Build and manage one private fman profile for the desktop UI driver."""

import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import uuid
from typing import Any
from .profile import stage_profile, _json_file

DRIVER_DIRECTORY = 'fman-ftp-ui'
FMAN_UI_DIRECTORY = 'fman-ui'


def driver_root() -> Path:
    """Return the private driver root under the current user's temp folder."""
    return Path(os.environ.get('TEMP', Path.home())) / DRIVER_DIRECTORY


def fman_source_dir() -> Path:
    """Return the configured fman checkout path."""
    source = os.environ.get('FMAN_SOURCE_DIR')
    if not source:
        raise RuntimeError('FMAN_SOURCE_DIR is not configured')
    return Path(source).expanduser().resolve()


def current_path() -> Path:
    """Return the fman UI driver's active-session marker path."""
    return driver_root() / FMAN_UI_DIRECTORY / 'current.json'


def _reserve_port() -> int:
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


def _python_executable() -> str:
    return os.environ.get('FMAN_PYTHON', sys.executable)


def _delegate(
    fman_source: Path, arguments: list[str],
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment['TEMP'] = str(driver_root())
    environment['TMP'] = str(driver_root())
    return subprocess.run(
        [_python_executable(), str(fman_source / 'tools' / 'fman_ui' / 'fman_ui.py'),
         *arguments],
        cwd=str(fman_source), env=environment, capture_output=True, text=True,
        check=False,
    )


def _wait_for_servers(
    process: subprocess.Popen[bytes], session_root: Path, timeout: float = 30,
) -> dict[str, Any]:
    marker = session_root / 'servers.json'
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if marker.is_file():
            return json.loads(marker.read_text(encoding='utf-8'))
        if process.poll() is not None:
            raise RuntimeError('fixture server process exited before readiness')
        time.sleep(0.1)
    raise RuntimeError('fixture servers did not become ready within 30 seconds')


def _run_driver(fman_source: Path, *arguments: str) -> dict[str, Any]:
    completed = _delegate(fman_source, list(arguments))
    if completed.returncode != 0:
        raise RuntimeError(completed.stdout.strip() or completed.stderr.strip())
    try:
        result = json.loads(completed.stdout)
    except ValueError as error:
        raise RuntimeError('fman UI driver returned invalid JSON') from error
    if isinstance(result, dict) and result.get('error'):
        raise RuntimeError(str(result['error']))
    return result


def _wait_for_state(
    fman_source: Path, process: subprocess.Popen[bytes], log_path: Path,
    timeout: float = 60,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error = ''
    while time.monotonic() < deadline:
        try:
            return _run_driver(fman_source, 'state')
        except (OSError, RuntimeError, ValueError) as error:
            last_error = str(error)
            if process.poll() is not None:
                break
            time.sleep(0.25)
    tail = log_path.read_text(encoding='utf-8', errors='replace').splitlines()[-40:]
    reason = 'fman did not start'
    if last_error:
        reason += f': {last_error}'
    raise RuntimeError(reason + '; log tail: ' + '\n'.join(tail))


def _probe_windows(fman_source: Path, session_root: Path) -> dict[str, Any]:
    result_path = session_root / 'probe-result.json'
    result_path.unlink(missing_ok=True)
    _run_driver(fman_source, 'command', 'ftp_ui_driver_windows', '{}')
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if result_path.is_file():
            result = json.loads(result_path.read_text(encoding='utf-8'))
            if result.get('error'):
                raise RuntimeError(str(result['error']))
            return result
        time.sleep(0.05)
    raise RuntimeError('fman UI probe did not write its windows result')


def _kill_process(process: subprocess.Popen[bytes] | None) -> None:
    if process is None or process.poll() is not None:
        return
    if os.name == 'nt':
        subprocess.run(
            ['taskkill', '/PID', str(process.pid), '/T', '/F'],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    else:
        process.kill()
    try:
        process.wait(timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass


def launch(delay_seconds: float = 1.0) -> dict[str, Any]:
    """Start fixture servers and fman with a session-only profile."""
    if delay_seconds < 0:
        raise ValueError('delay must be non-negative')
    marker = current_path()
    if marker.exists():
        raise RuntimeError(f'An fman FTP UI session is already active: {marker}')

    fman_source = fman_source_dir()
    fman_main = fman_source / 'src' / 'main' / 'python' / 'fman' / 'main.py'
    fman_driver = fman_source / 'tools' / 'fman_ui' / 'fman_ui.py'
    if not fman_main.is_file() or not fman_driver.is_file():
        raise RuntimeError(f'Invalid fman checkout: {fman_source}')

    root = driver_root()
    root.mkdir(parents=True, exist_ok=True)
    fman_root = root / FMAN_UI_DIRECTORY
    fman_root.mkdir(parents=True, exist_ok=True)
    session_id = uuid.uuid4().hex
    session_root = fman_root / session_id
    server_process: subprocess.Popen[bytes] | None = None
    fman_process: subprocess.Popen[bytes] | None = None
    try:
        from . import fixtures

        fixtures.seed(session_root)
        server_script = Path(__file__).with_name('servers.py')
        server_environment = os.environ.copy()
        server_process = subprocess.Popen(
            [_python_executable(), str(server_script), str(session_root),
             str(delay_seconds)],
            cwd=str(fman_source), env=server_environment,
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        servers = _wait_for_servers(server_process, session_root)
        port = _reserve_port()
        staged = stage_profile(
            session_root, Path(__file__).resolve().parents[2],
            Path(__file__).with_name('FTPUiDriverProbe'), port, uuid.uuid4().hex,
            ftp_port=servers['ftp_port'] or 0,
            sftp_port=servers['sftp_port'],
        )
        session: dict[str, Any] = {
            'session_id': session_id,
            'pid': None,
            'port': port,
            'token_path': staged['token_path'],
            'profile': staged['profile'],
            'left': staged['left'],
            'right': staged['right'],
            'servers_pid': servers['pid'],
            'ftp_url': staged['ftp_url'],
            'sftp_url': staged['sftp_url'],
            'remote_paths': staged['remote_paths'],
            'ftp_error': servers['ftp_error'],
            'delay_seconds': delay_seconds,
        }
        environment = os.environ.copy()
        environment['FMAN_DATA_DIRECTORY'] = staged['profile']
        environment['PYTHONPATH'] = str(fman_source / 'src' / 'main' / 'python')
        environment['FTP_UI_DRIVER_SESSION'] = str(session_root)
        flags = (
            getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0)
            | getattr(subprocess, 'DETACHED_PROCESS', 0)
        )
        log_path = session_root / 'fman.log'
        with log_path.open('wb') as log:
            fman_process = subprocess.Popen(
                [_python_executable(), str(fman_main), staged['left'], staged['right']],
                cwd=str(fman_source), env=environment,
                stdout=log, stderr=subprocess.STDOUT, creationflags=flags,
            )
        session['pid'] = fman_process.pid
        _json_file(marker, session)
        _wait_for_state(fman_source, fman_process, log_path)
        _probe_windows(fman_source, session_root)
        return {
            'session_id': session_id,
            'left': staged['left'],
            'right': staged['right'],
            'ftp_url': staged['ftp_url'],
            'sftp_url': staged['sftp_url'],
            'ftp_error': servers['ftp_error'],
            'delay_seconds': delay_seconds,
        }
    except Exception:
        _kill_process(fman_process)
        _kill_process(server_process)
        shutil.rmtree(session_root, ignore_errors=True)
        marker.unlink(missing_ok=True)
        raise


def close() -> dict[str, Any]:
    """Stop the fixture server, then let fman's driver close and clean fman."""
    marker = current_path()
    if not marker.is_file():
        raise RuntimeError('No active fman FTP UI session')
    current = json.loads(marker.read_text(encoding='utf-8'))
    session_root = Path(current['profile']).parent
    server_pid = current.get('servers_pid')
    if server_pid is not None:
        subprocess.run(
            ['taskkill', '/PID', str(server_pid), '/T', '/F'],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    result = _run_driver(fman_source_dir(), 'close')
    shutil.rmtree(session_root, ignore_errors=True)
    marker.unlink(missing_ok=True)
    return result
