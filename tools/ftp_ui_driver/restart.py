"""Restart fman of the active FTP UI session, keeping profile and fixture servers."""

import subprocess
import time
from pathlib import Path
from typing import Any

from . import session

QUIT_COMMAND = 'quit'
QUIT_TIMEOUT_SECONDS = 15


def _process_running(pid: int) -> bool:
    completed = subprocess.run(
        ['tasklist', '/FI', f'PID eq {pid}', '/NH', '/FO', 'CSV'],
        capture_output=True, text=True, check=False,
    )
    return f'"{pid}"' in completed.stdout


def _wait_for_exit(pid: int, timeout: float = QUIT_TIMEOUT_SECONDS) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_running(pid):
            return
        time.sleep(0.25)
    if _process_running(pid):
        # shortcut: pid is not checked against its start time, so a pid reused within
        # the wait could be killed; upgrade to psutil identity if that ever happens.
        session._taskkill(pid)


def restart() -> dict[str, Any]:
    """Restart fman while retaining the active profile and fixture servers."""
    current = session.read_current()
    fman_source = session.fman_source_dir()
    session_root = Path(current['profile']).parent
    try:
        session._run_driver(fman_source, 'command', QUIT_COMMAND, '{}')
    except (OSError, RuntimeError):
        # fman may close the scripting connection while processing quit.
        pass
    _wait_for_exit(current['pid'])
    process = session._start_fman(fman_source, current, session_root)
    try:
        session._wait_for_state(
            fman_source, process, session_root / session.FMAN_LOG_FILE)
        session._probe_windows(fman_source, session_root)
    except Exception as error:
        session._kill_process(process)
        raise RuntimeError(
            f'fman did not restart: {error}; run close to end the session') from error
    return {
        'restarted': True, **session.session_result(current), 'pid': current['pid'],
    }
