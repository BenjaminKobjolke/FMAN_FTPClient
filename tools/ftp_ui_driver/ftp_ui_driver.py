"""Isolated fman FTP desktop driver.

Commands: launch [--delay SECONDS], reset, server-files ftp|sftp, windows,
window-click, minimized-click, window-screenshot, close, state, commands,
dialog, command, files, keys, type, click and screenshot.
"""

import argparse
import importlib.util
import json
from pathlib import Path
import sys
import time
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.ftp_ui_driver import fixtures, session  # noqa: E402

_CONSTANTS_PATH = (
    Path(__file__).parent / 'FTPUiDriverProbe' / 'ftp_ui_driver_probe' /
    'constants.py'
)
_CONSTANTS_SPEC = importlib.util.spec_from_file_location(
    '_ftp_ui_driver_probe_constants', _CONSTANTS_PATH)
if _CONSTANTS_SPEC is None or _CONSTANTS_SPEC.loader is None:
    raise RuntimeError(f'Could not load probe constants from {_CONSTANTS_PATH}')
constants = importlib.util.module_from_spec(_CONSTANTS_SPEC)
_CONSTANTS_SPEC.loader.exec_module(constants)


GENERIC_COMMANDS = frozenset({
    'state', 'commands', 'dialog', 'command', 'files', 'keys', 'type',
    'click', 'screenshot',
})
PROBE_COMMANDS = {
    'windows': (constants.WINDOWS_COMMAND, lambda _args: {}),
    'window-click': (
        constants.WINDOW_CLICK_COMMAND,
        lambda args: {'title': args.title, 'button': args.button},
    ),
    'minimized-click': (
        constants.MINIMIZED_CLICK_COMMAND,
        lambda args: {'title': args.title},
    ),
    'window-screenshot': (
        constants.WINDOW_SCREENSHOT_COMMAND,
        lambda args: {'path': args.path, 'title': args.title},
    ),
}


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(prog='ftp_ui_driver', add_help=False)
    commands = parser.add_subparsers(dest='action', required=True)
    launch = commands.add_parser('launch', add_help=False)
    launch.add_argument('--delay', type=float, default=1.0)
    commands.add_parser('reset', add_help=False)
    server_files = commands.add_parser('server-files', add_help=False)
    server_files.add_argument('protocol', choices=fixtures.PROTOCOLS)
    commands.add_parser('windows', add_help=False)
    window_click = commands.add_parser('window-click', add_help=False)
    window_click.add_argument('--title', required=True)
    window_click.add_argument('--button', required=True)
    minimized_click = commands.add_parser('minimized-click', add_help=False)
    minimized_click.add_argument('--title', required=True)
    screenshot = commands.add_parser('window-screenshot', add_help=False)
    screenshot.add_argument('--path', required=True)
    screenshot.add_argument('--title')
    commands.add_parser('close', add_help=False)
    return parser


def _json_result(result: Any) -> None:
    sys.stdout.write(json.dumps(result) + '\n')


def _session_root() -> Path:
    marker = session.current_path()
    if not marker.is_file():
        raise RuntimeError('No active fman FTP UI session')
    current = json.loads(marker.read_text(encoding='utf-8'))
    return Path(current['profile']).parent


def _reset() -> dict[str, Any]:
    root = _session_root()
    paths = fixtures.seed(root)
    return {
        'ok': True,
        'paths': paths,
        'message': 'Fixtures reset; reload the pane to see the seeded files.',
    }


def _server_files(protocol: str) -> dict[str, Any]:
    return {
        'protocol': protocol,
        'files': fixtures.server_files(_session_root(), protocol),
    }


def _probe(
    command_name: str, arguments: dict[str, Any], timeout: float = 5.0,
) -> dict[str, Any]:
    root = _session_root()
    result_path = root / constants.PROBE_RESULT_FILE
    result_path.unlink(missing_ok=True)
    completed = session._delegate(
        session.fman_source_dir(),
        ['command', command_name, json.dumps(arguments, separators=(',', ':'))],
    )
    if completed.returncode != 0:
        detail = completed.stdout.strip() or completed.stderr.strip()
        raise RuntimeError(detail or 'fman UI command failed')
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result_path.is_file():
            result = json.loads(result_path.read_text(encoding='utf-8'))
            if not result.get('ok', False):
                raise RuntimeError(str(result.get('error', 'fman UI probe failed')))
            return result
        time.sleep(0.05)
    raise RuntimeError('fman UI probe did not write its result within five seconds')


def _dispatch_custom(arguments: list[str]) -> int:
    args = _parser().parse_args(arguments)
    if args.action == 'launch':
        if args.delay < 0:
            raise ValueError('delay must be non-negative')
        _json_result(session.launch(args.delay))
    elif args.action == 'reset':
        _json_result(_reset())
    elif args.action == 'server-files':
        _json_result(_server_files(args.protocol))
    elif args.action == 'close':
        _json_result(session.close())
    else:
        command_name, build_arguments = PROBE_COMMANDS[args.action]
        _json_result(_probe(command_name, build_arguments(args)))
    return 0


def main(arguments: list[str] | None = None) -> int:
    """Dispatch a driver command and return its process exit code."""
    values = list(sys.argv[1:] if arguments is None else arguments)
    try:
        if values and values[0] in GENERIC_COMMANDS:
            completed = session._delegate(session.fman_source_dir(), values)
            sys.stdout.write(completed.stdout)
            sys.stderr.write(completed.stderr)
            return completed.returncode
        return _dispatch_custom(values)
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        _json_result({'error': str(error)})
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
