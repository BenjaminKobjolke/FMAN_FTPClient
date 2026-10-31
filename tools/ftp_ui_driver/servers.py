"""Run the FTP and SFTP fixture servers for one isolated UI session."""

import json
import os
from pathlib import Path
import shutil
import sys
import threading


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

# sftp_test_support seeds a standalone test tree on import; this process uses
# only the session tree passed on its command line.
import sftp_test_support  # noqa: E402

shutil.rmtree(sftp_test_support.temp_root, ignore_errors=True)

import ftp_server_support  # noqa: E402
from tools import ftp_cli_driver_env  # noqa: E402


FTP_VENDOR_ERROR = r'test-vendor/ is missing; run tools\vendor_test_deps.bat'


def _write_known_hosts(path: Path, host: str, port: int, key: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='\n') as stream:
        stream.write(
            f'[{host}]:{port} {key.get_name()} {key.get_base64()}\n'
        )


def run(session_root: Path, delay_seconds: float) -> None:
    """Start fixture servers, publish readiness, and live until cleanup."""
    remote_root = session_root / 'remote'
    sftp = sftp_test_support.SftpTestServer(
        root=str(remote_root / 'sftp'), delay_seconds=delay_seconds)
    ftp = None
    try:
        ftp_error = None
        if ftp_cli_driver_env.ftp_vendor_available():
            ftp = ftp_server_support.FtpTestServer(
                root=str(remote_root / 'ftp'), delay_seconds=delay_seconds)
            ftp_port = ftp.port
        else:
            ftp_port = None
            ftp_error = FTP_VENDOR_ERROR

        _write_known_hosts(
            session_root / 'known_hosts', '127.0.0.1', sftp.port,
            sftp.host_key)
        ready = {
            'pid': os.getpid(),
            'ftp_port': ftp_port,
            'sftp_port': sftp.port,
            'ftp_error': ftp_error,
        }
        temporary_marker = session_root / 'servers.json.tmp'
        temporary_marker.write_text(
            json.dumps(ready, indent=2) + '\n', encoding='utf-8')
        temporary_marker.replace(session_root / 'servers.json')

        while session_root.is_dir():
            threading.Event().wait(1)
    finally:
        if ftp is not None:
            ftp.close()
        sftp.close()


def main(arguments: list[str] | None = None) -> int:
    """Run as ``python servers.py <session_root> <delay>``."""
    values = sys.argv[1:] if arguments is None else arguments
    if len(values) != 2:
        print('usage: python servers.py <session_root> <delay>', file=sys.stderr)
        return 2
    try:
        delay = float(values[1])
        if delay < 0:
            raise ValueError('delay must be non-negative')
        run(Path(values[0]), delay)
    except Exception as error:
        print(f'fixture server failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
