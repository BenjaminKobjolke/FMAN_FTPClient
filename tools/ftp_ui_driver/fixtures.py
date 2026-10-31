"""Seed and inspect the local files used by the FTP desktop UI driver."""

import os
import shutil
from pathlib import Path


PROTOCOLS = ('ftp', 'sftp')
LEFT_NAME = 'left'
RIGHT_NAME = 'right'
REMOTE_NAME = 'remote'
UPLOAD_NAME = 'upload'
DELETE_NAME = 'delete-me'
FILE_SIZE = 32 * 1024


def _write_file(root: Path, relative_path: str, content: bytes) -> None:
    path = root.joinpath(*relative_path.split('/'))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _fixture_files() -> list[tuple[str, bytes]]:
    files = [
        (f'{LEFT_NAME}/{UPLOAD_NAME}/file{i:02}.txt', bytes([i]) * FILE_SIZE)
        for i in range(1, 11)
    ]
    files.extend([
        (f'{LEFT_NAME}/{UPLOAD_NAME}/nested/a.txt', b'nested a'),
        (f'{LEFT_NAME}/{UPLOAD_NAME}/nested/b.txt', b'nested b'),
        (f'{LEFT_NAME}/single.txt', b'single local file'),
    ])
    for protocol in PROTOCOLS:
        remote = f'{REMOTE_NAME}/{protocol}'
        files.append((f'{remote}/folder/source.txt', b'fixture source'))
        files.extend(
            (f'{remote}/{DELETE_NAME}/d{i:02}.txt', f'delete {i}'.encode())
            for i in range(1, 7)
        )
        files.extend(
            (f'{remote}/{DELETE_NAME}/sub/s{i}.txt', f'sub {i}'.encode())
            for i in range(1, 4)
        )
        files.append((f'{remote}/single.txt', b'single remote file'))
    return files


def seed(session_root: str | os.PathLike[str]) -> list[str]:
    """Recreate both panes and the FTP/SFTP server trees; return file paths."""
    root = Path(session_root)
    for name in (LEFT_NAME, RIGHT_NAME, REMOTE_NAME):
        shutil.rmtree(root / name, ignore_errors=True)
    (root / LEFT_NAME).mkdir(parents=True)
    (root / RIGHT_NAME).mkdir()
    paths = _fixture_files()
    for relative_path, content in paths:
        _write_file(root, relative_path, content)
    return sorted(relative_path for relative_path, _ in paths)


def server_files(
    session_root: str | os.PathLike[str], protocol: str
) -> list[str]:
    """Return sorted file paths relative to one fixture server's root."""
    if protocol not in PROTOCOLS:
        raise ValueError(f'unknown fixture protocol: {protocol}')
    server_root = Path(session_root) / REMOTE_NAME / protocol
    if not server_root.is_dir():
        return []
    return sorted(
        path.relative_to(server_root).as_posix()
        for path in server_root.rglob('*')
        if path.is_file()
    )
