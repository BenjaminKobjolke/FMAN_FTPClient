"""Parse offline fman++ transfer queue files and resolve their paths."""
import ntpath
import os
import posixpath
from dataclasses import dataclass
from typing import Iterable, List, Tuple

from fman.url import as_url


@dataclass(frozen=True)
class QueueEntry:
    """One upload or download from a queue file."""

    op: str
    src: str
    dst: str
    line: int


class QueueError(ValueError):
    """A queue parse error containing every invalid input line."""

    def __init__(self, errors: List[str]) -> None:
        self.errors = errors
        super().__init__('\n'.join(errors))


def _tokenize(line: str) -> List[str]:
    """Split quoted or whitespace-free arguments without unescaping paths."""
    tokens = []
    index = 0
    while index < len(line):
        while index < len(line) and line[index].isspace():
            index += 1
        if index == len(line):
            break
        if line[index] == '"':
            end = line.find('"', index + 1)
            if end < 0:
                raise ValueError('unclosed quoted argument')
            token = line[index + 1:end]
            index = end + 1
            if index < len(line) and not line[index].isspace():
                raise ValueError('quoted arguments must be separated by spaces')
        else:
            end = index
            while end < len(line) and not line[end].isspace():
                if line[end] == '"':
                    raise ValueError('quote must start an argument')
                end += 1
            token = line[index:end]
            index = end
        tokens.append(token)
    return tokens


def parse_queue(text: str) -> List[QueueEntry]:
    """Parse WinSCP-style put/get lines and collect errors by source line."""
    entries = []
    errors = []
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        try:
            parts = _tokenize(line)
        except ValueError as error:
            errors.append('line %d: %s' % (line_number, error))
            continue
        if len(parts) != 3 or parts[0].lower() not in ('put', 'get'):
            errors.append(
                'line %d: expected put or get followed by two paths' % line_number)
            continue
        entries.append(QueueEntry(parts[0].lower(), parts[1], parts[2],
                                  line_number))
    if errors:
        raise QueueError(errors)
    return entries


def _local_path(path: str, queue_dir: str) -> str:
    if not os.path.isabs(path):
        path = os.path.join(queue_dir, path)
    return os.path.abspath(path)


def _remote_path(path: str, remote_cwd: str) -> str:
    path = path.replace('\\', '/')
    if not path.startswith('/'):
        path = posixpath.join(remote_cwd or '/', path)
    return posixpath.normpath(path)


def _remote_url(base_url: str, path: str, remote_cwd: str) -> str:
    resolved_path = _remote_path(path, remote_cwd)
    return base_url.rstrip('/') + '/' + resolved_path.lstrip('/')


def _is_folder(path: str) -> bool:
    return path.endswith(('/', '\\'))


def _append_remote_basename(path: str, source: str) -> str:
    return path.rstrip('/\\') + '/' + ntpath.basename(source.rstrip('/\\'))


def resolve(
    entries: Iterable[QueueEntry], queue_dir: str, base_url: str,
    remote_cwd: str,
) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Resolve entries to URL pairs and report missing local upload sources."""
    pairs = []
    missing = []
    for entry in entries:
        if entry.op == 'put':
            local_src = _local_path(entry.src, queue_dir)
            if not os.path.exists(local_src):
                missing.append(local_src)
            remote_dst = entry.dst
            if _is_folder(remote_dst):
                remote_dst = _append_remote_basename(remote_dst, local_src)
            pairs.append((as_url(local_src),
                          _remote_url(base_url, remote_dst, remote_cwd)))
        else:
            remote_src = entry.src
            local_dst = entry.dst
            if _is_folder(local_dst):
                local_dst = os.path.join(
                    local_dst, ntpath.basename(remote_src.rstrip('/\\')))
            if not os.path.isabs(local_dst):
                local_dst = os.path.join(queue_dir, local_dst)
            remote_dst = os.path.abspath(local_dst)
            pairs.append((_remote_url(base_url, remote_src, remote_cwd),
                          as_url(remote_dst)))
    return pairs, missing


def has_relative_remote(entries: Iterable[QueueEntry]) -> bool:
    """Return whether any remote path depends on the current remote folder."""
    for entry in entries:
        remote_path = entry.dst if entry.op == 'put' else entry.src
        if not remote_path.startswith('/'):
            return True
    return False
