"""Offline self-check: probing the bare scheme root must not open a connection.

fman's `is_parent` walks a URL's ancestors up to `ftp://`, so `exists`/`is_dir`
get called with an empty path. Connecting there means dialing host '' port 21.

Run: python test_root_path_no_connect.py
"""
import importlib.util
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))

fman = types.ModuleType('fman')
fman.__path__ = []
fman.load_json = lambda name, default=None, save_on_quit=False: \
    {} if default is None else default
fman.show_status_message = lambda *a, **kw: None
fman.Task = object
fman.submit_task = lambda task: None

fman_fs = types.ModuleType('fman.fs')
fman_fs.FileSystem = type('FileSystem', (), {})
fman_fs.cached = lambda f: f
fman_fs.notify_file_added = lambda url: None
fman_fs.notify_file_removed = lambda url: None
fman.fs = fman_fs

fman_url = types.ModuleType('fman.url')
fman_url.join = lambda *a: '/'.join(a)
fman_url.splitscheme = lambda url: url.split('://', 1)

sys.modules['fman'] = fman
sys.modules['fman.fs'] = fman_fs
sys.modules['fman.url'] = fman_url

# Synthetic package, so `from .ftp import ...` resolves without running
# ftpclient/__init__.py (which pulls in commands, columns and listeners).
pkg = types.ModuleType('ftpclient')
pkg.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = pkg

spec = importlib.util.spec_from_file_location(
    'ftpclient.filesystems', os.path.join(HERE, 'ftpclient', 'filesystems.py'))
filesystems = importlib.util.module_from_spec(spec)
sys.modules['ftpclient.filesystems'] = filesystems
spec.loader.exec_module(filesystems)


def _no_connections_please(url):
    raise AssertionError('tried to connect for %r' % (url,))


filesystems.FtpWrapper = _no_connections_please

for cls in (filesystems.FtpFs, filesystems.FtpsFs):
    fs_obj = cls()
    fs_obj.cache = types.SimpleNamespace(clear=lambda path: None)
    assert fs_obj.exists('') is False, cls
    assert fs_obj.is_dir('') is False, cls

print('ok: root path answers False without connecting')

# mkdir on an existing directory must raise FileExistsError instead of
# "succeeding" and notifying. fman calls makedirs(dest_dir, exist_ok=True)
# before every copy; a notification there sends the pane to its parent.
removed = []
fman_fs.notify_file_removed = lambda url: removed.append(url)

fs_obj = filesystems.FtpFs()
fs_obj.cache = types.SimpleNamespace(clear=lambda path: None)
fs_obj.exists = lambda path: True
try:
    fs_obj.mkdir('host/e/GIT')
except FileExistsError:
    pass
else:
    raise AssertionError('mkdir on an existing dir must raise FileExistsError')
assert not removed, removed
print('ok: mkdir of an existing dir raises, emits no removal event')
