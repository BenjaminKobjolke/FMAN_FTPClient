"""Offline regression check for permanent FTP deletion through fman's trash action.

Run: python test_move_to_trash.py
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
fman.show_status_message = lambda *args, **kwargs: None
fman.Task = object
fman.submit_task = lambda task: None

fman_fs = types.ModuleType('fman.fs')
fman_fs.FileSystem = type('FileSystem', (), {})
fman_fs.cached = lambda method: method
fman_fs.notify_file_added = lambda url: None
fman_fs.notify_file_removed = lambda url: None
fman_fs.notify_file_changed = lambda url: None
fman.fs = fman_fs

fman_url = types.ModuleType('fman.url')
fman_url.join = lambda *parts: '/'.join(parts)
fman_url.splitscheme = lambda url: url.split('://', 1)
fman_url.basename = lambda url: url.rsplit('/', 1)[-1]
fman_url.dirname = lambda url: url.rsplit('/', 1)[0]

sys.modules.update({
    'fman': fman,
    'fman.fs': fman_fs,
    'fman.url': fman_url,
})

# Load the filesystem module without initializing the rest of the plugin.
package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package
spec = importlib.util.spec_from_file_location(
    'ftpclient.filesystems', os.path.join(HERE, 'ftpclient', 'filesystems.py'))
filesystems = importlib.util.module_from_spec(spec)
sys.modules['ftpclient.filesystems'] = filesystems
spec.loader.exec_module(filesystems)

fs_obj = filesystems.FtpFs()
deleted = []
fs_obj.delete = lambda path: deleted.append(path)

path = 'example.com/public/file.txt'
fs_obj.move_to_trash(path)
assert deleted == [path], deleted
print('ok: move_to_trash permanently deletes the same FTP path')
