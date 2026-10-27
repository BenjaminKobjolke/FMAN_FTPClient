"""Offline regression checks for FTP file creation and pane refreshes."""
import importlib
import os
import sys
import types
from ftp_test_support import FakeWrapper as BaseFakeWrapper, make_fman_url


HERE = os.path.dirname(os.path.abspath(__file__))
notifications = []


class Task:
    def __init__(self, *args, **kwargs):
        pass


class FileSystem:
    def __init__(self):
        self.cache = types.SimpleNamespace(clear=lambda path: None)


class Column:
    pass


class DirectoryPaneCommand:
    pass


class DirectoryPaneListener:
    pass


fman = types.ModuleType('fman')
fman.__path__ = []
fman.Task = Task
fman.DirectoryPaneCommand = DirectoryPaneCommand
fman.DirectoryPaneListener = DirectoryPaneListener
fman.NO = fman.YES = 0
fman.QuicksearchItem = lambda *args, **kwargs: None
fman.load_json = lambda name, default=None, save_on_quit=False: \
    {} if default is None else default
fman.save_json = lambda *args, **kwargs: None
fman.show_alert = fman.show_status_message = lambda *args, **kwargs: None
fman.show_quicksearch = fman.show_prompt = lambda *args, **kwargs: ('', False)
fman.submit_task = lambda task: task()
fs = types.ModuleType('fman.fs')
fs.FileSystem = FileSystem
fs.Column = Column
fs.cached = lambda method: method
fs.notify_file_added = lambda url: notifications.append(('added', url))
fs.notify_file_removed = lambda url: notifications.append(('removed', url))
fs.notify_file_changed = lambda url: notifications.append(('changed', url))
fs.exists = lambda url: False
fs.is_dir = lambda url: False
fs.touch = lambda url: None
fs.query = lambda url, method: None
fman.fs = fs
fman.url = url = make_fman_url()
sys.modules.update({'fman': fman, 'fman.fs': fs, 'fman.url': url})

clipboard = types.ModuleType('fman.clipboard')
clipboard.set_text = lambda *args: None
sys.modules['fman.clipboard'] = clipboard

core = types.ModuleType('core')
core.__path__ = []
core_commands = types.ModuleType('core.commands')
core_commands._open_local_files = lambda *args: None
sys.modules.update({'core': core, 'core.commands': core_commands})

qt_thread = types.ModuleType('fman.impl.util.qt.thread')
qt_thread.run_in_main_thread = lambda fn: fn
for name in ('fman.impl', 'fman.impl.util', 'fman.impl.util.qt'):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
sys.modules['fman.impl.util.qt.thread'] = qt_thread
qtcore = types.ModuleType('PyQt5.QtCore')
qtcore.QFileSystemWatcher = qtcore.QTimer = type('QtStub', (), {})
pyqt5 = types.ModuleType('PyQt5')
pyqt5.__path__ = []
sys.modules.update({'PyQt5': pyqt5, 'PyQt5.QtCore': qtcore})

# Import plugin modules without running package UI initialization.
package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package
filesystems = importlib.import_module('ftpclient.filesystems')
columns = importlib.import_module('ftpclient.columns')
listeners = importlib.import_module('ftpclient.listeners')
commands = importlib.import_module('ftpclient.commands')


class FakeSession:
    def __init__(self):
        self.uploads = []

    def storbinary(self, command, file_obj, callback=None):
        self.uploads.append((command, file_obj.read()))


class FakeHost:
    def __init__(self):
        self._session = FakeSession()
        self.path = types.SimpleNamespace(
            exists=lambda path: False, isdir=lambda path: False)
        self.renames = []
        self.removes = []
        self.directories = []

    def rename(self, src, dst):
        self.renames.append((src, dst))

    def remove(self, path):
        self.removes.append(path)

    def rmtree(self, path):
        self.removes.append(path)

    def makedirs(self, path):
        self.directories.append(path)


host = FakeHost()


class FakeWrapper(BaseFakeWrapper):
    def download(self, dst_file, on_chunk):
        raise AssertionError('download is not used in this test')


filesystems.connection_for = lambda url: FakeWrapper(url, host)


def expect_notifications(expected):
    actual = notifications[:]
    actual_mutations = [item for item in actual if item[0] != 'changed']
    expected_mutations = [item for item in expected if item[0] != 'changed']
    actual_changed = sorted(item for item in actual if item[0] == 'changed')
    expected_changed = sorted(
        item for item in expected if item[0] == 'changed')
    assert (actual_mutations, actual_changed) == \
        (expected_mutations, expected_changed), (actual, expected)
    notifications.clear()


ftp_fs = filesystems.FtpFs()
ftp_fs.exists = lambda path: False
ftp_fs.touch('example.com/dir/new.txt')
assert host._session.uploads == [('STOR /dir/new.txt', b'')]
expect_notifications([
    ('added', 'ftp://example.com/dir/new.txt'),
    ('changed', 'ftp://example.com/dir')])

ftp_fs.delete('example.com/dir/old.txt')
expect_notifications([
    ('removed', 'ftp://example.com/dir/old.txt'),
    ('changed', 'ftp://example.com/dir')])
ftp_fs.mkdir('example.com/dir/new-folder')
expect_notifications([
    ('added', 'ftp://example.com/dir/new-folder'),
    ('changed', 'ftp://example.com/dir')])

ftp_fs.move('ftp://example.com/dir/a.txt', 'ftp://example.com/other/a.txt')
expect_notifications([
    ('removed', 'ftp://example.com/dir/a.txt'),
    ('added', 'ftp://example.com/other/a.txt'),
    ('changed', 'ftp://example.com/dir'),
    ('changed', 'ftp://example.com/other')])
ftp_fs.move('ftp://example.com/dir/a.txt', 'ftp://example.com/dir/b.txt')
assert [n for n in notifications if n[0] == 'changed'] == [
    ('changed', 'ftp://example.com/dir')]
notifications.clear()

for column in (columns.Permissions(), columns.Owner(), columns.Group()):
    columns.query = lambda url, method: \
        (_ for _ in ()).throw(OSError('offline'))
    assert column.get_str('ftp://example.com/file.txt') == ''

listener = listeners.FtpListener()
listener.pane = types.SimpleNamespace(get_path=lambda: 'ftp://example.com/dir')
assert listener.on_command('create_and_edit_file', {}) == \
    ('create_and_edit_ftp_file', {})
listener.pane.get_path = lambda: 'file:///tmp'
assert listener.on_command('create_and_edit_file', {}) is None
listener.pane.get_path = lambda: 'ftp://example.com/dir'
assert listener.on_command('move_to_trash', {}) == \
    ('delete_permanently', {})
listener.pane.get_path = lambda: 'file:///tmp'
assert listener.on_command('move_to_trash', {}) is None

prompt_calls = []
fman.show_prompt = lambda prompt, default='': (
    prompt_calls.append((prompt, default)) or ('new.txt', True))
commands.show_prompt = fman.show_prompt
created = []
fs.touch = created.append
pane_calls = []
command = commands.CreateAndEditFtpFile()
command.pane = types.SimpleNamespace(
    get_path=lambda: 'ftp://example.com/dir',
    get_file_under_cursor=lambda: 'ftp://example.com/dir/old.txt',
    place_cursor_at=pane_calls.append,
    run_command=lambda *args: pane_calls.append(args))
command()
assert prompt_calls == [('Enter file name to create/edit:', 'old.txt')]
assert created == ['ftp://example.com/dir/new.txt']
assert pane_calls == [
    'ftp://example.com/dir/new.txt',
    ('open_with_editor', {'url': 'ftp://example.com/dir/new.txt'})]

print('ok: FTP create, mutation refresh, resilient columns, and command routing')
