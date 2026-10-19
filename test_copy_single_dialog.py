"""Offline self-check: a transfer must show one progress dialog, not two.

fman runs a copy or move inside its own progress dialog and asks the file
system for subtasks. Those subtasks must carry their size and must not open a
dialog of their own. A move must also keep the source when the copy fails.

Run: python test_copy_single_dialog.py
"""
import importlib
import os
import sys
import tempfile
import types

HERE = os.path.dirname(os.path.abspath(__file__))
SIZE = 5000

submitted = []
deleted = []


class Task:
    def __init__(self, title, size=0, fn=lambda: None, args=(), kwargs=None):
        self._title = title
        self._size = size
        self._fn = fn
        self._args = args
        self._kwargs = kwargs or {}
        self.progress = 0

    def __call__(self):
        self._fn(*self._args, **self._kwargs)

    def get_title(self):
        return self._title

    def get_size(self):
        return self._size

    def set_size(self, size):
        # What fman's ChildProgressDialog does to a running subtask.
        raise NotImplementedError('subtask size is fixed once it has started')

    def set_progress(self, progress):
        self.progress = progress

    def check_canceled(self):
        pass

    def run(self, subtask):
        subtask()


def submit_task(task):
    submitted.append(task.get_title())
    task()


class FileSystem:
    def __init__(self):
        self.cache = types.SimpleNamespace(clear=lambda path: None)

    def prepare_copy(self, src_url, dst_url):
        return [Task('Copying ' + url.basename(src_url),
                     fn=self.copy, args=(src_url, dst_url))]

    def prepare_move(self, src_url, dst_url):
        return [Task('Moving ' + url.basename(src_url),
                     fn=self.move, args=(src_url, dst_url))]


fman = types.ModuleType('fman')
fman.__path__ = []
fman.Task = Task
fman.submit_task = submit_task
fman.show_status_message = lambda *args, **kwargs: None
fman.load_json = lambda name, default=None, save_on_quit=False: \
    {} if default is None else default

fs = types.ModuleType('fman.fs')
fs.FileSystem = FileSystem
fs.cached = lambda method: method
fs.is_dir = lambda url: False
fs.exists = lambda url: True
fs.delete = deleted.append
fs.notify_file_added = lambda url: None
fman.fs = fs

url = types.ModuleType('fman.url')
url.join = lambda *parts: '/'.join(parts)
url.basename = lambda u: u.rsplit('/', 1)[-1]
url.splitscheme = lambda u: (u[:u.index('://') + 3], u[u.index('://') + 3:])
fman.url = url

sys.modules.update({'fman': fman, 'fman.fs': fs, 'fman.url': url})

# A bare package, so the plugin's __init__ (commands, listeners) is not run.
package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package
filesystems = importlib.import_module('ftpclient.filesystems')


class FakeConn:
    fail = False

    def upload(self, src, dst, callback=None):
        if self.fail:
            raise OSError('connection lost')
        for _ in range(5):
            callback(b'x' * (SIZE // 5))


class FakeWrapper:
    conn = FakeConn()

    def __init__(self, url):
        self.path = url

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


filesystems.FtpWrapper = FakeWrapper

with tempfile.TemporaryDirectory() as tmp_dir:
    with open(os.path.join(tmp_dir, 'a.bin'), 'wb') as f:
        f.write(b'x' * SIZE)
    src = 'file://' + tmp_dir.replace(os.sep, '/') + '/a.bin'
    dst = 'ftp://example.com/dir/a.bin'
    ftp_fs = filesystems.FtpFs()

    tasks = list(ftp_fs.prepare_copy(src, dst))
    assert [t.get_title() for t in tasks] == ['Uploading a.bin'], tasks
    assert tasks[0].get_size() == SIZE, 'size must be known before it runs'
    tasks[0]()
    assert submitted == [], 'a subtask must not open a dialog of its own'
    assert tasks[0].progress == SIZE

    ftp_fs.copy(src, dst)
    assert submitted == ['Copying a.bin'], 'a direct copy shows one dialog'

    move, = ftp_fs.prepare_move(src, dst)
    assert move.get_size() == SIZE
    FakeConn.fail = True
    try:
        move()
    except OSError:
        pass
    else:
        raise AssertionError('a failed upload must fail the move')
    assert deleted == [], 'a failed upload must not delete the source'

    FakeConn.fail = False
    move()
    assert deleted == [src], 'a finished move deletes the source'
    assert submitted == ['Copying a.bin'], 'a move subtask opens no dialog'

# A folder moves file by file, so fman's workers can share it. Creating the
# destination holds back the files that go into it; the source folder goes
# last, once the moves out of it are done, and only if nothing is left in it.
FakeConn.path = types.SimpleNamespace(getsize=lambda path: SIZE)
folder = 'ftp://example.com/dir'
fs.makedirs = lambda *args, **kwargs: None
fs.is_dir = lambda u: u == folder
fs.iterdir = lambda u: ['x.bin'] if u == folder else []
tasks = list(ftp_fs.prepare_move(folder, 'file:///tmp/dir'))
assert [t.get_title() for t in tasks] == \
    ['Creating dir', 'Moving x.bin', 'Deleting dir'], tasks
assert tasks[0].blocks_later_tasks, 'files wait for their folder'
assert tasks[-1].waits_for_earlier_tasks, 'the folder waits for its files'
deleted.clear()
tasks[-1]()
assert deleted == [], 'a folder with files left in it is kept'
fs.iterdir = lambda u: []
tasks[-1]()
assert deleted == [folder], 'an emptied folder is deleted'
fs.is_dir = lambda url: False

# Between FTP and this machine, fman runs several transfers at once. Between
# two FTP servers it does not.
local, remote = 'file:///tmp/a', 'ftp://example.com/a'
parallel = filesystems.PARALLEL_TRANSFERS
assert ftp_fs.get_parallel_transfers(remote, local) == parallel, 'download'
assert ftp_fs.get_parallel_transfers(local, remote) == parallel, 'upload'
assert ftp_fs.get_parallel_transfers(remote, 'ftps://other/a') == 1

print('ok: one dialog per transfer, source kept when a move fails, '
      'folders move file by file, '
      'parallel transfers only between FTP and this machine')
