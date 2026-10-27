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
import threading
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
url.dirname = lambda u: u.rsplit('/', 1)[0]
url.splitscheme = lambda u: (u[:u.index('://') + 3], u[u.index('://') + 3:])
fman.url = url

sys.modules.update({'fman': fman, 'fman.fs': fs, 'fman.url': url})

# A bare package, so the plugin's __init__ (commands, listeners) is not run.
package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package
filesystems = importlib.import_module('ftpclient.filesystems')
transfers = importlib.import_module('ftpclient.transfers')


class FakeConn:
    fail = False

    def __init__(self):
        self._session = self

    def storbinary(self, command, src, callback=None):
        if self.fail:
            raise OSError('connection lost')
        while True:
            block = src.read(SIZE // 5)
            if not block:
                break
            callback(block)


class FakeWrapper:
    conn = FakeConn()

    def __init__(self, url):
        self.path = url

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def drop_connection(self):
        pass

    def upload(self, src_file, on_chunk=None):
        transfer_events.append(('transfer', self.path))
        callback = on_chunk or (lambda _chunk: None)
        self.conn.storbinary('STOR ' + self.path, src_file, callback)


transfers.connection_for = lambda url: FakeWrapper(url)
transfer_events = []
fs.makedirs = lambda path, **kwargs: transfer_events.append(('mkdir', path))

with tempfile.TemporaryDirectory() as tmp_dir:
    with open(os.path.join(tmp_dir, 'a.bin'), 'wb') as f:
        f.write(b'x' * SIZE)
    src = 'file://' + tmp_dir.replace(os.sep, '/') + '/a.bin'
    dst = 'ftp://example.com/dir/a.bin'
    ftp_fs = filesystems.FtpFs()

    tasks = list(ftp_fs.prepare_copy(src, dst))
    assert [t.get_title() for t in tasks] == \
        ['Uploading a.bin', 'Checking failed transfers'], tasks
    assert tasks[0].get_size() == SIZE, 'size must be known before it runs'
    tasks[0]()
    assert submitted == [], 'a subtask must not open a dialog of its own'
    assert tasks[0]._unit.progress == SIZE
    assert transfer_events[0] == ('mkdir', 'ftp://example.com/dir'), \
        'the destination parent is created before the transfer'
    assert transfer_events[1] == ('transfer', 'ftp://example.com/dir/a.bin')

    ftp_fs.copy(src, dst)
    assert submitted == ['Copying a.bin'], 'a direct copy shows one dialog'

    move, _check = ftp_fs.prepare_move(src, dst)
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
fs.makedirs = lambda path, **kwargs: transfer_events.append(('mkdir', path))
fs.is_dir = lambda u: u == folder
fs.iterdir = lambda u: ['x.bin'] if u == folder else []
tasks = list(ftp_fs.prepare_move(folder, 'file:///tmp/dir'))
assert [t.get_title() for t in tasks] == \
    ['Creating dir', 'Moving x.bin', 'Deleting dir',
     'Checking failed transfers'], tasks
assert not getattr(tasks[0], 'blocks_later_tasks', False), \
    'creating a folder does not block unrelated files'
assert tasks[2].waits_for_earlier_tasks, 'the folder waits for its files'
deleted.clear()
tasks[2]()
assert deleted == [], 'a folder with files left in it is kept'
fs.iterdir = lambda u: []
tasks[2]()
assert deleted == [folder], 'an emptied folder is deleted'
fs.is_dir = lambda url: False

# Folder creation is shared by all files in one copy/move operation. Only one
# worker creates a given URL, and a failed create is retried by the next file.
folders = transfers._Folders()
mkdir_calls = []
mkdir_started = threading.Event()
release_mkdir = threading.Event()

def slow_makedirs(path, **kwargs):
    mkdir_calls.append(path)
    mkdir_started.set()
    release_mkdir.wait()

fs.makedirs = slow_makedirs
workers = [threading.Thread(target=folders.ensure, args=('ftp://host/parent',))
           for _ in range(4)]
for worker in workers:
    worker.start()
assert mkdir_started.wait(1), 'one worker should start creating the folder'
release_mkdir.set()
for worker in workers:
    worker.join(1)
assert all(not worker.is_alive() for worker in workers), 'workers should finish'
assert mkdir_calls == ['ftp://host/parent'], 'the folder is created once'

mkdir_attempts = []
def fail_once(path, **kwargs):
    mkdir_attempts.append(path)
    if len(mkdir_attempts) == 1:
        raise OSError('temporary failure')

fs.makedirs = fail_once
retry_folders = transfers._Folders()
try:
    retry_folders.ensure('ftp://host/retry')
except OSError:
    pass
else:
    raise AssertionError('a failed folder creation must surface')
retry_folders.ensure('ftp://host/retry')
assert mkdir_attempts == ['ftp://host/retry', 'ftp://host/retry'], \
    'a failed creation must leave the folder eligible for retry'

# Between FTP and this machine, fman runs several transfers at once. Between
# two FTP servers it does not.
local, remote = 'file:///tmp/a', 'ftp://example.com/a'
parallel = transfers.PARALLEL_TRANSFERS
assert ftp_fs.get_parallel_transfers(remote, local) == parallel, 'download'
assert ftp_fs.get_parallel_transfers(local, remote) == parallel, 'upload'
assert ftp_fs.get_parallel_transfers(remote, 'ftps://other/a') == 1

print('ok: one dialog per transfer, source kept when a move fails, '
      'folders move file by file, '
      'parallel transfers only between FTP and this machine')
