"""Offline self-check: individual failed transfers are retried silently.

Each transfer retries `transfer_retries` times on a new connection. fman owns
any retry prompt after a copy or move finishes.

Run: python test_transfer_retry.py
"""
import importlib
import os
import tempfile
import types
from test_fman_setup import install_fman_modules

HERE = os.path.dirname(os.path.abspath(__file__))

settings = {}
deleted = []


class Canceled(Exception):
    pass


class Task:
    def __init__(self, title, size=0, fn=lambda: None, args=(), kwargs=None):
        self._title = title
        self._size = size
        self._fn = fn
        self._args = args
        self._kwargs = kwargs or {}

    def __call__(self):
        self._fn(*self._args, **self._kwargs)

    def get_title(self):
        return self._title

    def get_size(self):
        return self._size

    def set_progress(self, progress):
        pass

    def check_canceled(self):
        pass

    def run(self, subtask):
        subtask()

fman = types.ModuleType('fman')
fman.__path__ = []
fman.Task = Task
fman.show_status_message = lambda *args, **kwargs: None
fman.submit_task = lambda task: None
fman.load_json = lambda name, default=None, save_on_quit=False: settings

fs = types.ModuleType('fman.fs')
fs.is_dir = lambda url: False
fs.exists = lambda url: True
fs.delete = deleted.append
fs.makedirs = lambda url, exist_ok=False: None
fs.notify_file_added = lambda url: None
install_fman_modules(fman, fs, HERE)
transfers = importlib.import_module('ftpclient.transfers')
transfer_retry = importlib.import_module('ftpclient.transfer_retry')


class FakeWrapper:
    """Fails the next `failures[path]` uploads of that path."""
    failures = {}
    error = OSError
    dropped = []
    uploads = []

    def __init__(self, remote_url):
        self.path = remote_url

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def drop_connection(self):
        self.dropped.append(self.path)

    def upload(self, src_file, on_chunk=None):
        if self.failures.get(self.path, 0):
            self.failures[self.path] -= 1
            raise self.error('connection lost')
        self.uploads.append(self.path)


transfers.connection_for = FakeWrapper


def reset(retries, failures):
    settings.clear()
    settings['transfer_retries'] = retries
    FakeWrapper.failures = dict(failures)
    FakeWrapper.error = OSError
    del FakeWrapper.dropped[:], FakeWrapper.uploads[:], deleted[:]


def run_like_fman(task_lists):
    """Run every gathered task in order; a failure does not stop the rest."""
    tasks = [task for task_list in task_lists for task in task_list]
    for task in tasks:
        try:
            task()
        except transfer_retry.TRANSIENT_ERRORS:
            pass


with tempfile.TemporaryDirectory() as tmp_dir:
    def local(name):
        path = os.path.join(tmp_dir, name)
        with open(path, 'wb') as f:
            f.write(b'x')
        return 'file://' + path.replace(os.sep, '/')

    a = local('a.bin')
    dst_a = 'ftp://host/a.bin'

    def prepare(src, dst, prepare_fn=transfers.prepare_copy):
        return list(prepare_fn(src, dst, lambda path: None))

    # A transient failure is retried on a fresh connection.
    reset(2, {dst_a: 2})
    run_like_fman([prepare(a, dst_a)])
    assert FakeWrapper.uploads == [dst_a], FakeWrapper.uploads
    assert FakeWrapper.dropped == [dst_a, dst_a], FakeWrapper.dropped

    # fman receives one task for a file, without a trailing prompt task.
    tasks = prepare(a, dst_a)
    assert len(tasks) == 1, tasks
    assert tasks[0].get_title() == 'Uploading a.bin', tasks[0].get_title()

    # retries=0 fails at once, like before retries existed.
    reset(0, {dst_a: 1})
    unit = prepare(a, dst_a)[0]
    try:
        unit()
    except OSError:
        pass
    else:
        raise AssertionError('the last failure must reach fman')
    assert FakeWrapper.dropped == []

    # Moves return one task; a failed move keeps its source, and fman's retry
    # can run that same task again after the failure is used up.
    reset(0, {dst_a: 1})
    tasks = prepare(a, dst_a, transfers.prepare_move)
    assert [t.get_title() for t in tasks] == ['Moving a.bin'], tasks
    try:
        tasks[0]()
    except OSError:
        pass
    assert deleted == [], 'a failed move keeps its source'
    tasks[0]()
    assert deleted == [a], deleted

    # Cancellation and bugs are not retried or recorded.
    reset(3, {dst_a: 1})
    FakeWrapper.error = Canceled
    tasks = prepare(a, dst_a)
    try:
        tasks[0]()
    except Canceled:
        pass
    else:
        raise AssertionError('cancellation must propagate')
    assert FakeWrapper.dropped == [], 'a canceled transfer is not retried'
    # The setting is validated.
    assert transfer_retry.parse_transfer_retries('3') == 3
    for bad in ('-1', '11', 'x', None, '2.5'):
        assert transfer_retry.parse_transfer_retries(bad) is None, bad
    settings.clear()
    settings['transfer_retries'] = 'nonsense'
    assert transfer_retry.transfer_retries() == transfer_retry.DEFAULT_TRANSFER_RETRIES

print('ok: silent per-transfer retries, one task per file, moves delete only '
      'after success, cancellation not retried, setting validated')
