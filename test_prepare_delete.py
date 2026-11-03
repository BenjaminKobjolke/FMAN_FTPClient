"""Offline regression checks for per-entry FTP deletion tasks.

Run: python test_prepare_delete.py
"""
import os

from ftp_test_support import load_filesystems


class Task:
    def __init__(self, title, fn, args, size):
        self.title = title
        self.fn = fn
        self.args = args
        self.size = size

    def get_size(self):
        return self.size

    def __call__(self):
        return self.fn(*self.args)


removed_notifications = []
changed_notifications = []
fman_fs, filesystems = load_filesystems(
    os.path.dirname(os.path.abspath(__file__)), Task)
fman_fs.notify_file_removed = removed_notifications.append
fman_fs.notify_file_changed = changed_notifications.append


tree = {
    'top': ['a.txt', 'sub', 'c.txt'],
    'top/sub': ['b.txt'],
}
directories = {'top', 'top/sub'}


class FakeConnection:
    def __init__(self, path, calls):
        self.path = path.split('://', 1)[1]
        self.conn = self
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def remove(self, path):
        self.calls.append(('remove', path))

    def rmdir(self, path):
        self.calls.append(('rmdir', path))


def make_fs():
    calls = []
    fs_obj = filesystems.FtpFs()
    fs_obj.is_dir = lambda path: path in directories
    fs_obj.iterdir = lambda path: iter(tree[path])
    filesystems.connection_for = lambda url: FakeConnection(url, calls)
    return fs_obj, calls


fs_obj, calls = make_fs()
assert fs_obj.get_parallel_deletes('host/x') == \
    filesystems.transfers.PARALLEL_TRANSFERS

tasks = list(fs_obj.prepare_delete('top'))
assert [task.args[0] for task in tasks] == [
    'top/a.txt', 'top/sub/b.txt', 'top/c.txt', 'top/sub', 'top']
assert [task.get_size() for task in tasks] == [1, 1, 1, 1, 1]
assert [task.title for task in tasks] == [
    'Deleting a.txt', 'Deleting b.txt', 'Deleting c.txt',
    'Deleting sub', 'Deleting top']
assert [getattr(task, 'waits_for_earlier_tasks', False) for task in tasks] == [
    False, False, False, True, True]
assert [task.args[2] for task in tasks] == [False, False, False, False, True]
assert calls == []

for task in tasks:
    task()

assert calls == [
    ('remove', 'top/a.txt'),
    ('remove', 'top/sub/b.txt'),
    ('remove', 'top/c.txt'),
    ('rmdir', 'top/sub'),
    ('rmdir', 'top'),
], calls
assert removed_notifications == ['ftp://top'], removed_notifications
assert changed_notifications == [], changed_notifications

trash_fs, _ = make_fs()
trash_tasks = list(trash_fs.prepare_trash('top'))
assert [(task.title, task.args, task.get_size()) for task in trash_tasks] == [
    (task.title, task.args, task.get_size()) for task in tasks]

removed_notifications.clear()
changed_notifications.clear()
delete_fs, delete_calls = make_fs()
delete_fs.delete('top')
assert delete_calls == [
    ('remove', 'top/a.txt'),
    ('remove', 'top/sub/b.txt'),
    ('rmdir', 'top/sub'),
    ('remove', 'top/c.txt'),
    ('rmdir', 'top'),
], delete_calls
assert removed_notifications == ['ftp://top'], removed_notifications
assert changed_notifications == [], changed_notifications

print(
    'ok: delete tasks run child-first with one top-level removal '
    'and no folder reload'
)
