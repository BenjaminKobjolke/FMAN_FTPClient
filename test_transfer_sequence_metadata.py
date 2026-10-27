"""Offline self-check: FTP transfer tasks expose their tool-window options.

Run: python test_transfer_sequence_metadata.py
"""
import importlib
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))


class Task:
    def __init__(self, *args, **kwargs):
        self.title = args[0] if args else None
        self.size = kwargs.get('size', 0)

    def get_size(self):
        return self.size


fman = types.ModuleType('fman')
fman.__path__ = []
fman.Task = Task
submitted_tasks = []
fman.submit_task = submitted_tasks.append
fman.show_status_message = lambda *args, **kwargs: None
fman.load_json = lambda name, default=None, save_on_quit=False: {}

fs = types.ModuleType('fman.fs')
prepared_copy = []
fs.prepare_copy = lambda src, dst: (
    prepared_copy.append((src, dst)) or [Task('copy', size=1)])
fman.fs = fs

url = types.ModuleType('fman.url')
url.basename = lambda value: value.rsplit('/', 1)[-1]
url.dirname = lambda value: value.rsplit('/', 1)[0]
url.join = lambda *parts: '/'.join(parts)
url.splitscheme = lambda value: (value[:value.index('://') + 3],
                                 value[value.index('://') + 3:])
url.scheme = lambda value: value.split(':', 1)[0]
fman.url = url

sys.modules.update({'fman': fman, 'fman.fs': fs, 'fman.url': url})

package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package
transfers = importlib.import_module('ftpclient.transfers')

assert transfers.TransferSequence.minimizable is True
assert os.path.isfile(transfers.TransferSequence.icon), \
    transfers.TransferSequence.icon
assert not getattr(transfers.TransferSequence, 'keeps_focus', False)

assert issubclass(transfers.EditUpload, transfers.TransferSequence)
assert transfers.EditUpload.keeps_focus is True
transfers.upload_edited_file(
    'file:///tmp/x/settings.php', 'ftp://host/settings.php')
assert len(submitted_tasks) == 1, submitted_tasks
upload = submitted_tasks[0]
assert isinstance(upload, transfers.EditUpload), type(upload)
assert upload.title == 'Uploading settings.php', upload.title
assert prepared_copy == [
    ('file:///tmp/x/settings.php', 'ftp://host/settings.php')], prepared_copy

print(
    'ok: transfer sequence metadata and edited-file upload task are correct'
)
