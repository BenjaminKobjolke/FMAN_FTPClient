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
        pass


fman = types.ModuleType('fman')
fman.__path__ = []
fman.Task = Task
fman.show_status_message = lambda *args, **kwargs: None
fman.load_json = lambda name, default=None, save_on_quit=False: {}

fs = types.ModuleType('fman.fs')
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

print(
    'ok: transfer sequence is minimizable, has an icon, and keeps '
    'default focus behavior'
)
