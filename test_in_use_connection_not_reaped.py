"""Offline self-check: the idle-timeout cleanup must not close a connection
that is still inside a `with FtpWrapper(...)` block. A transfer longer than the
timeout would otherwise be killed by the next FTP access from another thread.

Run: python test_in_use_connection_not_reaped.py
"""
import importlib.util
import os
import sys
import time
import types

HERE = os.path.dirname(os.path.abspath(__file__))

fman = types.ModuleType('fman')
fman.__path__ = []
fman.load_json = lambda name, default=None, save_on_quit=False: \
    {} if default is None else default
sys.modules['fman'] = fman

spec = importlib.util.spec_from_file_location(
    'ftpclient_ftp', os.path.join(HERE, 'ftpclient', 'ftp.py'))
ftp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ftp)


class FakeSession:
    def voidcmd(self, command):
        pass


class FakeHost:
    def __init__(self):
        self.closed = False
        self._children = []
        self._session = FakeSession()

    def close(self):
        self.closed = True


pool = ftp.FtpWrapper._FtpWrapper__conn_pool
timestamps = ftp.FtpWrapper._FtpWrapper__conn_timestamps


def make_idle(wrapper):
    timestamps[wrapper.hash] = time.time() - 1000


transfer = ftp.FtpWrapper('ftp://user:pw@example.com:2121/big.bin')
# Another user, so another pool entry: stands in for a second thread.
other = ftp.FtpWrapper('ftp://other:pw@example.com:2121/dir')
host = FakeHost()
# Put the fake straight into the pool, so no network is touched.
pool[transfer.hash] = host

with transfer:
    with transfer:
        make_idle(transfer)
        other._cleanup_stale_connections()
        assert transfer.hash in pool and not host.closed, \
            'a connection in use must survive the idle cleanup'
    make_idle(transfer)
    other._cleanup_stale_connections()
    assert transfer.hash in pool and not host.closed, \
        'leaving a nested block must not release the outer one'

other._cleanup_stale_connections()
assert transfer.hash in pool, 'the idle clock restarts when the block ends'

make_idle(transfer)
other._cleanup_stale_connections()
assert transfer.hash not in pool and host.closed, \
    'once released and idle, the connection is reaped as before'

print('ok: connections in use are not reaped by the idle cleanup')
