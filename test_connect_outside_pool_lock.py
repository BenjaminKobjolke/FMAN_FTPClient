"""Offline self-check: one thread waiting for a slow or silent server must not
freeze every other thread. The server may accept a connection and then never
greet - e.g. once it has as many sessions as it allows. Connecting used to
happen under the pool's global lock, so all parallel transfers, and the pane
listing, waited for that greeting forever.

Run: python test_connect_outside_pool_lock.py
"""
import importlib.util
import os
import sys
import threading
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


class FakeStatCache:
    def resize(self, size):
        pass


class FakeHost:
    def __init__(self, host, *args, **kwargs):
        if host == 'silent.example.com':
            release_silent_server.wait(10)
        self.closed = False
        self._children = []
        self.stat_cache = FakeStatCache()

    def close(self):
        self.closed = True


release_silent_server = threading.Event()
ftp.ftputil.FTPHost = FakeHost

silent = ftp.FtpWrapper('ftp://user:pw@silent.example.com/dir')
stuck = threading.Thread(target=lambda: silent.__enter__(), daemon=True)
stuck.start()
stuck.join(.2)
assert stuck.is_alive(), 'the silent server keeps its thread waiting'

done = threading.Event()


def other_transfer():
    with ftp.FtpWrapper('ftp://user:pw@fine.example.com/dir'):
        pass
    done.set()


threading.Thread(target=other_transfer, daemon=True).start()
assert done.wait(2), 'another connection must not wait for the silent server'
release_silent_server.set()
stuck.join(2)

# A server that never answers must raise after a while, not hang forever.
session = ftp.FtpSession.__new__(ftp.FtpSession)
ftp.FtpSession.connect = lambda self, host, port: None
ftp.FtpSession.login = lambda self, user, password: None
ftp.FtpSession.__init__(session, 'example.com', 21, 'user', 'pw')
assert session.timeout == ftp.SOCKET_TIMEOUT_SECS, session.timeout

print('ok: a silent server stalls only its own thread, and times out')
