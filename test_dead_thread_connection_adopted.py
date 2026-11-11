"""Offline checks for reusing idle FTP connections from finished threads."""
import os
import threading
import time

from ftp_test_support import load_ftp_module


HERE = os.path.dirname(os.path.abspath(__file__))
ftp = load_ftp_module(HERE)


class FakeSession:
    def __init__(self, fail_noop=False):
        self.fail_noop = fail_noop

    def voidcmd(self, command):
        if self.fail_noop:
            raise OSError('connection is no longer usable')


class FakeHost:
    def __init__(self, fail_noop=False):
        self.closed = False
        self._children = []
        self._session = FakeSession(fail_noop)

    def close(self):
        self.closed = True


FtpWrapper = ftp.FtpWrapper
pool = FtpWrapper._FtpWrapper__conn_pool
timestamps = FtpWrapper._FtpWrapper__conn_timestamps
threads = FtpWrapper._FtpWrapper__conn_threads
last_noop_check = FtpWrapper._FtpWrapper__last_noop_check
base_urls = FtpWrapper._FtpWrapper__conn_base_urls
in_use = FtpWrapper._FtpWrapper__in_use


def reset_pool():
    FtpWrapper.close_all_connections()
    in_use.clear()


def open_on_finished_thread(url, host):
    wrapper = FtpWrapper(url)
    saved = []

    def worker():
        wrapper._open_host = lambda: host
        saved.append(wrapper.hash)
        with wrapper:
            pass

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()
    assert not thread.is_alive()
    assert len(saved) == 1 and saved[0] in pool
    return saved[0]


reset_pool()

# A finished thread's connection is transferred to the next matching thread.
first_host = FakeHost()
dead_thread_hash = open_on_finished_thread(
    'ftp://user:pw@example.com:2121/first', first_host)
wrapper = FtpWrapper('ftp://user:pw@example.com:2121/next')
wrapper._open_host = lambda: (_ for _ in ()).throw(
    AssertionError('a matching idle connection should be adopted'))
with wrapper:
    assert pool[wrapper.hash] is first_host
assert not first_host.closed
assert all(dead_thread_hash not in mapping for mapping in (
    pool, timestamps, threads, last_noop_check, base_urls))
assert FtpWrapper.count_connections(
    'ftp://user:pw@example.com:2121/') == 1
reset_pool()

# Credentials, including the password, must match before adoption.
password_host = FakeHost()
password_hash = open_on_finished_thread(
    'ftp://user:old@example.com:2121/first', password_host)
wrapper = FtpWrapper('ftp://user:new@example.com:2121/next')
replacement_host = FakeHost()
wrapper._open_host = lambda: replacement_host
with wrapper:
    assert pool[wrapper.hash] is replacement_host
assert password_host.closed and password_hash not in pool
reset_pool()

# Adoption happens before cleanup, but cleanup still honors the old idle time.
old_host = FakeHost()
old_hash = open_on_finished_thread(
    'ftp://user:pw@example.com:2121/first', old_host)
timestamps[old_hash] = time.time() - FtpWrapper._FtpWrapper__CONNECTION_TIMEOUT - 1
wrapper = FtpWrapper('ftp://user:pw@example.com:2121/next')
replacement_host = FakeHost()
wrapper._open_host = lambda: replacement_host
with wrapper:
    assert pool[wrapper.hash] is replacement_host
assert old_host.closed and old_hash not in pool
reset_pool()

# An idle connection stays with its still-live owner thread.
live_host = FakeHost()
live_wrapper = FtpWrapper('ftp://user:pw@example.com:2121/live')
live_wrapper._open_host = lambda: live_host
ready = threading.Event()
release = threading.Event()
live_hashes = []


def hold_live_thread():
    live_hashes.append(live_wrapper.hash)
    with live_wrapper:
        pass
    ready.set()
    release.wait()


thread = threading.Thread(target=hold_live_thread)
thread.start()
assert ready.wait(5), 'live worker did not finish its FTP operation'
wrapper = FtpWrapper('ftp://user:pw@example.com:2121/next')
replacement_host = FakeHost()
wrapper._open_host = lambda: replacement_host
with wrapper:
    assert pool[wrapper.hash] is replacement_host
assert live_hashes[0] in pool and not live_host.closed
release.set()
thread.join()
reset_pool()

# An adopted host that fails its overdue NOOP check is replaced.
unhealthy_host = FakeHost(fail_noop=True)
unhealthy_hash = open_on_finished_thread(
    'ftp://user:pw@example.com:2121/first', unhealthy_host)
last_noop_check[unhealthy_hash] = (
    time.time() - FtpWrapper._FtpWrapper__NOOP_CHECK_INTERVAL - 1)
wrapper = FtpWrapper('ftp://user:pw@example.com:2121/next')
replacement_host = FakeHost()
wrapper._open_host = lambda: replacement_host
with wrapper:
    assert pool[wrapper.hash] is replacement_host
assert unhealthy_host.closed and unhealthy_hash not in pool
reset_pool()

print('ok: finished-thread FTP connection adoption')
