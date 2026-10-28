"""Offline checks for dead-thread cleanup and connection-limit recovery."""
import os
import threading
import time

from ftp_test_support import load_ftp_module


HERE = os.path.dirname(os.path.abspath(__file__))
ftp = load_ftp_module(HERE)


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


FtpWrapper = ftp.FtpWrapper
pool = FtpWrapper._FtpWrapper__conn_pool
timestamps = FtpWrapper._FtpWrapper__conn_timestamps
base_urls = FtpWrapper._FtpWrapper__conn_base_urls
in_use = FtpWrapper._FtpWrapper__in_use


def reset_pool():
    FtpWrapper.close_all_connections()
    in_use.clear()


reset_pool()

# A connection owned by a worker is reaped on the next enter after it exits.
worker_host = FakeHost()
worker = FtpWrapper('ftp://user:pw@example.com:2121/worker')
worker._open_host = lambda: worker_host
worker_done = threading.Event()
worker_hashes = []


def open_worker_connection():
    worker_hashes.append(worker.hash)
    with worker:
        pass
    worker_done.set()


thread = threading.Thread(target=open_worker_connection)
thread.start()
thread.join()
assert worker_done.is_set()
dead_worker_hash = worker_hashes[0]
assert dead_worker_hash in pool and not worker_host.closed

next_wrapper = FtpWrapper('ftp://user:pw@another.example:2121/next')
next_host = FakeHost()
next_wrapper._open_host = lambda: next_host
with next_wrapper:
    pass
assert dead_worker_hash not in pool and worker_host.closed, \
    'a connection owned by an exited thread must be reaped'

reset_pool()

# A fresh, idle connection owned by a live worker survives another enter.
live_host = FakeHost()
live_worker = FtpWrapper('ftp://user:pw@example.com:2121/live')
live_worker._open_host = lambda: live_host
ready = threading.Event()
release = threading.Event()
live_worker_hashes = []


def hold_live_worker():
    live_worker_hashes.append(live_worker.hash)
    with live_worker:
        pass
    ready.set()
    release.wait()


thread = threading.Thread(target=hold_live_worker)
thread.start()
assert ready.wait(5), 'worker did not finish its FTP operation'

next_wrapper = FtpWrapper('ftp://user:pw@another.example:2121/next')
next_host = FakeHost()
next_wrapper._open_host = lambda: next_host
with next_wrapper:
    pass
assert live_worker_hashes[0] in pool and not live_host.closed, \
    'a young idle connection owned by a live thread must be kept'
release.set()
thread.join()
reset_pool()


def connection_limit_error():
    cause = ftp.ftputil.error.PermanentError(
        '530 Sorry, the maximum number of connections are already connected.')
    error = ftp.FtpConnectionError('Could not connect')
    error.__cause__ = cause
    return error


# A 530 connection-limit error closes same-base idle entries, keeps in-use
# entries, and uses the result of one retry.
wrapper = FtpWrapper('ftp://user:pw@example.com:2121/new')
same_base_idle = FtpWrapper('ftp://user:other@example.com:2121/idle')
same_base_in_use = FtpWrapper('ftp://user:other@example.com:2121/in-use')
other_base = FtpWrapper('ftp://user:pw@different.example:2121/idle')
idle_host, in_use_host, other_host, retry_host = (
    FakeHost(), FakeHost(), FakeHost(), FakeHost())
same_base_idle_hash = -1001
same_base_in_use_hash = -1002
other_base_hash = -1003
pool[same_base_idle_hash] = idle_host
timestamps[same_base_idle_hash] = time.time()
base_urls[same_base_idle_hash] = same_base_idle._get_base_url()
pool[same_base_in_use_hash] = in_use_host
timestamps[same_base_in_use_hash] = time.time()
base_urls[same_base_in_use_hash] = same_base_in_use._get_base_url()
in_use[same_base_in_use_hash] = 1
pool[other_base_hash] = other_host
timestamps[other_base_hash] = time.time()
base_urls[other_base_hash] = other_base._get_base_url()
open_results = [connection_limit_error(), retry_host]


def open_then_retry():
    result = open_results.pop(0)
    if isinstance(result, BaseException):
        raise result
    return result


wrapper._open_host = open_then_retry
with wrapper:
    assert pool[wrapper.hash] is retry_host
assert idle_host.closed and same_base_idle_hash not in pool, \
    'same-base idle connection must close before retry'
assert not in_use_host.closed and same_base_in_use_hash in pool, \
    'same-base connection in use must remain open'
assert not other_host.closed and other_base_hash in pool, \
    'connections for another base URL must remain open'
reset_pool()

# A second connection-limit failure propagates unchanged.
wrapper = FtpWrapper('ftp://user:pw@example.com:2121/retry-fails')
failure = connection_limit_error()
attempts = []


def fail_twice():
    attempts.append(None)
    raise failure


wrapper._open_host = fail_twice
try:
    with wrapper:
        pass
except ftp.FtpConnectionError as error:
    assert error is failure, 'the retry failure must propagate unchanged'
else:
    raise AssertionError('the second connection attempt should fail')
assert len(attempts) == 2, 'the connection-limit failure must retry once'
reset_pool()

print('ok: dead-thread cleanup and one-time connection-limit recovery')
