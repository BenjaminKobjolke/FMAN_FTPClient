"""Offline self-check: the idle-timeout cleanup must not close a connection
that is still inside a `with FtpWrapper(...)` block. A transfer longer than the
timeout would otherwise be killed by the next FTP access from another thread.

Run: python test_in_use_connection_not_reaped.py
"""
import os
from ftp_test_support import load_ftp_module
import time

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


pool = ftp.FtpWrapper._FtpWrapper__conn_pool
timestamps = ftp.FtpWrapper._FtpWrapper__conn_timestamps


def make_idle(wrapper):
    timestamps[wrapper.hash] = time.time() - 1000


transfer = ftp.FtpWrapper('ftp://user:pw@example.com:2121/big.bin')
# Scheme and default port are part of the protocol-neutral pool identity.
sftp = ftp.FtpWrapper('sftp://user:pw@example.com/big.bin')
assert sftp._port == 22
assert sftp.hash != ftp.FtpWrapper(
    'ftp://user:pw@example.com:22/big.bin').hash
assert sftp._get_base_url() == 'sftp://user@example.com:22'
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

# Fresh worker connections are retained regardless of pool size. The timeout,
# rather than a global size limit, reaps them after they have been idle.
workers = [
    ftp.FtpWrapper('ftp://worker{}:pw@example.com:2121/file'.format(i))
    for i in range(4)
]
worker_hosts = [FakeHost() for _ in workers]
for worker, worker_host in zip(workers, worker_hosts):
    pool[worker.hash] = worker_host
    timestamps[worker.hash] = time.time()

other._cleanup_stale_connections()
assert all(worker.hash in pool for worker in workers), \
    'fresh idle connections must not be removed because of pool size'
assert all(not worker_host.closed for worker_host in worker_hosts), \
    'fresh idle connections must stay open regardless of pool size'

for worker in workers:
    make_idle(worker)
other._cleanup_stale_connections()
assert all(worker.hash not in pool for worker in workers), \
    'connections idle beyond the timeout must be reaped'
assert all(worker_host.closed for worker_host in worker_hosts), \
    'reaped connections must be closed'

print('ok: fresh connections stay open until the idle timeout')
