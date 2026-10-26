"""Offline self-check: a write on one thread must invalidate the stat cache of
the pooled FTPHost another thread reads listings from.

Run: python test_stat_cache_generation.py
"""
import os
from ftp_test_support import load_ftp_module

HERE = os.path.dirname(os.path.abspath(__file__))

ftp = load_ftp_module(HERE)


class FakeStatCache:
    def __init__(self):
        self.clears = 0

    def clear(self):
        self.clears += 1


class FakeHost:
    def __init__(self):
        self.stat_cache = FakeStatCache()


wrapper = ftp.FtpWrapper('ftp://user:pw@example.com:2121/dir')
host = FakeHost()
# Put the fake straight into the pool, so no network is touched.
ftp.FtpWrapper._FtpWrapper__conn_pool[wrapper.hash] = host

assert wrapper.conn is host
assert host.stat_cache.clears == 1, 'first use primes the generation'
wrapper.conn
wrapper.conn
assert host.stat_cache.clears == 1, 'no write happened, must not re-clear'

ftp.invalidate_stat_caches()
wrapper.conn
assert host.stat_cache.clears == 2, 'a write must drop the cache once'
wrapper.conn
assert host.stat_cache.clears == 2, 'and only once per write'

del ftp.FtpWrapper._FtpWrapper__conn_pool[wrapper.hash]
print('ok: stat cache cleared once per invalidate_stat_caches()')
