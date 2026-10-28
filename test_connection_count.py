"""Offline checks for the FTP connection count exposed to the transfer dialog."""
import os
import sys

from ftp_test_support import load_ftp_module


HERE = os.path.dirname(os.path.abspath(__file__))
ftp = load_ftp_module(HERE)
bookmarks = sys.modules['ftpclient.bookmarks']
FtpWrapper = ftp.FtpWrapper
pool = FtpWrapper._FtpWrapper__conn_pool
base_urls = FtpWrapper._FtpWrapper__conn_base_urls


class FakeHost:
    def __init__(self, children=()):
        self._children = list(children)
        self.closed = False

    def close(self):
        self.closed = True


class FakeChild:
    def close(self):
        pass


def reset_pool():
    FtpWrapper.close_all_connections()


def add_connection(conn_hash, base_url, host):
    pool[conn_hash] = host
    base_urls[conn_hash] = base_url


reset_pool()
assert FtpWrapper.count_connections('ftp://u@h/x') == 0

# Explicit default ports and paths resolve to the same server pool key.
first_host = FakeHost()
second_host = FakeHost()
other_host = FakeHost()
add_connection(1, 'ftp://u@h:21', first_host)
add_connection(2, 'ftp://u@h:21', second_host)
add_connection(3, 'ftp://u@other:21', other_host)
assert FtpWrapper.count_connections('ftp://u@h/any/path') == 2
assert FtpWrapper.count_connections('ftp://u@other/path') == 1
second_host._children.append(FakeChild())
assert FtpWrapper.count_connections('ftp://u@h/any/path') == 3
load_json = bookmarks.load_json
bookmarks.load_json = lambda name, default=None: {
    'ftp://alias': ('ftp://u@h', '/'),
}
assert FtpWrapper.count_connections('ftp://alias/any/path') == 3
bookmarks.load_json = load_json
reset_pool()

# The shared pool keeps protocols distinct and applies SFTP's default port.
add_connection(4, 'sftp://u@h:22', FakeHost())
assert FtpWrapper.count_connections('ftp://u@h/path') == 0
assert FtpWrapper.count_connections('sftp://u@h/path') == 1
reset_pool()

print('ok: connection counts include pooled sessions and child sessions')
