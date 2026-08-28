"""Offline self-check: a failing connect must become a readable FtpConnectionError.

Run: python test_ftp_connection_error.py
"""
import importlib.util
import os
import sys
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


def _fail(*args, **kwargs):
    raise OSError('530 Login incorrect.\nDebugging info: ftputil 3.4')


ftp.ftputil.FTPHost = _fail

try:
    with ftp.FtpWrapper('ftp://user:pw@example.com:2121/dir'):
        raise AssertionError('expected FtpConnectionError')
except ftp.FtpConnectionError as e:
    message = str(e)

assert 'ftp://user@example.com:2121' in message, message
assert '530 Login incorrect.' in message, message
assert 'pw' not in message, 'password must not leak into the alert'
assert 'Debugging info' not in message, message
print('ok:', message.replace('\n', ' | '))
