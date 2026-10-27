"""Offline self-check: a failing connect must become a readable FtpConnectionError.

Run: python test_ftp_connection_error.py
"""
import importlib
import os
from ftp_test_support import load_ftp_module

HERE = os.path.dirname(os.path.abspath(__file__))

ftp = load_ftp_module(HERE)


def _fail(*args, **kwargs):
    raise OSError('530 Login incorrect.\nDebugging info: ftputil 3.4')


ftp.ftputil.FTPHost = _fail

try:
    with ftp.FtpWrapper('ftp://user:pw@example.com:2121/dir'):
        raise AssertionError('expected FtpConnectionError')
except importlib.import_module('ftpclient.ftp_session').FtpConnectionError as e:
    message = str(e)

assert 'ftp://user@example.com:2121' in message, message
assert '530 Login incorrect.' in message, message
assert 'pw' not in message, 'password must not leak into the alert'
assert 'Debugging info' not in message, message
print('ok:', message.replace('\n', ' | '))
