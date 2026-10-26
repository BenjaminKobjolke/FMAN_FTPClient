"""Focused checks for FTP Credential Manager lookup and URL precedence."""
import importlib
import os
import sys
import types


HERE = os.path.dirname(os.path.abspath(__file__))
fman = types.ModuleType('fman')
fman.load_json = lambda *args, **kwargs: {}
sys.modules['fman'] = fman
package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package

credentials = importlib.import_module('ftpclient.credentials')
ftp = importlib.import_module('ftpclient.ftp')

assert credentials.credential_target('sftp://alice@example.com:22') == \
    'FMAN_FTPClient:SFTP:sftp://alice@example.com:22'
assert credentials.credential_target('ftp://alice@example.com:21') == \
    'FMAN_FTPClient:FTP:ftp://alice@example.com:21'
assert credentials.credential_target('ftps://alice@example.com:21') == \
    'FMAN_FTPClient:FTP:ftps://alice@example.com:21'

received = []
ftp.credentials.read_password = lambda target: 'saved-password'
ftp.ftputil.FTPHost = lambda *args, **kwargs: received.append((args, kwargs)) or \
    types.SimpleNamespace(
        use_list_a_option=True,
        stat_cache=types.SimpleNamespace(resize=lambda size: None))

wrapper = ftp.FtpWrapper('ftp://alice@example.com:21/folder')
before = wrapper.hash
wrapper._open_host()
assert wrapper.hash == before
assert received[-1][0] == ('example.com', 21, 'alice', 'saved-password')

url_password = ftp.FtpWrapper(
    'ftp://alice:url-password@example.com:21/folder')
url_password._open_host()
assert received[-1][0][-1] == 'url-password'
print('FTP stored-password checks passed')
