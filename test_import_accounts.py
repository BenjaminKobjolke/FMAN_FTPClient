"""Focused import checks with fake fman and reader APIs."""
import importlib
import os
import sys
import types


HERE = os.path.dirname(os.path.abspath(__file__))
bookmarks = {
    'ftp://Folder - site': ['ftp://other@other.example:21', '/old'],
    'ftp://known-duplicate': ['ftp://eve@existing.example:21', '/old'],
}
alerts = []
passwords = []


class DirectoryPaneCommand:
    pass


class QuicksearchItem:
    def __init__(self, label, **kwargs):
        self.label = label
        self.kwargs = kwargs


fman = types.ModuleType('fman')
fman.__path__ = []
fman.DirectoryPaneCommand = DirectoryPaneCommand
fman.QuicksearchItem = QuicksearchItem
fman.load_json = lambda name, default=None, **kwargs: bookmarks


def save_json(*args, **kwargs):
    for alias, value in list(bookmarks.items()):
        if isinstance(value, tuple):
            bookmarks[alias] = list(value)


fman.save_json = save_json
fman.show_alert = lambda message, **kwargs: alerts.append(message)
selections = ['WinSCP', 'Import all (5)']


def choose_item(callback):
    choices = list(callback(''))
    selected = selections.pop(0)
    assert any(item.label == selected for item in choices), choices
    if selected.startswith('Import all'):
        assert choices[0].label == selected
    return (0, selected)


fman.show_quicksearch = choose_item
sys.modules['fman'] = fman
package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package


class Account:
    def __init__(self, name, host, user, password, port, protocol,
                 folder='', remote_dir=''):
        self.name = name
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.protocol = protocol
        self.folder = folder
        self.remote_dir = remote_dir

    def describe(self):
        return '%s (%s://%s@%s:%d)' % (
            self.name, self.protocol, self.user, self.host, self.port)

    def matches(self, query):
        return query.lower() in self.describe().lower()


accounts = [
    Account('site', 'ftp.example', 'alice', 'ftp-pass', 21, 'ftp',
            folder='Folder', remote_dir='/home'),
    Account('secure', 'ftps.example', 'bob@example', 'ftps-pass', 990,
            'ftps'),
    Account('private', 'sftp.example', 'carol', '', 22, 'sftp'),
    Account('invalid', '', 'nobody', 'bad', 21, 'ftp'),
    Account('duplicate', 'existing.example', 'eve', 'duplicate', 21, 'ftp'),
    Account('dan@nested', 'nested.example', 'dan', 'nested-pass', 21, 'ftp',
            folder='A/B'),
]
sys.path.append(os.path.join(HERE, 'ftpclient', 'vendor-import'))
import ftp_account_readers as reader  # noqa: E402

reader.is_winscp_available = lambda: True
reader.is_filezilla_available = lambda: False
reader.is_nppftp_available = lambda: False
reader.read_winscp = lambda: accounts
reader.read_filezilla = reader.read_nppftp = lambda: []

module = importlib.import_module('ftpclient.import_accounts')
credentials = importlib.import_module('ftpclient.credentials')
credentials.save_password = lambda target, password: passwords.append(
    (target, password))

command = module.ImportFtpAccounts()
command()

assert bookmarks['ftp://Folder - site (2)'] == [
    'ftp://alice@ftp.example:21', '/home']
assert bookmarks['ftps://secure'] == [
    'ftps://bob%40example@ftps.example:990', '/']
assert bookmarks['sftp://private'] == ['sftp://carol@sftp.example:22', '/']
# Aliases are opened as URLs, so their netloc must not contain '/', '@', ...
assert bookmarks['ftp://A - B - dan_nested'] == [
    'ftp://dan@nested.example:21', '/']
assert 'ftp://duplicate' not in bookmarks
assert all('invalid' not in key for key in bookmarks)
assert passwords == [
    ('FMAN_FTPClient:FTP:ftp://alice@ftp.example:21', 'ftp-pass'),
    ('FMAN_FTPClient:FTP:ftps://bob@example@ftps.example:990', 'ftps-pass'),
    ('FMAN_FTPClient:FTP:ftp://dan@nested.example:21', 'nested-pass'),
]
assert any('Skipped 1 duplicate' in message and
           '1 account(s) had no password' in message for message in alerts)
if sys.platform != 'win32':
    assert 'not stored' in alerts[-1]

before_error = dict(bookmarks)
reader.read_winscp = lambda: (_ for _ in ()).throw(ValueError('bad XML'))
selections.append('WinSCP')
command()
assert bookmarks == before_error
assert 'bad XML' in alerts[-1]
print('FTP account import checks passed')
