"""Import saved sites from WinSCP, FileZilla, and NppFTP as FTP bookmarks."""
import os
import sys
from urllib.parse import quote

from fman import DirectoryPaneCommand, QuicksearchItem, show_alert, show_quicksearch

try:
    import ftp_account_readers
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), 'vendor-import'))
    import ftp_account_readers

from . import credentials
from .bookmarks import bookmark_targets, save_bookmark
from .ftp_session import format_base_url


SOURCE_LABELS = {
    'winscp': 'WinSCP',
    'filezilla': 'FileZilla',
    'nppftp': 'NppFTP',
}
SUPPORTED_SCHEMES = ('ftp', 'ftps', 'sftp')
NO_SOURCES_MESSAGE = 'No supported FTP account sources are available.'
PASSWORDS_NOT_STORED_MESSAGE = \
    'Passwords were not stored because Windows Credential Manager is unavailable.'


class ImportFtpAccounts(DirectoryPaneCommand):
    """Import accounts from one supported application into FTP bookmarks."""

    command_name = 'import_ftp_accounts'

    def __call__(self):
        sources = self._available_sources()
        if not sources:
            show_alert(NO_SOURCES_MESSAGE)
            return

        result = show_quicksearch(lambda query: self._source_items(sources, query))
        if not result or not result[1]:
            return
        source = next((key for key, label in sources.items()
                       if label == result[1]), None)
        if source is None:
            return

        try:
            accounts = self._read_source(source)
        except Exception as error:
            show_alert('Could not read %s accounts: %s' % (
                sources[source], error))
            return

        accounts = [account for account in accounts if self._is_supported(account)]
        if not accounts:
            show_alert('No supported accounts were found in %s.' % sources[source])
            return

        result = show_quicksearch(
            lambda query: self._account_items(accounts, query))
        if not result or not result[1]:
            return
        all_label = 'Import all (%d)' % len(accounts)
        if result[1] == all_label:
            selected = accounts
        else:
            selected = [account for account in accounts
                        if account.describe() == result[1]][:1]
        self._import_accounts(selected)

    @staticmethod
    def _available_sources():
        availability = (
            ('winscp', ftp_account_readers.is_winscp_available),
            ('filezilla', ftp_account_readers.is_filezilla_available),
            ('nppftp', ftp_account_readers.is_nppftp_available),
        )
        return {key: SOURCE_LABELS[key] for key, check in availability if check()}

    @staticmethod
    def _source_items(sources, query):
        terms = query.lower().split()
        for label in sources.values():
            if all(term in label.lower() for term in terms):
                yield QuicksearchItem(label)

    @staticmethod
    def _read_source(source):
        readers = {
            'winscp': ftp_account_readers.read_winscp,
            'filezilla': ftp_account_readers.read_filezilla,
            'nppftp': ftp_account_readers.read_nppftp,
        }
        return readers[source]()

    @staticmethod
    def _is_supported(account):
        scheme = getattr(account.protocol, 'value', account.protocol)
        return bool(account.host and account.host.strip()) and \
            scheme in SUPPORTED_SCHEMES

    @staticmethod
    def _account_items(accounts, query):
        all_label = 'Import all (%d)' % len(accounts)
        if all(term in all_label.lower() for term in query.lower().split()):
            yield QuicksearchItem(all_label)
        for account in accounts:
            if account.matches(query):
                yield QuicksearchItem(account.describe())

    def _import_accounts(self, accounts):
        targets = bookmark_targets()
        imported = 0
        duplicates = 0
        without_password = 0

        for account in accounts:
            scheme = getattr(account.protocol, 'value', account.protocol)
            base_url = format_base_url(
                scheme + '://', account.user, account.host, account.port)
            bookmark_url = format_base_url(
                scheme + '://', quote(account.user, safe=''),
                account.host, account.port)
            if base_url in targets.values() or bookmark_url in targets.values():
                duplicates += 1
                continue

            name = '%s/%s' % (account.folder, account.name) \
                if account.folder else account.name
            alias = scheme + '://' + name
            original_alias = alias
            suffix = 2
            while alias in targets:
                alias = '%s (%d)' % (original_alias, suffix)
                suffix += 1

            save_bookmark(alias, bookmark_url, account.remote_dir or '/')
            targets[alias] = bookmark_url
            if account.password:
                credentials.save_password(
                    credentials.credential_target(base_url), account.password)
            else:
                without_password += 1
            imported += 1

        summary = ('Imported %d account(s). Skipped %d duplicate(s); '
                   '%d account(s) had no password.' % (
                       imported, duplicates, without_password))
        if sys.platform != 'win32':
            summary += '\n\n' + PASSWORDS_NOT_STORED_MESSAGE
        show_alert(summary)
