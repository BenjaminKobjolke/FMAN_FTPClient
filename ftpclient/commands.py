from operator import itemgetter
from urllib.parse import urlparse

from fman import \
    DirectoryPaneCommand, NO, QuicksearchItem, YES, load_json, show_alert, \
    show_prompt, show_quicksearch
from fman.url import splitscheme

from .filesystems import is_ftp
from .ftp import FtpWrapper


class OpenFtpLocation(DirectoryPaneCommand):
    def __call__(self):
        text, ok = show_prompt(
            'Please enter the URL',
            default='ftp[s]://[user[:password]@]ftp.host[:port][/path/to/dir]')
        if text and ok:
            self.pane.set_path(text)
            return


class OpenFtpBookmark(DirectoryPaneCommand):
    def __call__(self):
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            # Fetch bookmarks to connect to the default path
            bookmarks = \
                load_json('FTP Bookmarks.json', default={})
            bookmark = bookmarks[result[1]]
            url = urlparse(result[1])._replace(path=bookmark[1]).geturl()
            self.pane.set_path(url)

    def _get_items(self, query):
        bookmarks = \
            load_json('FTP Bookmarks.json', default={})

        for item in sorted(bookmarks.keys()):
            try:
                index = item.lower().index(query)
            except ValueError:
                continue
            else:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(item, highlight=highlight)


class AddFtpBookmark(DirectoryPaneCommand):
    def __call__(self):
        url = self.pane.get_path()
        if not is_ftp(url):
            url = 'ftp[s]://user[:password]@other.host[:port]/some_dir'

        url, ok = show_prompt(
            'New FTP bookmark, please enter the URL', default=url)

        if not (url and ok):
            return
        if not is_ftp(url):
            show_alert(
                'URL must include any of the following schemes: '
                'ftp://, ftps://')
            return

        bookmarks = \
            load_json('FTP Bookmarks.json', default={}, save_on_quit=True)

        # XXX URL is split in `(base, path)` to allow setting a default path
        u = urlparse(url)
        base = alias = u._replace(path='').geturl()
        path = u.path

        if base in bookmarks:
            # XXX if base URL points to an alias, resolve to an existing URL
            base = bookmarks[base][0]

        if path and path.strip('/'):
            alias += '-'.join(path.split('/'))
        alias, ok = show_prompt(
            'Please enter an alias (will override aliases with the same name)',
            default=alias)

        if not (alias and ok):
            return
        if not is_ftp(alias):
            # XXX alias must include the FTP scheme
            scheme, _ = splitscheme(base)
            alias = scheme + alias
        if urlparse(alias).path:
            show_alert('Aliases should not include path information')
            return

        bookmarks[alias] = (base, path)


class RemoveFtpBookmark(DirectoryPaneCommand):
    def __call__(self):
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            choice = show_alert(
                'Are you sure you want to delete "%s"' % (result[1],),
                buttons=YES | NO,
                default_button=NO
            )
            if choice == YES:
                bookmarks = \
                    load_json('FTP Bookmarks.json', default={}, save_on_quit=True)
                bookmarks.pop(result[1], None)

    def _get_items(self, query):
        bookmarks = \
            load_json('FTP Bookmarks.json', default={})

        for item in sorted(bookmarks.keys()):
            try:
                index = item.lower().index(query)
            except ValueError:
                continue
            else:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(item, highlight=highlight)


class OpenFtpHistory(DirectoryPaneCommand):
    def __call__(self):
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            self.pane.set_path(result[1])

    def _get_items(self, query):
        bookmarks = \
            load_json('FTP History.json', default={})

        for item, _ in sorted(
                bookmarks.items(), key=itemgetter(1), reverse=True):
            try:
                index = item.lower().index(query)
            except ValueError:
                continue
            else:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(item, highlight=highlight)


class RemoveFtpHistory(DirectoryPaneCommand):
    def __call__(self):
        choice = show_alert(
            'Are you sure you want to delete the FTP connection history?',
            buttons=YES | NO,
            default_button=NO
        )
        if choice == YES:
            history = \
                load_json('FTP History.json', default={}, save_on_quit=True)
            history.clear()


class ToggleFtpDetailedStats(DirectoryPaneCommand):
    def __call__(self):
        settings = load_json('FTP Settings.json', default={}, save_on_quit=True)

        # Toggle the setting
        current = settings.get('disable_detailed_stats', False)
        settings['disable_detailed_stats'] = not current

        # Show current state
        if settings['disable_detailed_stats']:
            show_alert('FTP detailed stats disabled. Only filenames will be shown.\n\n'
                      'This provides faster directory listings but no file metadata.')
        else:
            show_alert('FTP detailed stats enabled. Full file information will be shown.\n\n'
                      'This includes size, date, permissions, owner, and group.')


class CloseFtpConnections(DirectoryPaneCommand):
    def __call__(self):
        # Close all active FTP connections
        FtpWrapper.close_all_connections()

        # Navigate to home directory if currently viewing FTP
        current_url = self.pane.get_path()
        if is_ftp(current_url):
            from os.path import expanduser
            self.pane.set_path('file://' + expanduser('~'))

        show_alert('All FTP connections have been closed.\n\n'
                  'You have been disconnected from the FTP server.')
