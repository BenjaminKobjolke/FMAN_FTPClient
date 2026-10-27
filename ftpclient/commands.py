from os.path import expanduser
from urllib.parse import urlparse

from fman import \
    DirectoryPaneCommand, NO, QuicksearchItem, YES, fs, load_json, save_json, \
    show_alert, show_prompt, show_quicksearch
from fman.url import basename, join

from .bookmarks import bookmark_targets, save_bookmark
from .connections import connection_for, is_ftp
from .ftp import FtpWrapper
from .ftp_session import FtpConnectionError


def _matching_items(names, query):
    """Yield matches with their case-insensitive occurrence highlighted."""
    for name in names:
        try:
            index = name.lower().index(query)
        except ValueError:
            continue
        yield QuicksearchItem(
            name, highlight=range(index, index + len(query)))


def _goto_ftp(pane, url):
    """Navigate to `url`, showing a readable alert if the server refuses us."""
    if not is_ftp(url):
        pane.set_path(url)
        return
    try:
        with connection_for(url):
            pass
    except FtpConnectionError as e:
        show_alert(str(e))
        return
    pane.set_path(url)


class CreateAndEditFtpFile(DirectoryPaneCommand):
    def __call__(self):
        current_url = self.pane.get_file_under_cursor()
        default_name = ''
        if current_url and not fs.is_dir(current_url):
            default_name = basename(current_url)

        name, ok = show_prompt(
            'Enter file name to create/edit:', default_name)
        if not (name and ok):
            return

        url = join(self.pane.get_path(), name)
        if not fs.exists(url):
            fs.touch(url)
        try:
            self.pane.place_cursor_at(url)
        except ValueError:
            pass
        self.pane.run_command('open_with_editor', {'url': url})


class OpenFtpLocation(DirectoryPaneCommand):
    def __call__(self):
        text, ok = show_prompt(
            'Please enter the URL',
            default='ftp[s]://[user[:password]@]ftp.host[:port][/path] '
                    'or sftp://[user[:password]@]sftp.host[:port][/path]')
        if text and ok:
            _goto_ftp(self.pane, text)
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
            _goto_ftp(self.pane, url)

    def _get_items(self, query):
        bookmarks = \
            load_json('FTP Bookmarks.json', default={})

        yield from _matching_items(sorted(bookmarks.keys()), query)


class AddFtpBookmark(DirectoryPaneCommand):
    def __call__(self):
        url = self.pane.get_path()
        if not is_ftp(url):
            url = (
                'ftp[s]://user[:password]@ftp.host[:port]/some_dir '
                'or sftp://user@host[:port]/some_dir')

        url, ok = show_prompt(
            'New FTP/SFTP bookmark, please enter the URL', default=url)

        if not (url and ok):
            return
        if not is_ftp(url):
            show_alert(
                'URL must include any of the following schemes: '
                'ftp://, ftps://, sftp://')
            return

        # XXX URL is split in `(base, path)` to allow setting a default path
        u = urlparse(url)
        base = alias = u._replace(path='').geturl()
        path = u.path

        bookmarks = bookmark_targets()
        if base in bookmarks:
            # XXX if base URL points to an alias, resolve to an existing URL
            base = bookmarks[base]

        if path and path.strip('/'):
            alias += '-'.join(path.split('/'))
        alias, ok = show_prompt(
            'Please enter an alias (will override aliases with the same name)',
            default=alias)

        if not (alias and ok):
            return
        alias_scheme = urlparse(alias).scheme
        base_scheme = urlparse(base).scheme
        if not alias_scheme:
            alias = base_scheme + '://' + alias
        elif alias_scheme != base_scheme:
            show_alert('Bookmark alias must use the same scheme as its URL')
            return
        if urlparse(alias).path:
            show_alert('Aliases should not include path information')
            return

        save_bookmark(alias, base, path)


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
                save_json('FTP Bookmarks.json')

    def _get_items(self, query):
        bookmarks = \
            load_json('FTP Bookmarks.json', default={})

        yield from _matching_items(sorted(bookmarks.keys()), query)


class OpenFtpHistory(DirectoryPaneCommand):
    def __call__(self):
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            _goto_ftp(self.pane, result[1])

    def _get_items(self, query):
        bookmarks = \
            load_json('FTP History.json', default={})

        names = (item for item, _ in sorted(
            bookmarks.items(), key=lambda entry: entry[1], reverse=True))
        yield from _matching_items(names, query)


class NavigateToOpenFtpConnection(DirectoryPaneCommand):
    def __call__(self):
        connections = FtpWrapper.get_open_connections()
        if not connections:
            show_alert('No open FTP/SFTP connections.\n\n'
                      'Connect to a server first using a bookmark or URL.')
            return
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            # Look up the last visited path for the selected base URL
            connections = FtpWrapper.get_open_connections()
            for base_url, last_url in connections:
                if base_url == result[1]:
                    self.pane.set_path(last_url)
                    return
            # Fallback: navigate to base URL with root path
            self.pane.set_path(result[1] + '/')

    def _get_items(self, query):
        connections = FtpWrapper.get_open_connections()
        yield from _matching_items(
            (base_url for base_url, _ in connections), query)


class CloseIndividualFtpConnection(DirectoryPaneCommand):
    def __call__(self):
        connections = FtpWrapper.get_open_connections()
        if not connections:
            show_alert('No open FTP/SFTP connections.\n\n'
                      'There are no active connections to close.')
            return
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            base_url = result[1]
            # Close the selected connection
            FtpWrapper.close_connection_by_url(base_url)
            # If currently viewing that FTP, navigate to home
            current_url = self.pane.get_path()
            if is_ftp(current_url) and base_url in current_url:
                self.pane.set_path('file://' + expanduser('~'))
            show_alert('Connection closed:\n\n' + base_url)

    def _get_items(self, query):
        connections = FtpWrapper.get_open_connections()
        yield from _matching_items(
            (base_url for base_url, _ in connections), query)


class RemoveFtpHistory(DirectoryPaneCommand):
    def __call__(self):
        choice = show_alert(
            'Are you sure you want to delete the FTP/SFTP connection history?',
            buttons=YES | NO,
            default_button=NO
        )
        if choice == YES:
            history = \
                load_json('FTP History.json', default={}, save_on_quit=True)
            history.clear()
            save_json('FTP History.json')


class ToggleFtpDetailedStats(DirectoryPaneCommand):
    def __call__(self):
        settings = load_json('FTP Settings.json', default={}, save_on_quit=True)

        # Toggle the setting
        current = settings.get('disable_detailed_stats', False)
        settings['disable_detailed_stats'] = not current
        save_json('FTP Settings.json')

        # Show current state
        if settings['disable_detailed_stats']:
            show_alert(
                'Remote detailed stats disabled. Only filenames will be shown.\n\n'
                'This provides faster directory listings but no file metadata.'
            )
        else:
            show_alert(
                'Remote detailed stats enabled. Full file information '
                'will be shown.\n\n'
                'This includes size, date, permissions, owner, and group.'
            )


class CloseFtpConnections(DirectoryPaneCommand):
    def __call__(self):
        # Close all active FTP connections
        FtpWrapper.close_all_connections()

        # Navigate to home directory if currently viewing FTP
        current_url = self.pane.get_path()
        if is_ftp(current_url):
            self.pane.set_path('file://' + expanduser('~'))

        show_alert('All FTP/SFTP connections have been closed.\n\n'
                  'You have been disconnected from the server.')
