from operator import itemgetter
from urllib.parse import urlparse
import webbrowser

from fman import \
    DirectoryPaneCommand, NO, QuicksearchItem, YES, fs, load_json, save_json, \
    show_alert, show_prompt, show_quicksearch, show_status_message
from fman.clipboard import set_text
from fman.url import basename, join

from .bookmarks import bookmark_targets, save_bookmark
from .connections import connection_for
from .filesystems import is_ftp
from .ftp import FtpConnectionError, FtpWrapper


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
            _goto_ftp(self.pane, result[1])

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
        for base_url, _ in connections:
            try:
                index = base_url.lower().index(query)
            except ValueError:
                continue
            else:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(base_url, highlight=highlight)


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
                from os.path import expanduser
                self.pane.set_path('file://' + expanduser('~'))
            show_alert('Connection closed:\n\n' + base_url)

    def _get_items(self, query):
        connections = FtpWrapper.get_open_connections()
        for base_url, _ in connections:
            try:
                index = base_url.lower().index(query)
            except ValueError:
                continue
            else:
                highlight = range(index, index + len(query))
                yield QuicksearchItem(base_url, highlight=highlight)


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
            from os.path import expanduser
            self.pane.set_path('file://' + expanduser('~'))

        show_alert('All FTP/SFTP connections have been closed.\n\n'
                  'You have been disconnected from the server.')


def _get_ftp_bookmark_info(ftp_url):
    """
    Helper function to get bookmark information for an FTP URL.
    Returns bookmark data or four None values when the URL is invalid.
    """
    # Check if we're on an FTP path
    if not is_ftp(ftp_url):
        show_alert('This command only works on FTP/SFTP paths')
        return None, None, None, None

    # Parse the FTP URL
    u = urlparse(ftp_url)
    url_without_path = u._replace(path='').geturl()

    # Load bookmarks
    bookmarks = load_json('FTP Bookmarks.json', default={}, save_on_quit=True)

    if url_without_path not in bookmarks:
        show_alert(
            'No bookmark found for this server.\n\n'
            'Please add a bookmark first using "Add Ftp Bookmark".'
        )
        return None, None, None, None

    bookmark = bookmarks[url_without_path]
    return url_without_path, bookmark, bookmarks, u


class CopyFtpWebUrl(DirectoryPaneCommand):
    """Copy web URL for FTP file to clipboard"""

    def __call__(self):
        # Get current path or selected file
        selected = self.pane.get_selected_files()
        if selected:
            ftp_url = selected[0]
        else:
            ftp_url = self.pane.get_path()

        web_url = _get_ftp_web_url(ftp_url)
        if web_url is None:
            return

        # Copy to clipboard
        set_text(web_url)
        show_status_message(f'Copied to clipboard: {web_url}')

    def is_visible(self):
        # Only show in command palette when on FTP path
        return is_ftp(self.pane.get_path())


class ChangeFtpWebUrl(DirectoryPaneCommand):
    """Change the web URL for the current FTP bookmark"""

    def __call__(self):
        # Get current path
        ftp_url = self.pane.get_path()

        # Get bookmark info
        url_without_path, bookmark, bookmarks, u = _get_ftp_bookmark_info(ftp_url)
        if not bookmark:
            return

        # Get current web URL if set
        current_web_url = ''
        if len(bookmark) >= 3 and bookmark[2]:
            current_web_url = bookmark[2]

        # Prompt user for new web URL
        new_web_url, ok = show_prompt(
            'Enter the new base web URL for this server\n'
            '(e.g., https://example.com)\n'
            'Leave empty to remove the web URL',
            default=current_web_url
        )

        if not ok:
            return

        # Update the bookmark with new web URL
        # Bookmark structure: [ftp_url, default_path, web_url]
        if len(bookmark) >= 2:
            bookmarks[url_without_path] = (bookmark[0], bookmark[1], new_web_url)
        else:
            bookmarks[url_without_path] = (bookmark[0], '', new_web_url)
        save_json('FTP Bookmarks.json')

        if new_web_url:
            show_alert(f'Web URL updated to:\n{new_web_url}')
        else:
            show_alert('Web URL has been removed')

    def is_visible(self):
        # Only show in command palette when on FTP path
        return is_ftp(self.pane.get_path())


def _get_ftp_web_url(ftp_url):
    url_without_path, bookmark, bookmarks, parsed_url = _get_ftp_bookmark_info(ftp_url)
    if not bookmark:
        return None

    base_web_url = bookmark[2] if len(bookmark) >= 3 and bookmark[2] else None
    if not base_web_url:
        current_web_url = bookmark[2] if len(bookmark) >= 3 and bookmark[2] else ''
        base_web_url, ok = show_prompt(
            'Enter the base web URL for this server\n'
            '(e.g., https://example.com)',
            default=current_web_url if current_web_url else 'https://'
        )
        if not (base_web_url and ok):
            return None

        # Preserve the existing bookmark values while adding the web URL.
        if len(bookmark) >= 2:
            bookmarks[url_without_path] = (bookmark[0], bookmark[1], base_web_url)
        else:
            bookmarks[url_without_path] = (bookmark[0], '', base_web_url)
        save_json('FTP Bookmarks.json')

    return base_web_url.rstrip('/') + parsed_url.path


class OpenFtpWebUrl(DirectoryPaneCommand):
    """Open web URL for FTP file in default browser"""

    def __call__(self):
        # Get current path or selected file
        selected = self.pane.get_selected_files()
        if selected:
            ftp_url = selected[0]
        else:
            ftp_url = self.pane.get_path()

        web_url = _get_ftp_web_url(ftp_url)
        if web_url is None:
            return

        # Open in default browser
        try:
            webbrowser.open(web_url)
            show_status_message(f'Opened in browser: {web_url}')
        except Exception as e:
            show_alert(f'Failed to open browser: {str(e)}')

    def is_visible(self):
        # Only show in command palette when on FTP path
        return is_ftp(self.pane.get_path())
