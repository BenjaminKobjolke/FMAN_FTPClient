from urllib.parse import urlparse

from fman import (
    DirectoryPaneCommand, NO, YES, load_json, save_json, show_alert,
    show_prompt, show_quicksearch,
)

from .bookmarks import bookmark_targets, save_bookmark
from .command_utils import goto_ftp, matching_items
from .connections import is_ftp


class OpenFtpBookmark(DirectoryPaneCommand):
    def __call__(self):
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            # Fetch bookmarks to connect to the default path
            bookmarks = \
                load_json('FTP Bookmarks.json', default={})
            bookmark = bookmarks[result[1]]
            url = urlparse(result[1])._replace(path=bookmark[1]).geturl()
            goto_ftp(self.pane, url)

    def _get_items(self, query):
        bookmarks = \
            load_json('FTP Bookmarks.json', default={})

        yield from matching_items(sorted(bookmarks.keys()), query)


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

        yield from matching_items(sorted(bookmarks.keys()), query)


class OpenFtpHistory(DirectoryPaneCommand):
    def __call__(self):
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            goto_ftp(self.pane, result[1])

    def _get_items(self, query):
        bookmarks = \
            load_json('FTP History.json', default={})

        names = (item for item, _ in sorted(
            bookmarks.items(), key=lambda entry: entry[1], reverse=True))
        yield from matching_items(names, query)
