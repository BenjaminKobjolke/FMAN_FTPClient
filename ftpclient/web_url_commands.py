"""Commands that map FTP paths to web URLs."""
import webbrowser
from urllib.parse import urlparse

from fman import (
    DirectoryPaneCommand, load_json, save_json, show_alert, show_prompt,
    show_status_message,
)
from fman.clipboard import set_text

from .connections import is_ftp


def _get_ftp_bookmark_info(ftp_url):
    """Return bookmark details for an FTP URL, or alert and return empty data."""
    if not is_ftp(ftp_url):
        show_alert('This command only works on FTP/SFTP paths')
        return None, None, None, None

    u = urlparse(ftp_url)
    url_without_path = u._replace(path='').geturl()
    bookmarks = load_json('FTP Bookmarks.json', default={}, save_on_quit=True)
    if url_without_path not in bookmarks:
        show_alert(
            'No bookmark found for this server.\n\n'
            'Please add a bookmark first using "Add Ftp Bookmark".'
        )
        return None, None, None, None
    bookmark = bookmarks[url_without_path]
    return url_without_path, bookmark, bookmarks, u


def _get_ftp_web_url(ftp_url):
    url_without_path, bookmark, bookmarks, parsed_url = \
        _get_ftp_bookmark_info(ftp_url)
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

        if len(bookmark) >= 2:
            bookmarks[url_without_path] = (bookmark[0], bookmark[1], base_web_url)
        else:
            bookmarks[url_without_path] = (bookmark[0], '', base_web_url)
        save_json('FTP Bookmarks.json')

    return base_web_url.rstrip('/') + parsed_url.path


class CopyFtpWebUrl(DirectoryPaneCommand):
    """Copy the web URL for an FTP file to the clipboard."""
    def __call__(self):
        selected = self.pane.get_selected_files()
        ftp_url = selected[0] if selected else self.pane.get_path()
        web_url = _get_ftp_web_url(ftp_url)
        if web_url is None:
            return
        set_text(web_url)
        show_status_message(f'Copied to clipboard: {web_url}')

    def is_visible(self):
        return is_ftp(self.pane.get_path())


class ChangeFtpWebUrl(DirectoryPaneCommand):
    """Change the web URL for the current FTP bookmark."""
    def __call__(self):
        ftp_url = self.pane.get_path()
        url_without_path, bookmark, bookmarks, _ = \
            _get_ftp_bookmark_info(ftp_url)
        if not bookmark:
            return

        current_web_url = bookmark[2] if len(bookmark) >= 3 and bookmark[2] else ''
        new_web_url, ok = show_prompt(
            'Enter the new base web URL for this server\n'
            '(e.g., https://example.com)\n'
            'Leave empty to remove the web URL', default=current_web_url)
        if not ok:
            return

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
        return is_ftp(self.pane.get_path())


class OpenFtpWebUrl(DirectoryPaneCommand):
    """Open the web URL for an FTP file in the default browser."""
    def __call__(self):
        selected = self.pane.get_selected_files()
        ftp_url = selected[0] if selected else self.pane.get_path()
        web_url = _get_ftp_web_url(ftp_url)
        if web_url is None:
            return
        try:
            webbrowser.open(web_url)
            show_status_message(f'Opened in browser: {web_url}')
        except Exception as e:
            show_alert(f'Failed to open browser: {str(e)}')

    def is_visible(self):
        return is_ftp(self.pane.get_path())
