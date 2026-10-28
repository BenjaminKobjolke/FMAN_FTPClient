"""Read and write FTP bookmarks in fman's shared bookmark store."""
from urllib.parse import ParseResult, urlparse

from fman import load_json


BOOKMARKS_FILE = 'FTP Bookmarks.json'


def resolve_bookmark(url: str) -> ParseResult:
    """Resolve a saved alias while preserving the URL's path."""
    parsed_url = urlparse(url)
    url_without_path = parsed_url._replace(path='').geturl()

    bookmarks = load_json(BOOKMARKS_FILE, default={})
    if url_without_path in bookmarks:
        parsed_url = urlparse(bookmarks[url_without_path][0])._replace(
            path=parsed_url.path)

    return parsed_url


def bookmark_targets() -> dict[str, str]:
    """Return the saved alias-to-base URL mapping."""
    bookmarks = load_json(BOOKMARKS_FILE, default={})
    return {alias: value[0] for alias, value in bookmarks.items()}


def save_bookmark(alias: str, base_url: str, default_path: str) -> None:
    """Save one bookmark while resolving aliases as AddFtpBookmark does."""
    from fman import save_json

    bookmarks = load_json(BOOKMARKS_FILE, default={}, save_on_quit=True)
    if base_url in bookmarks:
        base_url = bookmarks[base_url][0]
    bookmarks[alias] = (base_url, default_path)
    save_json(BOOKMARKS_FILE)
