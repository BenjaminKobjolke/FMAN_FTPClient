"""Read and write FTP bookmarks in fman's shared bookmark store."""
from fman import load_json, save_json


BOOKMARKS_FILE = 'FTP Bookmarks.json'


def bookmark_targets() -> dict[str, str]:
    """Return the saved alias-to-base URL mapping."""
    bookmarks = load_json(BOOKMARKS_FILE, default={})
    return {alias: value[0] for alias, value in bookmarks.items()}


def save_bookmark(alias: str, base_url: str, default_path: str) -> None:
    """Save one bookmark while resolving aliases as AddFtpBookmark does."""
    bookmarks = load_json(BOOKMARKS_FILE, default={}, save_on_quit=True)
    if base_url in bookmarks:
        base_url = bookmarks[base_url][0]
    bookmarks[alias] = (base_url, default_path)
    save_json(BOOKMARKS_FILE)
