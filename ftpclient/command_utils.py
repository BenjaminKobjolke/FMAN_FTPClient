from fman import QuicksearchItem, show_alert

from .connections import connection_for, is_ftp
from .ftp_session import FtpConnectionError


def matching_items(names, query):
    """Yield matches with their case-insensitive occurrence highlighted."""
    for name in names:
        try:
            index = name.lower().index(query)
        except ValueError:
            continue
        yield QuicksearchItem(name, highlight=range(index, index + len(query)))


def goto_ftp(pane, url):
    """Navigate to `url`, showing a readable alert if the server refuses us."""
    if not is_ftp(url):
        pane.set_path(url)
        return
    try:
        with connection_for(url):
            pass
    except FtpConnectionError as error:
        show_alert(str(error))
        return
    pane.set_path(url)
