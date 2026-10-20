# FTP Client for fman

Browse and manage remote FTP servers from fman. Connect with an FTP or FTPS URL, navigate remote folders, and work with remote files alongside your local files.

## Features

- Connect to FTP and FTPS servers and browse remote directories.
- Create, copy, move, rename, delete, download, and upload remote files through fman's file manager operations.
- Open remote files in your editor; changes are uploaded when you finish editing.
- Reuse connections with connection pooling and manage active connections.
- Save server locations as bookmarks and revisit recent connections from history.
- View file details such as size, date, permissions, owner, and group, or turn detailed stats off for faster listings.
- Map FTP paths to a server's web URL to copy or open the matching URL in your browser.

## Commands

Use these commands from fman's command palette. File editing is also available with **F4** or **Enter**.

| Command | What it does |
| --- | --- |
| `open_ftp_location` | Connect to an FTP location from a URL. |
| `add_ftp_bookmark` | Add a server or path as a bookmark. |
| `open_ftp_bookmark` | Open a saved bookmark. |
| `remove_ftp_bookmark` | Remove a saved bookmark. |
| `open_ftp_history` | Reopen a previous FTP location. |
| `remove_ftp_history` | Clear the connection history. |
| `navigate_to_open_ftp_connection` | Return to an active connection at its last visited path. |
| `close_individual_ftp_connection` | Close a selected active connection. |
| `close_ftp_connections` | Close all active FTP connections. |
| `toggle_ftp_detailed_stats` | Toggle detailed file information for directory listings. |
| `copy_ftp_web_url` | Copy the mapped web URL for the current path or selected file. |
| `change_ftp_web_url` | Set or change the web URL associated with a bookmark. |
| `open_ftp_web_url` | Open the mapped web URL for the current path or selected file. |

To connect, use a URL in this format:

```text
ftp[s]://[user[:password]@]ftp.host[:port][/path/to/dir]
```
