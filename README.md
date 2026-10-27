# FTPClient

A [fman++](https://fman.io/) FTP and SFTP client. FTP support uses the powerful [ftputil](https://ftputil.sschwarzer.net) library.

## Usage

### Commands

- **Edit remote file** (F4 / Enter): Open an FTP or SFTP file in the configured editor or default app. Saves upload automatically while the “Done editing” dialog is open; click OK when finished.
- **Open ftp location** (`open_ftp_location`): Connect to an FTP, FTPS or SFTP server using its URL.
- **Add ftp bookmark** (`add_ftp_bookmark`): Bookmark the current or a custom FTP/SFTP URL.
- **Import FTP accounts** (`import_ftp_accounts`): Import saved sites from WinSCP, FileZilla or NppFTP as FTP/SFTP bookmarks.
- **Open ftp bookmark** (`open_ftp_bookmark`): Open a bookmarked FTP/SFTP URL.
- **Remove ftp bookmark** (`remove_ftp_bookmark`): Remove a bookmarked URL.
- **Open ftp history** (`open_ftp_history`): Open a previous URL.
- **Remove ftp history** (`remove_ftp_history`): Remove the whole connection history.
- **Navigate to open FTP connection** (`navigate_to_open_ftp_connection`): Show list of active FTP connections and navigate to the last visited path.
- **Close individual FTP connection** (`close_individual_ftp_connection`): Show list of active FTP connections and close the selected one.
- **Close FTP connections** (`close_ftp_connections`): Manually close all active FTP connections and navigate to home directory.
- **Toggle FTP detailed stats** (`toggle_ftp_detailed_stats`): Toggle between showing full file details (size, date, permissions) or just filenames for faster directory listings.
- **Copy FTP web URL** (`copy_ftp_web_url`): Copy the web URL for the current FTP path or selected file to clipboard.
- **Change FTP web URL** (`change_ftp_web_url`): Change the base web URL associated with the current FTP bookmark.
- **Open FTP web URL** (`open_ftp_web_url`): Open the web URL for the current FTP path or selected file in the default browser.

### Connection URL

Use one of these URL formats:

```
ftp[s]://[user[:password]@]ftp.host[:port][/path/to/dir]
sftp://[user[:password]@]sftp.host[:port][/path/to/dir]
```

## Features
- Support for URL-encoded chars in user/password (e.g. `@` -> `%40`).
- Show extra file/directory attributes: **Permissions**, **Owner** and **Group**.
- Intelligent connection pooling with automatic reuse and timeout handling.
- Optimized NOOP validation caching for reduced network overhead.
- Large directory support with increased stat cache (up to 20,000 files).
- Optional fast mode: disable detailed stats for faster listings (filename-only mode).
- FTP/SFTP bookmarks and connection history.
- FTP/SFTP file view and edit support, with saved changes uploaded automatically.
- Navigate back to open FTP connections with last visited path memory.
- Web URL mapping for FTP paths (copy, open in browser).

## Performance Optimizations

This plugin includes several optimizations for better remote file access performance:

- **Connection Pooling**: Reuses FTP and SFTP connections instead of creating new ones for each operation.
- **NOOP Validation Caching**: Only validates connections every 5 seconds instead of on every operation (~99% reduction in validation overhead).
- **Large Directory Support**: Increased stat cache from 5,000 to 20,000 entries to prevent cache eviction in large directories.
- **Smart Connection Management**: Automatically closes stale connections and limits pool size to prevent "too many connections" errors.
- **Optional Fast Mode**: Toggle detailed stats on/off for faster directory listings when metadata isn't needed.
- **Streaming Transfers**: Copies and moves start while the folder tree is still being walked. The walk uses its own connection.
- **Parallel Transfers**: Downloads and uploads, copies and moves alike, run 3 files at once (`PARALLEL_TRANSFERS` in `ftpclient/transfers.py`), each on its own connection. Add or remove workers with `+` / `−` in the progress dialog; a removed worker finishes its file first. A move deletes each source file right after its own copy, and a source folder once it is empty. Server-to-server transfers stay sequential.

## Saved passwords and host keys

- SSH keys and Pageant/SSH agent logins are tried automatically, without asking for a password.
- FTP and SFTP connections read saved passwords from Windows Credential Manager. Passwords imported from WinSCP, FileZilla and NppFTP are saved there, so imported FTP sites connect without asking for a password. A password included in a URL is used for that connection and is not saved. Avoid putting passwords in URLs or bookmarks because URLs can expose them as plain text.
- Server keys are checked against `~/.ssh/known_hosts`. The first connection shows the server's SHA256 fingerprint and asks whether to trust it. Accepting appends the key to `known_hosts`; a changed key is rejected until that file is corrected.
- `~/.ssh/config` aliases and `IdentityFile` entries are not read. Paramiko searches the standard `~/.ssh/id_*` keys and uses available SSH agents.
- Paramiko and its pinned dependencies are vendored under `ftpclient/vendor-sftp/`: Paramiko 5.1.0, cryptography 50.0.2, bcrypt 5.0.0, PyNaCl 1.6.2, cffi 2.1.1, pycparser 3.12 and invoke 3.0.3. `tools/vendor_sftp_deps.bat` downloads the pinned Windows wheels to reproduce that folder when updating the bundled packages.
- FTP account readers and PyCryptodome are vendored under `ftpclient/vendor-import/`. `tools/vendor_import_deps.bat` downloads the pinned dependencies for importing accounts; the imported reader library is [python-ftp-account-readers](https://github.com/BenjaminKobjolke/python-ftp-account-readers).

## Tests

The offline checks are root-level scripts run with `python test_<name>.py`. `test_ftp_fs.py` and `test_sftp_fs.py` use local throwaway servers, so no real account is needed. The FTP server packages are test-only and live in `test-vendor/`, outside the plugin: pyftpdlib 2.2.0, pyasyncore 1.0.5, pyasynchat 1.0.5 and pyOpenSSL 26.4.0. Create that folder with `tools\vendor_test_deps.bat` (requires `uv` and internet access). `test_ftp_fs.py` skips while those packages are missing.

`tools\ftp_cli_driver.bat` provides isolated connect, list, upload, download, delete and sample account-import commands for automatic checks; see [the CLI driver guide](docs/FTP_CLI_DRIVER.md).

## TODO
- Allow setting file/folder permissions, if applicable.

## Known issues
- **Move to trash** permanently deletes files and folders because FTP and SFTP have no Trash.
- Servers that limit connections per IP may refuse some of the connections a copy opens (one for the walk, one per worker). Press `−` in the progress dialog to use fewer.
- A password typed into a connection URL is visible in plain text; prompted SFTP passwords are saved in Windows Credential Manager.
- Currently `ftputil` is loaded from a frozen copy included in the plugin source pointing to the **3.4** version. I have not found a better way to include it.

## History

See the [CHANGELOG](CHANGELOG.md).

## Credits

- Michael Herrmann ([@mherrmann](https://github.com/mherrmann)), the [fman](https://fman.io/) author.
- Stefan Schwarzer ([@sschwarzer](https://pypi.org/user/sschwarzer/)), the [ftputil](https://ftputil.sschwarzer.net) author.
- The [python-ftp-account-readers](https://github.com/BenjaminKobjolke/python-ftp-account-readers) project for WinSCP, FileZilla and NppFTP readers.

## License

See the [LICENSE](LICENSE.md) file for license rights and limitations (MIT).
