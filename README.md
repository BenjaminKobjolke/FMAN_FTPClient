# FTPClient

A [fman](https://fman.io/) FTP Client that uses the powerful [ftputil](https://ftputil.sschwarzer.net) library.

## Usage

### Commands

- **Open ftp location** (`open_ftp_location`): Connect to a FTP server using the given URL.
- **Add ftp bookmark** (`add_ftp_bookmark`): Bookmark current -or custom- URL.
- **Open ftp bookmark** (`open_ftp_bookmark`): Open a bookmarked URL.
- **Remove ftp bookmark** (`remove_ftp_bookmark`): Remove a bookmarked URL.
- **Open ftp history** (`open_ftp_history`): Open a previous URL.
- **Remove ftp history** (`remove_ftp_history`): Remove the whole connection history.
- **Close FTP connections** (`close_ftp_connections`): Manually close all active FTP connections and navigate to home directory.
- **Toggle FTP detailed stats** (`toggle_ftp_detailed_stats`): Toggle between showing full file details (size, date, permissions) or just filenames for faster directory listings.

### Connection URL

The URL must follow the format below:

```
ftp[s]://[user[:password]@]ftp.host[:port][/path/to/dir]
```

## Features
- Support for URL-encoded chars in user/password (e.g. `@` -> `%40`).
- Show extra file/directory attributes: **Permissions**, **Owner** and **Group**.
- Intelligent connection pooling with automatic reuse and timeout handling.
- Optimized NOOP validation caching for reduced network overhead.
- Large directory support with increased stat cache (up to 20,000 files).
- Optional fast mode: disable detailed stats for faster listings (filename-only mode).
- Bookmarks and connection history.
- File view/edit support.

## Performance Optimizations

This plugin includes several optimizations for better FTP performance:

- **Connection Pooling**: Reuses FTP connections instead of creating new ones for each operation.
- **NOOP Validation Caching**: Only validates connections every 5 seconds instead of on every operation (~99% reduction in validation overhead).
- **Large Directory Support**: Increased stat cache from 5,000 to 20,000 entries to prevent cache eviction in large directories.
- **Smart Connection Management**: Automatically closes stale connections and limits pool size to prevent "too many connections" errors.
- **Optional Fast Mode**: Toggle detailed stats on/off for faster directory listings when metadata isn't needed.

## TODO
- Allow setting file/folder permissions, if applicable.

## Known issues
- When editing files, there is no way to know if a file has been edited. Must be uploaded manually through the popup.
- **Create file** command shows an **editing files is not supported** alert after file creation, although file edition is enabled.
- **Move to trash** has been disabled on purpose, there is no Trash support.
- Although there is -theoretically- **FTP_TLS** support, it has not been tested.
- Passwords are stored in plain text when creating Bookmarks.
- Passwords are shown in plain text in the URL (this can be mitigated using Bookmarks).
- Currently `ftputil` is loaded from a frozen copy included in the plugin source pointing to the **3.4** version. I have not found a better way to include it.

## History

See the [CHANGELOG](CHANGELOG.md).

## Credits

- Michael Herrmann ([@mherrmann](https://github.com/mherrmann)), the [fman](https://fman.io/) author.
- Stefan Schwarzer ([@sschwarzer](https://pypi.org/user/sschwarzer/)), the [ftputil](https://ftputil.sschwarzer.net) author.

## License

See the [LICENSE](LICENSE.md) file for license rights and limitations (MIT).
