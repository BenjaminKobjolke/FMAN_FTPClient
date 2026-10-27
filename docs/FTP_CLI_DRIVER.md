# FTP CLI driver

The CLI driver lets an automatic checker exercise the plugin's FTP, FTPS, SFTP,
transfer and account-import paths against local, temporary fixtures. Each command
runs in the foreground, starts a local server only when needed, and exits when the
operation finishes. It never connects to an external server.

## Prerequisite

SFTP works without extra packages. FTP and FTPS require the test-only packages in
`test-vendor/`; create that folder with `tools\vendor_test_deps.bat`. Until then,
FTP and FTPS commands print an `error:` line naming `test-vendor/` and exit with
code 1.

## Calling the driver

From the project folder, run:

```bat
tools\ftp_cli_driver.bat <command> [arguments]
```

The optional global `--state-dir <folder>` argument must come before the command.
The default is `%TEMP%\fman-ftp-cli-driver`. A custom state folder must also be
inside the system temporary folder. State is kept between invocations so uploads,
downloads, bookmarks and duplicate detection can be checked. Use `reset` to
restore the fixture and `close` to remove its state folder.

Every remote command starts an ephemeral server on `127.0.0.1`, uses the dummy
login `user` / `secret`, and closes the server and pooled connections before it
returns. The server root is the state's `remote/` folder.

## Commands

### Reset and close

```bat
tools\ftp_cli_driver.bat reset
```

Resets the selected state folder and prints the seeded paths:

```text
/folder/source.txt
local/upload.txt
```

```bat
tools\ftp_cli_driver.bat close
```

Removes the selected state folder and prints `closed`.

### Connect and list

```bat
tools\ftp_cli_driver.bat connect sftp
tools\ftp_cli_driver.bat list sftp /folder
```

`connect` accepts `ftp`, `ftps` or `sftp`; its output includes the scheme, dummy
user, local host and random port, but never the password. `list` accepts an
absolute remote path (default `/`) and prints sorted entries, for example:

```text
connected sftp://user@127.0.0.1:43210
file source.txt 25
```

### Upload, download and delete

```bat
tools\ftp_cli_driver.bat upload sftp upload.txt /folder/uploaded.txt
tools\ftp_cli_driver.bat download sftp /folder/uploaded.txt copy.txt
tools\ftp_cli_driver.bat delete sftp /folder/uploaded.txt
```

Local names must be plain file names within the state's `local/` folder. Remote
paths must be absolute paths within the server fixture root. Example output:

```text
uploaded /folder/uploaded.txt
downloaded copy.txt (16 bytes)
deleted /folder/uploaded.txt
```

### Import sample accounts

```bat
tools\ftp_cli_driver.bat import-accounts
```

This uses `tools/ftp_cli_driver_accounts.json` through a stand-in WinSCP reader.
The fixture contains one FTP account in a folder with a remote directory, one
FTPS account, one SFTP account without a password, and one unsupported account
without a host. All hosts use the reserved `.example` domain and are never
contacted. The command runs the plugin's real import flow, saves bookmarks and
dummy credentials in the state folder, then prints the summary, bookmark aliases
and credential targets. It prints credential target names only, never passwords.
Running it again reports the already imported accounts as duplicates.

Example output includes:

```text
alert: Imported 3 account(s). Skipped 0 duplicate(s); 1 account(s) had no password.
bookmark ftp://Folder/site -> ftp://alice@ftp.example:21
credential FMAN_FTPClient:FTP:ftp://alice@ftp.example:21
```

Use `import-accounts <fixture.json>` to read another JSON fixture with the same
required fields: `name`, `host`, `user`, `password`, `port` and `protocol`.
`folder` and `remote_dir` are optional.

## Exit codes and limitations

Successful commands exit with code 0. Invalid paths, unavailable FTP test
packages, fixture errors and operation failures print `error: <message>` and exit
with code 1.

The driver cannot check fman's interface (panes, dialogs, command palette or
quicksearch), real servers, or the real WinSCP, FileZilla and NppFTP account
readers. It does not use Windows Credential Manager. Password prompts and SFTP
host-key dialogs are automatically answered for the isolated local server.
