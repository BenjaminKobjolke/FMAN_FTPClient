# FTP desktop UI driver

The desktop UI driver starts a disposable fman instance with this working-tree copy of the FTP
plugin. It adds local FTP and SFTP fixtures so automated checks can inspect the plugin's panes,
transfer windows, and dialogs without using a saved account.

## Prerequisites

- Copy `tools/ftp_ui_driver_config.example.bat` to
  `tools/ftp_ui_driver_config.bat` and set `FMAN_SOURCE_DIR` to an fman checkout.
- The fman checkout must have its `.venv` installed by that checkout's `install.bat`; the driver
  uses the checkout's `tools/find_build_python.bat` and its fman UI driver.
- SFTP fixtures use the plugin's existing vendored SFTP packages. FTP fixtures need the test-only
  packages in `test-vendor/`, installed with `tools\vendor_test_deps.bat` (requires `uv` and
  internet access). Without them, the SFTP fixture remains available and `launch` reports
  `ftp_error`.

## Isolation and safety

The driver keeps its profile, fixture trees, and session state under `%TEMP%\fman-ftp-ui\`. It
starts a separate fman process; it never attaches to the user's fman process, reads or writes
`%APPDATA%\fman`, or loads other user plugins. The private profile contains only this plugin and
the small probe plugin used to inspect its windows.

Both servers bind to loopback and use the fixture account `user` / `secret`. Password reads and
writes use a session-only file, and SSH `known_hosts` is stored in the session. SSH agent and key
file lookup are disabled; saved account import sources report unavailable. Close the driver after
each session, including when a check fails, so it stops the servers and removes the temporary
profile.

## Fixture layout

- Left pane: `single.txt` and `upload/` with ten 32 KB files plus `nested/a.txt` and
  `nested/b.txt`.
- Right pane: initially empty.
- Each remote server: `folder/source.txt`, `delete-me/` with six files and three nested files,
  and `single.txt`.
- Aliases: `ftp-fixture` and `sftp-fixture`, both with fixture user `user`, password `secret`,
  and their respective loopback server port.

`launch --delay SECONDS` waits that many seconds before each FTP/SFTP file operation. The default
is one second so transfer progress stays visible; use `--delay 0` for immediate operations. The
delay does not apply to directory listings. `reset` restores the fixture trees, but panes may
need reloading to show the reset.

## Commands

Each custom command prints one JSON result; failures print a JSON `error` and return nonzero.
Generic commands delegate to fman's UI driver and preserve its output and exit code. Commands run
in the foreground.

```bat
tools\ftp_ui_driver.bat launch
tools\ftp_ui_driver.bat launch --delay 1
tools\ftp_ui_driver.bat reset
tools\ftp_ui_driver.bat server-files ftp
tools\ftp_ui_driver.bat server-files sftp
tools\ftp_ui_driver.bat windows
tools\ftp_ui_driver.bat window-click --title "Copying upload" --button "–"
tools\ftp_ui_driver.bat minimized-click --title "Copying upload"
tools\ftp_ui_driver.bat window-screenshot --path "%TEMP%\ftp-transfer.png" --title "Copying upload"
tools\ftp_ui_driver.bat close
```

The generic fman commands are `state`, `commands`, `dialog`, `command`, `files`, `keys`, `type`,
`click`, and `screenshot`. They have the same arguments as the fman UI driver; for example:

```bat
tools\ftp_ui_driver.bat files --pane 1
tools\ftp_ui_driver.bat command open_directory "{\"url\":\"ftp://ftp-fixture/\"}" --pane 1
tools\ftp_ui_driver.bat keys F8
tools\ftp_ui_driver.bat keys Return
tools\ftp_ui_driver.bat screenshot "%TEMP%\fman-main.png"
```

Example results (session IDs, paths, progress, and fixture contents vary):

```json
{"session_id":"<id>","ftp_url":"ftp://ftp-fixture/","sftp_url":"sftp://sftp-fixture/","delay_seconds":1.0,"ftp_error":null}
{"protocol":"ftp","files":["delete-me/d01.txt","folder/source.txt","single.txt"]}
{"ok":true,"windows":[{"title":"Copying upload","texts":["Copying upload"],"buttons":[{"text":"–","enabled":true}],"value":4,"maximum":12}]}
{"ok":true,"clicked":"–","title":"Copying upload"}
{"ok":true,"restored":"Copying upload"}
{"ok":true}
```

`launch` reports the session ID, pane paths, fixture URLs, delay, and `ftp_error` (null when FTP
is ready). `server-files` returns sorted paths currently present on that server. `windows` reports
each visible window's title, visible text, optional progress value and maximum, and visible
buttons with their enabled state. The window title is based on the visible text, for example
`Copying upload` or `Deleting delete-me`. `window-click` selects a window by a case-insensitive
title substring and clicks one exact visible button label. `minimized-click` restores the matching
minimized window. `window-screenshot` writes a PNG of the matching window; omit `--title` for the
main window. Generic `screenshot` captures the main window or active dialog.

`reset` returns the seeded paths and a reload reminder. `close` stops the fixture server process
tree before closing fman and deleting the session folder.

## Example sequence for check #1556: minimize during a copy

This sequence illustrates how to inspect the transfer window and server contents; it does not
claim the check passed. Run `launch`, then copy the left pane's `upload` folder to
`ftp://ftp-fixture/`. Get the transfer title from `windows` as `Copying ` plus the displayed
name, match it by substring, click its `–` button with `window-click`, and use `screenshot` to
capture the main window's status-bar icon. Use `minimized-click` with the same title and inspect
`windows` again. Poll `server-files ftp` until all 12 files have arrived.

## Example sequence for check #1560: cancel a delete

This sequence is also an example, not a passing result. Open `sftp://sftp-fixture/`, select
`delete-me`, press F8, and confirm with Return. Read the progress and count from `windows`; get
the title from `Deleting ` plus the displayed name, match it by substring, and click the exact
`Deleting Cancel` button with `window-click`. Check `server-files sftp` to see which entries
remain. Then perform a full delete and confirm that the pane refreshes with `files --pane 1`.
Repeat the operation with `single.txt`, then repeat the folder and single-file examples over FTP
using Shift+F8.

Screenshots can support an observation, but they do not prove that an interaction occurred.

## Limits

The driver covers local fixtures only. It does not verify real saved accounts or external
servers, real account readers, Windows Credential Manager, the SFTP unknown-host-key prompt (the
fixture key is pre-trusted), the SFTP password prompt (credentials are pre-seeded),
connection-limited server behavior, operating-system dialogs, or subjective timing and
performance. In particular, it does not cover check #1576's connection-limited upload/delete
sequence.
