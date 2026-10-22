## Unreleased

Features:

- F4 edits FTP files in the configured editor, with every save uploaded
  automatically. Enter uses the same editing flow.
- Copying or moving a folder from or to an FTP server starts transferring right
  away instead of walking the whole tree first.
- Downloads and uploads transfer three files at once, each on its own
  connection. The progress dialog lists the workers; `−` and `+` remove or add
  one while the transfer runs, and a removed worker finishes its file first.
  Needs an fman++ build with parallel transfer support.
- Moves go file by file: each source file is deleted right after its own copy,
  and a source folder once it is empty. A canceled or failed move leaves the
  files not yet moved in the source.

Bugfixes:

- Deleting on FTP now asks to delete permanently instead of offering the Recycle Bin.
- Move to trash on FTP now deletes permanently instead of failing with "FTP has no Trash support".
- Creating a new file on FTP no longer fails with `PermissionError`, and the
  file opens in the configured editor. FTP panes update after deleting,
  renaming or creating files and folders without pressing Ctrl+R.
- Transfers running longer than two minutes no longer fail with
  `ConnectionResetError`: the idle-connection cleanup skips connections that
  are still in use.

## [1.0.1](https://github.com/crimoniv/FTPClient/releases/tag/v1.0.1) (2018-06-20)

Bugfixes:

- Inverse History order so the most accessed connections appear first.
- Avoid listing files/directories on URLs without connection details.

Others:

- Improved README.
- PEP8 and Flake8 cleanup.

## [1.0.0](https://github.com/crimoniv/FTPClient/releases/tag/v1.0.0) (2018-06-20)

Initial version.
