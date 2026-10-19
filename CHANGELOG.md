## Unreleased

Features:

- F4 edits FTP files in the configured editor, with every save uploaded
  automatically. Enter uses the same editing flow.
- Copying a folder from or to an FTP server starts transferring right away
  instead of walking the whole tree first. Moves still walk first, so the
  source is only deleted after everything was copied.
- Downloads and uploads transfer three files at once, each on its own
  connection. The progress dialog lists the workers; `−` and `+` remove or add
  one while the copy runs, and a removed worker finishes its file first. Needs
  an fman build with parallel transfer support.

Bugfixes:

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
