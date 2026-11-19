from fman import DirectoryPaneCommand

from .columns import GROUP_COLUMN_ID, OWNER_COLUMN_ID, PERMISSIONS_COLUMN_ID


def _toggle_column(pane, column_name):
    pane.run_command('toggle_column', {'column': column_name})


class ToggleFtpPermissionsColumn(DirectoryPaneCommand):
    command_name = 'toggle_ftp_permissions_column'

    def __call__(self):
        _toggle_column(self.pane, PERMISSIONS_COLUMN_ID)


class ToggleFtpOwnerColumn(DirectoryPaneCommand):
    command_name = 'toggle_ftp_owner_column'

    def __call__(self):
        _toggle_column(self.pane, OWNER_COLUMN_ID)


class ToggleFtpGroupColumn(DirectoryPaneCommand):
    command_name = 'toggle_ftp_group_column'

    def __call__(self):
        _toggle_column(self.pane, GROUP_COLUMN_ID)
