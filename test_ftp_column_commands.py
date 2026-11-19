"""Focused tests for the FTP column visibility commands."""
import os
import sys
import types
import unittest
from unittest.mock import Mock


HERE = os.path.dirname(os.path.abspath(__file__))

fman = types.ModuleType('fman')


class DirectoryPaneCommand:
    pass


fman.DirectoryPaneCommand = DirectoryPaneCommand
fman_fs = types.ModuleType('fman.fs')


class Column:
    pass


fman_fs.Column = Column
fman_fs.query = lambda url, method: None
connections = types.ModuleType('ftpclient.connections')
connections.is_ftp = lambda url: True
package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules.update({
    'fman': fman,
    'fman.fs': fman_fs,
    'ftpclient': package,
    'ftpclient.connections': connections,
})

from ftpclient.column_commands import (  # noqa: E402
    ToggleFtpGroupColumn,
    ToggleFtpOwnerColumn,
    ToggleFtpPermissionsColumn,
)


class ToggleFtpColumnTests(unittest.TestCase):
    def test_commands_toggle_their_qualified_columns_once(self):
        cases = (
            (ToggleFtpPermissionsColumn, 'ftpclient.columns.Permissions'),
            (ToggleFtpOwnerColumn, 'ftpclient.columns.Owner'),
            (ToggleFtpGroupColumn, 'ftpclient.columns.Group'),
        )
        for command_type, column in cases:
            with self.subTest(command=command_type.__name__):
                command = command_type()
                command.pane = Mock(spec=['run_command'])

                command()

                command.pane.run_command.assert_called_once_with(
                    'toggle_column', {'column': column})

    def test_commands_have_expected_palette_names(self):
        cases = (
            (ToggleFtpPermissionsColumn, 'toggle_ftp_permissions_column'),
            (ToggleFtpOwnerColumn, 'toggle_ftp_owner_column'),
            (ToggleFtpGroupColumn, 'toggle_ftp_group_column'),
        )
        for command_type, name in cases:
            with self.subTest(command=command_type.__name__):
                self.assertEqual(command_type.command_name, name)


if __name__ == '__main__':
    unittest.main()
