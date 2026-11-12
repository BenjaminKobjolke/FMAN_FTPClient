import os
from os.path import expanduser
from urllib.parse import unquote, urlparse

from fman import \
    CANCEL, DirectoryPaneCommand, NO, Task, YES, fs, \
    load_json, save_json, submit_task, \
    show_alert, show_prompt, show_quicksearch
from fman.url import basename, join

from .connections import is_ftp
from .ftp import FtpWrapper
from .transfer_queue import QueueError, has_relative_remote, parse_queue, resolve
from .transfer_retry import (
    MAX_TRANSFER_RETRIES, parse_transfer_retries, transfer_retries,
)
from .transfers import TransferSequence
from .command_utils import goto_ftp, matching_items
from .bookmark_commands import \
    AddFtpBookmark as AddFtpBookmark, \
    OpenFtpBookmark as OpenFtpBookmark, \
    OpenFtpHistory as OpenFtpHistory, \
    RemoveFtpBookmark as RemoveFtpBookmark

_matching_items = matching_items
_goto_ftp = goto_ftp


class CreateAndEditFtpFile(DirectoryPaneCommand):
    def __call__(self):
        current_url = self.pane.get_file_under_cursor()
        default_name = ''
        if current_url and not fs.is_dir(current_url):
            default_name = basename(current_url)

        name, ok = show_prompt(
            'Enter file name to create/edit:', default_name)
        if not (name and ok):
            return

        url = join(self.pane.get_path(), name)
        if not fs.exists(url):
            fs.touch(url)
        try:
            self.pane.place_cursor_at(url)
        except ValueError:
            pass
        self.pane.run_command('open_with_editor', {'url': url})


class OpenFtpLocation(DirectoryPaneCommand):
    def __call__(self):
        text, ok = show_prompt(
            'Please enter the URL',
            default='ftp[s]://[user[:password]@]ftp.host[:port][/path] '
                    'or sftp://[user[:password]@]sftp.host[:port][/path]')
        if text and ok:
            goto_ftp(self.pane, text)
            return


class NavigateToOpenFtpConnection(DirectoryPaneCommand):
    def __call__(self):
        connections = FtpWrapper.get_open_connections()
        if not connections:
            show_alert('No open FTP/SFTP connections.\n\n'
                      'Connect to a server first using a bookmark or URL.')
            return
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            # Look up the last visited path for the selected base URL
            connections = FtpWrapper.get_open_connections()
            for base_url, last_url in connections:
                if base_url == result[1]:
                    self.pane.set_path(last_url)
                    return
            # Fallback: navigate to base URL with root path
            self.pane.set_path(result[1] + '/')

    def _get_items(self, query):
        connections = FtpWrapper.get_open_connections()
        yield from matching_items(
            (base_url for base_url, _ in connections), query)


class TransferQueue(DirectoryPaneCommand):
    """Run a selected local .fppq file through one open server connection."""

    def __call__(self):
        selected = self.pane.get_selected_files()
        if not selected:
            under_cursor = self.pane.get_file_under_cursor()
            selected = [under_cursor] if under_cursor else []
        elif isinstance(selected, str):
            selected = [selected]

        if len(selected) != 1 or not self._is_queue_file(selected[0]):
            show_alert('Select a .fppq queue file first.')
            return

        queue_url = selected[0]
        queue_path = self._local_path(queue_url)
        connections = FtpWrapper.get_open_connections()
        if not connections:
            show_alert('No open FTP/SFTP connections.\n\n'
                       'Connect to an FTP/SFTP server first, then run '
                       'Transfer queue again.')
            return

        if len(connections) == 1:
            base_url, last_url = connections[0]
        else:
            result = show_quicksearch(self._get_connection_items)
            if not result or not result[1]:
                return
            base_url = result[1]
            last_url = next(
                (last for base, last in connections if base == base_url),
                base_url + '/')

        try:
            with open(queue_path, encoding='utf-8-sig') as queue_file:
                entries = parse_queue(queue_file.read())
        except QueueError as error:
            show_alert(str(error))
            return
        except (OSError, UnicodeError) as error:
            show_alert('Could not read the queue file: %s' % error)
            return

        if not entries:
            show_alert('The queue file has no transfers.')
            return

        remote_cwd = urlparse(last_url).path or '/'
        if has_relative_remote(entries) and remote_cwd != '/':
            choice = show_alert(
                'The queue has relative remote paths. Use the current remote '
                'folder (%s)?\n\nChoose No to use the server root.' % remote_cwd,
                buttons=YES | NO | CANCEL,
                default_button=CANCEL)
            if choice == CANCEL:
                return
            if choice == NO:
                remote_cwd = '/'

        pairs, missing = resolve(
            entries, os.path.dirname(queue_path), base_url, remote_cwd)
        if missing:
            show_alert('These local upload sources do not exist:\n\n' +
                       '\n'.join(missing))
            return

        name = os.path.basename(queue_path)
        tasks = [task for src, dst in pairs
                 for task in fs.prepare_copy(src, dst)]
        settings = load_json('FTP Settings.json', default={})
        if settings.get('delete_queue_after_success', False):
            tasks.append(Task('Deleting ' + name, fn=fs.delete, args=(queue_url,)))
        submit_task(TransferSequence('Transfer queue ' + name, tasks))

    def _get_connection_items(self, query):
        connections = FtpWrapper.get_open_connections()
        yield from matching_items(
            (base_url for base_url, _ in connections), query)

    @staticmethod
    def _is_queue_file(url):
        parsed = urlparse(url)
        return (parsed.scheme.lower() == 'file' and
                parsed.netloc in ('', 'localhost') and
                os.path.splitext(unquote(parsed.path))[1].lower() == '.fppq' and
                not fs.is_dir(url))

    @staticmethod
    def _local_path(url):
        path = unquote(urlparse(url).path)
        if len(path) > 2 and path[0] == '/' and path[2] == ':':
            path = path[1:]
        return path


class CloseIndividualFtpConnection(DirectoryPaneCommand):
    def __call__(self):
        connections = FtpWrapper.get_open_connections()
        if not connections:
            show_alert('No open FTP/SFTP connections.\n\n'
                      'There are no active connections to close.')
            return
        result = show_quicksearch(self._get_items)
        if result and result[1]:
            base_url = result[1]
            # Close the selected connection
            FtpWrapper.close_connection_by_url(base_url)
            # If currently viewing that FTP, navigate to home
            current_url = self.pane.get_path()
            if is_ftp(current_url) and base_url in current_url:
                self.pane.set_path('file://' + expanduser('~'))
            show_alert('Connection closed:\n\n' + base_url)

    def _get_items(self, query):
        connections = FtpWrapper.get_open_connections()
        yield from matching_items(
            (base_url for base_url, _ in connections), query)


class RemoveFtpHistory(DirectoryPaneCommand):
    def __call__(self):
        choice = show_alert(
            'Are you sure you want to delete the FTP/SFTP connection history?',
            buttons=YES | NO,
            default_button=NO
        )
        if choice == YES:
            history = \
                load_json('FTP History.json', default={}, save_on_quit=True)
            history.clear()
            save_json('FTP History.json')


class ToggleFtpDetailedStats(DirectoryPaneCommand):
    def __call__(self):
        settings = load_json('FTP Settings.json', default={}, save_on_quit=True)

        # Toggle the setting
        current = settings.get('disable_detailed_stats', False)
        settings['disable_detailed_stats'] = not current
        save_json('FTP Settings.json')

        # Show current state
        if settings['disable_detailed_stats']:
            show_alert(
                'Remote detailed stats disabled. Only filenames will be shown.\n\n'
                'This provides faster directory listings but no file metadata.'
            )
        else:
            show_alert(
                'Remote detailed stats enabled. Full file information '
                'will be shown.\n\n'
                'This includes size, date, permissions, owner, and group.'
            )


class ToggleFtpQueueDeleteAfterSuccess(DirectoryPaneCommand):
    def __call__(self):
        settings = load_json('FTP Settings.json', default={}, save_on_quit=True)
        settings['delete_queue_after_success'] = not settings.get(
            'delete_queue_after_success', False)
        save_json('FTP Settings.json')
        if settings['delete_queue_after_success']:
            show_alert('Transfer queue files will be deleted after a successful run.')
        else:
            show_alert('Transfer queue files will be kept after a successful run.')


class SetFtpTransferRetries(DirectoryPaneCommand):
    def __call__(self):
        text, ok = show_prompt(
            'Retries per failed transfer (0-%d):' % MAX_TRANSFER_RETRIES,
            str(transfer_retries()))
        if not ok:
            return
        retries = parse_transfer_retries(text.strip())
        if retries is None:
            show_alert('Please enter a whole number from 0 to %d.'
                       % MAX_TRANSFER_RETRIES)
            return
        settings = load_json('FTP Settings.json', default={}, save_on_quit=True)
        settings['transfer_retries'] = retries
        save_json('FTP Settings.json')


class CloseFtpConnections(DirectoryPaneCommand):
    def __call__(self):
        # Close all active FTP connections
        FtpWrapper.close_all_connections()

        # Navigate to home directory if currently viewing FTP
        current_url = self.pane.get_path()
        if is_ftp(current_url):
            self.pane.set_path('file://' + expanduser('~'))

        show_alert('All FTP/SFTP connections have been closed.\n\n'
                  'You have been disconnected from the server.')
