import hashlib
import os
import shutil
import tempfile
import threading
from os.path import basename

from core.commands import _open_local_files
from fman.impl.util.qt.thread import run_in_main_thread
from PyQt5.QtCore import QFileSystemWatcher, QTimer

from fman import (
    DirectoryPaneListener, fs, load_json, save_json, show_alert,
    show_status_message
)
from fman.url import splitscheme

from .filesystems import is_ftp
from .ftp import FtpWrapper


class _UploadOnSave:
    def __init__(self, tmp_path, upload):
        self._tmp_path = tmp_path
        self._upload = upload
        self._last_uploaded_sha1 = self._sha1()
        self._lock = threading.Lock()
        self._watcher = None
        self._timer = None
        self._start()

    def _sha1(self):
        with open(self._tmp_path, 'rb') as file:
            return hashlib.sha1(file.read()).hexdigest()

    @run_in_main_thread
    def _start(self):
        self._watcher = QFileSystemWatcher([self._tmp_path])
        self._watcher.fileChanged.connect(self._on_file_changed)
        self._timer = QTimer()
        self._timer.setSingleShot(True)
        self._timer.setInterval(500)
        self._timer.timeout.connect(self._upload_in_thread)

    def _on_file_changed(self, _path):
        if os.path.exists(self._tmp_path) and \
                self._tmp_path not in self._watcher.files():
            self._watcher.addPath(self._tmp_path)
        self._timer.start()

    def _upload_in_thread(self):
        threading.Thread(target=self._upload_if_changed).start()

    def _upload_if_changed(self):
        with self._lock:
            try:
                digest = self._sha1()
                if digest == self._last_uploaded_sha1:
                    return
                self._upload()
            except Exception as error:
                _show_upload_error(basename(self._tmp_path), error)
            else:
                self._last_uploaded_sha1 = digest
                _show_upload_success(basename(self._tmp_path))

    @run_in_main_thread
    def stop(self):
        self._timer.stop()
        self._watcher.removePath(self._tmp_path)
        self._watcher.deleteLater()

    def flush(self):
        self._upload_if_changed()


@run_in_main_thread
def _show_upload_success(name):
    show_status_message('Uploaded %s' % name, timeout_secs=3)


@run_in_main_thread
def _show_upload_error(name, error):
    show_alert('Could not upload %s: %s' % (name, error))


class FtpListener(DirectoryPaneListener):
    def on_command(self, command_name, args):
        if command_name not in ('open_file', 'open_with_editor'):
            return

        url = args.get('url') or self.pane.get_file_under_cursor()
        if not url:
            return
        scheme, path = splitscheme(url)
        if not is_ftp(scheme) or fs.is_dir(url):
            return

        tmp_dir = tempfile.mkdtemp()
        tmp_path = os.path.join(tmp_dir, basename(path))
        tmp_url = 'file://' + tmp_path

        watcher = None
        try:
            fs.copy(url, tmp_url)
            watcher = _UploadOnSave(
                tmp_path, lambda: fs.copy(tmp_url, url)
            )
            if command_name == 'open_file':
                _open_local_files([tmp_path], self.pane)
            else:
                self.pane.run_command('open_with_editor', {'url': tmp_url})
            show_alert(
                'Editing %s.\nSaves upload to the server automatically.\n'
                'Click OK when done.' % basename(path)
            )
        finally:
            if watcher is not None:
                watcher.stop()
                watcher.flush()
            shutil.rmtree(tmp_dir, ignore_errors=True)

        return 'reload', {}

    def on_path_changed(self):
        url = self.pane.get_path()
        if not is_ftp(url):
            return
        scheme, path = splitscheme(url)
        if not path:
            # XXX avoid storing URLs with empty path
            return
        history = \
            load_json('FTP History.json', default={}, save_on_quit=True)
        history[url] = history.get(url, 0) + 1
        save_json('FTP History.json')
        # Track last visited path for open connections feature
        FtpWrapper.record_visited_path(url)
