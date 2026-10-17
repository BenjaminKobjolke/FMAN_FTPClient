"""Offline checks for FTP editor save uploads.

Run: python test_edit_upload.py
"""
import importlib
import os
import sys
import tempfile
import threading
import types

try:
    from PyQt5.QtCore import QCoreApplication, QEventLoop, QTimer
    FAKE_QT = False
except ImportError:
    class _Signal:
        def __init__(self):
            self._callbacks = []

        def connect(self, callback):
            self._callbacks.append(callback)

        def emit(self, *args):
            for callback in self._callbacks:
                callback(*args)

    class QCoreApplication:
        @staticmethod
        def instance():
            return None

        def __init__(self, _args):
            pass

    class QEventLoop:
        def __init__(self):
            self._done = threading.Event()

        def quit(self):
            self._done.set()

        def exec_(self):
            self._done.wait()

    class QTimer:
        def __init__(self):
            self.timeout = _Signal()
            self._timer = None

        def setSingleShot(self, _value):
            pass

        def setInterval(self, interval):
            self._interval = interval

        def start(self):
            self.stop()
            self._timer = threading.Timer(
                self._interval / 1000, self.timeout.emit
            )
            self._timer.start()

        def stop(self):
            if self._timer:
                self._timer.cancel()

        @staticmethod
        def singleShot(interval, callback):
            timer = threading.Timer(interval / 1000, callback)
            timer.start()

    class QFileSystemWatcher:
        def __init__(self, paths):
            self._files = list(paths)
            self.fileChanged = _Signal()

        def files(self):
            return list(self._files)

        def addPath(self, path):
            self._files.append(path)

        def removePath(self, path):
            self._files.remove(path)

        def deleteLater(self):
            pass

    qt_core = types.ModuleType('PyQt5.QtCore')
    qt_core.QCoreApplication = QCoreApplication
    qt_core.QEventLoop = QEventLoop
    qt_core.QFileSystemWatcher = QFileSystemWatcher
    qt_core.QTimer = QTimer
    pyqt5 = types.ModuleType('PyQt5')
    pyqt5.__path__ = []
    sys.modules.update({'PyQt5': pyqt5, 'PyQt5.QtCore': qt_core})
    FAKE_QT = True

HERE = os.path.dirname(os.path.abspath(__file__))

fman = types.ModuleType('fman')
fman.__path__ = []
fman.DirectoryPaneListener = type('DirectoryPaneListener', (), {})
fman.fs = types.SimpleNamespace()
fman.load_json = lambda *args, **kwargs: {}
fman.save_json = lambda *args, **kwargs: None
fman.show_alert = lambda *args, **kwargs: None
fman.show_status_message = lambda *args, **kwargs: None

fman_url = types.ModuleType('fman.url')
fman_url.splitscheme = lambda url: tuple(url.split('://', 1))

thread = types.ModuleType('fman.impl.util.qt.thread')
thread.run_in_main_thread = lambda fn: fn

core = types.ModuleType('core')
core.__path__ = []
core_commands = types.ModuleType('core.commands')
core_commands._open_local_files = lambda *args, **kwargs: None

package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
filesystems = types.ModuleType('ftpclient.filesystems')
filesystems.is_ftp = lambda scheme: scheme == 'ftp'
ftp = types.ModuleType('ftpclient.ftp')
ftp.FtpWrapper = object

sys.modules.update({
    'fman': fman,
    'fman.url': fman_url,
    'fman.impl': types.ModuleType('fman.impl'),
    'fman.impl.util': types.ModuleType('fman.impl.util'),
    'fman.impl.util.qt': types.ModuleType('fman.impl.util.qt'),
    'fman.impl.util.qt.thread': thread,
    'core': core,
    'core.commands': core_commands,
    'ftpclient': package,
    'ftpclient.filesystems': filesystems,
    'ftpclient.ftp': ftp,
})

listeners = importlib.import_module('ftpclient.listeners')


def wait_for_upload():
    loop = QEventLoop()
    QTimer.singleShot(700, loop.quit)
    loop.exec_()


app = QCoreApplication.instance() or QCoreApplication([])


def write_file(path, content, watcher):
    with open(path, 'wb') as file:
        file.write(content)
    if FAKE_QT:
        watcher._watcher.fileChanged.emit(path)


def read_file(path):
    with open(path, 'rb') as file:
        return file.read()


with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_path = os.path.join(tmp_dir, 'settings.php')
    with open(tmp_path, 'wb') as file:
        file.write(b'initial')
    uploads = []
    watcher = listeners._UploadOnSave(
        tmp_path,
        lambda: uploads.append(read_file(tmp_path))
    )

    write_file(tmp_path, b'first save', watcher)
    wait_for_upload()
    write_file(tmp_path, b'second save', watcher)
    wait_for_upload()
    assert uploads == [b'first save', b'second save'], uploads

    write_file(tmp_path, b'second save', watcher)
    wait_for_upload()
    assert uploads == [b'first save', b'second save'], uploads

    write_file(tmp_path, b'unsaved change', watcher)
    watcher.flush()
    assert uploads == [b'first save', b'second save', b'unsaved change'], uploads
    watcher.flush()
    assert uploads == [b'first save', b'second save', b'unsaved change'], uploads
    watcher.stop()

failed_tmp_dir = os.path.join(tempfile.gettempdir(), 'ftp-edit-copy-failure')
if os.path.exists(failed_tmp_dir):
    import shutil
    shutil.rmtree(failed_tmp_dir)
os.mkdir(failed_tmp_dir)
original_mkdtemp = listeners.tempfile.mkdtemp
original_copy = fman.fs.copy if hasattr(fman.fs, 'copy') else None
original_is_dir = fman.fs.is_dir if hasattr(fman.fs, 'is_dir') else None
listeners.tempfile.mkdtemp = lambda: failed_tmp_dir
fman.fs.is_dir = lambda _url: False

def fail_copy(_source, _destination):
    raise OSError('download failed')

fman.fs.copy = fail_copy
try:
    listener = listeners.FtpListener()
    listener.pane = None
    try:
        listener.on_command('open_file', {'url': 'ftp://host/file.txt'})
    except OSError as error:
        assert str(error) == 'download failed'
    else:
        raise AssertionError('initial download failure was swallowed')
    assert not os.path.exists(failed_tmp_dir)
finally:
    listeners.tempfile.mkdtemp = original_mkdtemp
    if original_copy is None:
        del fman.fs.copy
    else:
        fman.fs.copy = original_copy
    if original_is_dir is None:
        del fman.fs.is_dir
    else:
        fman.fs.is_dir = original_is_dir

print('ok: changed saves upload once, identical saves are skipped, flush works')
