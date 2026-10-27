"""Shared offline SFTP server and fman stubs for test_sftp_fs.py."""

import errno
import importlib
import logging
import os
import posixpath
import socket
import sys
import tempfile
import threading
import types
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(HERE, 'ftpclient', 'vendor-sftp'))

paramiko = importlib.import_module('paramiko')
logging.getLogger('paramiko').setLevel(logging.CRITICAL)


class Task:
    def __init__(self, title='', size=0, fn=None, args=(), kwargs=None):
        self.title = title
        self._size = size
        self.fn = fn
        self.args = args
        self.kwargs = kwargs or {}

    def get_size(self):
        return self._size

    def check_canceled(self):
        pass

    def set_progress(self, value):
        pass


class FileSystem:
    def __init__(self):
        self.cache = types.SimpleNamespace(
            clear=lambda *args: None, put=lambda *args: None)

    def prepare_move(self, src_url, dst_url):
        return iter(())


fman = types.ModuleType('fman')
fman.__path__ = []
fman.Task = Task
fman.NO = 0
fman.YES = 1
fman.show_alert = lambda *args, **kwargs: fman.NO
fman.fs = types.SimpleNamespace(
    notify_file_added=lambda *args: None,
    notify_file_changed=lambda *args: None,
    notify_file_removed=lambda *args: None,
    is_dir=lambda *args: False,
    iterdir=lambda *args: (),
    exists=lambda *args: False,
    delete=lambda *args: None,
    makedirs=lambda *args, **kwargs: None)
fman.load_json = lambda name, default=None, save_on_quit=False: \
    {} if default is None else default
fman.show_status_message = lambda *args, **kwargs: None
fman.submit_task = lambda *args, **kwargs: None
sys.modules['fman'] = fman
fs_module = types.ModuleType('fman.fs')
fs_module.FileSystem = FileSystem
fs_module.cached = lambda method: method
fman.fs.is_dir = lambda *args: False
url_module = types.ModuleType('fman.url')
url_module.basename = lambda value: value.rsplit('/', 1)[-1]
url_module.dirname = lambda value: value.rsplit('/', 1)[0]
url_module.join = lambda left, right: left.rstrip('/') + '/' + right
url_module.splitscheme = lambda value: (
    value[:value.index('://') + 3], value[value.index('://') + 3:])
fman.url = url_module
sys.modules.update({'fman.fs': fs_module, 'fman.url': url_module})
for module_name in ('fman.impl', 'fman.impl.util', 'fman.impl.util.qt'):
    package_stub = types.ModuleType(module_name)
    package_stub.__path__ = []
    sys.modules[module_name] = package_stub
thread_module = types.ModuleType('fman.impl.util.qt.thread')
thread_module.run_in_main_thread = lambda function: function
sys.modules['fman.impl.util.qt.thread'] = thread_module
pyqt5 = types.ModuleType('PyQt5')
pyqt5.__path__ = []
qt_widgets = types.ModuleType('PyQt5.QtWidgets')
qt_widgets.QDialog = type('QDialog', (), {'Accepted': 1})
qt_widgets.QInputDialog = type('QInputDialog', (), {})
qt_widgets.QLineEdit = type('QLineEdit', (), {'Password': 1})
sys.modules.update({'PyQt5': pyqt5, 'PyQt5.QtWidgets': qt_widgets})

package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package

temp_root = tempfile.mkdtemp(prefix='ftpclient-sftp-')
os.mkdir(os.path.join(temp_root, 'folder'))
with open(os.path.join(temp_root, 'folder', 'source.txt'), 'wb') as stream:
    stream.write(b'hello sftp')


class LocalSftpHandle(paramiko.SFTPHandle):
    def __init__(self, flags, local_path):
        super().__init__(flags)
        self._stream = open(local_path, self._mode(flags))

    @staticmethod
    def _mode(flags):
        access = flags & 3
        read = access != 1
        write = access != 0
        if flags & os.O_APPEND:
            return 'ab+'
        if write and flags & os.O_TRUNC:
            return 'wb+'
        if write and flags & os.O_CREAT:
            return 'wb+'
        return 'rb+' if write and read else ('rb' if read else 'wb')

    def read(self, offset, length):
        self._stream.seek(offset)
        return self._stream.read(length)

    def write(self, offset, data):
        self._stream.seek(offset)
        self._stream.write(data)
        self._stream.flush()
        return paramiko.SFTP_OK

    def close(self):
        self._stream.close()
        return super().close()


class LocalSftpInterface(paramiko.SFTPServerInterface):
    def __init__(self, server, root: str | None = None) -> None:
        super().__init__(server)
        self._root = root or temp_root

    def _local(self, path):
        path = posixpath.normpath('/' + path.lstrip('/'))
        local_path = os.path.abspath(os.path.join(
            self._root, *path.lstrip('/').split('/')))
        if os.path.commonpath((self._root, local_path)) != self._root:
            raise OSError(errno.EACCES, 'outside test root')
        return local_path

    def list_folder(self, path):
        result = []
        for name in os.listdir(self._local(path)):
            attr = paramiko.SFTPAttributes.from_stat(
                os.stat(self._local(path + '/' + name)))
            attr.filename = name
            result.append(attr)
        return result

    def stat(self, path):
        return paramiko.SFTPAttributes.from_stat(os.stat(self._local(path)))

    def lstat(self, path):
        return paramiko.SFTPAttributes.from_stat(os.lstat(self._local(path)))

    def open(self, path, flags, attr):
        local_path = self._local(path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        if flags & os.O_CREAT and not os.path.exists(local_path):
            open(local_path, 'wb').close()
        return LocalSftpHandle(flags, local_path)

    def remove(self, path):
        os.remove(self._local(path))
        return paramiko.SFTP_OK

    def mkdir(self, path, attr):
        os.mkdir(self._local(path))
        return paramiko.SFTP_OK

    def rmdir(self, path):
        os.rmdir(self._local(path))
        return paramiko.SFTP_OK

    def rename(self, oldpath, newpath):
        os.rename(self._local(oldpath), self._local(newpath))
        return paramiko.SFTP_OK

    def canonicalize(self, path):
        return '/' + os.path.normpath(path).lstrip('/')


class PasswordServer(paramiko.ServerInterface):
    def check_auth_password(self, username, password):
        return paramiko.AUTH_SUCCESSFUL if password == 'secret' \
            else paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return 'password'

    def check_channel_request(self, kind, chanid):
        return paramiko.OPEN_SUCCEEDED if kind == 'session' \
            else paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class SftpTestServer:
    def __init__(self, root: str | None = None) -> None:
        self.root = root or temp_root
        self.host_key = paramiko.RSAKey.generate(2048)
        self.listener = socket.socket()
        self.listener.bind(('127.0.0.1', 0))
        self.listener.listen(5)
        self.port = self.listener.getsockname()[1]
        self.running = True
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        while self.running:
            try:
                client, _ = self.listener.accept()
            except OSError:
                return
            threading.Thread(
                target=self._serve_client, args=(client,), daemon=True).start()

    def _serve_client(self, client):
        transport = paramiko.Transport(client)
        transport.add_server_key(self.host_key)
        transport.set_subsystem_handler(
            'sftp', paramiko.SFTPServer, LocalSftpInterface, self.root)
        try:
            transport.start_server(server=PasswordServer())
            while transport.is_active():
                threading.Event().wait(0.02)
        except Exception:
            transport.close()

    def close(self):
        self.running = False
        self.listener.close()
        self.thread.join(timeout=2)


