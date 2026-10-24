"""Offline SFTP integration checks using an in-process Paramiko server.

Run: python test_sftp_fs.py
"""
import errno
import importlib
import io
import logging
import os
import posixpath
import shutil
import socket
import sys
import tempfile
import threading
import types
from urllib.parse import urlparse


HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(HERE, 'ftpclient', 'vendor-sftp'))

import paramiko
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
    def _local(self, path):
        path = posixpath.normpath('/' + path.lstrip('/'))
        local_path = os.path.abspath(os.path.join(
            temp_root, *path.lstrip('/').split('/')))
        if os.path.commonpath((temp_root, local_path)) != temp_root:
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
    def __init__(self):
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
            'sftp', paramiko.SFTPServer, LocalSftpInterface)
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


server = SftpTestServer()
other_server = None
prompted_passwords = []
trusted_hosts = []
stored_passwords = {}
try:
    sftp = importlib.import_module('ftpclient.sftp')
    filesystems = importlib.import_module('ftpclient.filesystems')

    # Isolate this integration test from any machine SSH keys or agent.
    original_connect = sftp.paramiko.SSHClient.connect

    def connect_without_local_keys(client, *args, **kwargs):
        kwargs['allow_agent'] = False
        kwargs['look_for_keys'] = False
        return original_connect(client, *args, **kwargs)

    sftp.paramiko.SSHClient.connect = connect_without_local_keys
    known_hosts_file = os.path.join(temp_root, 'known_hosts')
    sftp._known_hosts_path = lambda: known_hosts_file
    sftp._confirm_host_key = lambda host, port, fingerprint: (
        trusted_hosts.append((host, port, fingerprint)) or True)
    sftp._prompt_password = lambda base_url: (
        prompted_passwords.append(base_url) or 'secret')
    sftp.credentials.read_password = lambda target: stored_passwords.get(target)
    sftp.credentials.save_password = lambda target, password: \
        stored_passwords.__setitem__(target, password)
    sftp.credentials.delete_password = lambda target: \
        stored_passwords.pop(target, None)

    url = 'sftp://user@127.0.0.1:%d/folder/source.txt' % server.port
    wrapper_type = sftp.SftpWrapper
    with wrapper_type(url) as wrapper:
        assert wrapper.conn.path.getsize(wrapper.path) == 10
        assert wrapper.conn.listdir('/folder') == ['source.txt']
    assert len(trusted_hosts) == 1, trusted_hosts
    assert len(prompted_passwords) == 1, prompted_passwords
    assert len(stored_passwords) == 1, stored_passwords
    with open(known_hosts_file, encoding='utf-8') as stream:
        assert '[127.0.0.1]:%d ssh-rsa' % server.port in stream.read()

    target = next(iter(stored_passwords))
    prompted_passwords.clear()
    with wrapper_type(url) as wrapper:
        wrapper.conn.makedirs('/folder/new')
        wrapper.conn.rename('/folder/new', '/folder/renamed')
        with wrapper.conn.open('/folder/uploaded.txt', 'wb') as stream:
            wrapper.conn.copyfileobj(io.BytesIO(b'abcdefgh'), stream)
        assert wrapper.conn.path.exists('/folder/uploaded.txt')
        uploaded_chunks = []
        wrapper.upload(io.BytesIO(b'progress'), on_chunk=uploaded_chunks.append)
        assert uploaded_chunks == [b'progress']
        downloaded = io.BytesIO()
        downloaded_chunks = []
        wrapper.download(downloaded, downloaded_chunks.append)
        assert downloaded.getvalue() == b'progress'
        assert downloaded_chunks == [b'progress']
    assert target in stored_passwords
    assert prompted_passwords == [], prompted_passwords

    assert wrapper_type(url).hash != importlib.import_module(
        'ftpclient.ftp').FtpWrapper(
            url.replace('sftp://', 'ftp://')).hash
    assert filesystems.is_ftp(url)
    assert filesystems.SftpFs.scheme == 'sftp://'
    assert filesystems.connection_for(url).__class__ is wrapper_type
    assert filesystems.SftpFs._is_rename(
        'sftp://user@host-a.example:22/first',
        'sftp://user@host-a.example:22/second')
    assert not filesystems.SftpFs._is_rename(
        'sftp://user@host-a.example:22/first',
        'sftp://user@host-b.example:22/second')
    assert not filesystems.SftpFs._is_rename(
        'sftp://user@host-a.example:22/first',
        'sftp://other@host-a.example:22/second')
    assert not filesystems.SftpFs._is_rename(
        'sftp://user@host-a.example:22/first',
        'ftp://user@host-a.example:22/second')
    assert filesystems.SftpFs.get_parallel_transfers(
        filesystems.SftpFs(), url, url.replace('sftp://', 'ftp://')) == 1

    sftp_fs = filesystems.SftpFs()
    path_prefix = 'user@127.0.0.1:%d/folder' % server.port
    assert sorted(sftp_fs.iterdir(path_prefix)) == [
        'renamed', 'source.txt', 'uploaded.txt']
    sftp_fs.mkdir(path_prefix + '/created')
    sftp_fs.touch(path_prefix + '/created/empty.txt')
    sftp_fs.move(
        'sftp://' + path_prefix + '/uploaded.txt',
        'sftp://' + path_prefix + '/renamed-upload.txt')
    sftp_fs.delete(path_prefix + '/renamed-upload.txt')
    sftp_fs.delete(path_prefix + '/created')

    # The per-base-URL lock makes concurrent workers share one prompted secret.
    stored_passwords.clear()
    prompted_passwords.clear()
    login_errors = []

    def connect_in_worker():
        try:
            with wrapper_type(url):
                pass
        except Exception as error:
            login_errors.append(error)

    workers = [threading.Thread(target=connect_in_worker) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=8)
    assert not any(worker.is_alive() for worker in workers)
    assert not login_errors, login_errors
    assert len(prompted_passwords) == 1, prompted_passwords

    # A rejected saved password is discarded and replaced after prompting.
    importlib.import_module('ftpclient.ftp').FtpWrapper.close_all_connections()
    stored_passwords[target] = 'wrong password'
    prompted_passwords.clear()
    with wrapper_type(url):
        pass
    assert prompted_passwords == [wrapper_type(url)._get_base_url()]
    assert stored_passwords[target] == 'secret'

    # Cancellation is surfaced without retrying or storing an empty password.
    sftp._prompt_password = lambda _base_url: None
    cancelled_url = url.replace('user@', 'cancelled@')
    try:
        with wrapper_type(cancelled_url):
            raise AssertionError('expected password prompt cancellation')
    except sftp.FtpConnectionError as error:
        assert 'cancelled' in str(error).lower()

    # Unknown host rejection and a changed host key both stop before auth.
    other_server = SftpTestServer()
    sftp._confirm_host_key = lambda *_args: False
    rejected_url = 'sftp://user@127.0.0.1:%d/folder/source.txt' % \
        other_server.port
    try:
        with wrapper_type(rejected_url):
            raise AssertionError('expected unknown-host rejection')
    except sftp.FtpConnectionError as error:
        assert 'not trusted' in str(error)

    with open(known_hosts_file, 'a', encoding='utf-8') as stream:
        stream.write('[127.0.0.1]:%d ssh-rsa %s\n' % (
            other_server.port, server.host_key.get_base64()))
    try:
        with wrapper_type(rejected_url):
            raise AssertionError('expected changed-host-key rejection')
    except sftp.FtpConnectionError as error:
        assert 'host key changed' in str(error)

    # Remote copies stay sequential and work between FTP and SFTP wrappers.
    ftp_files = {'/source.txt': b'from ftp'}

    class MemoryFtpHost:
        path = types.SimpleNamespace(
            getsize=lambda path: len(ftp_files.get(path, b'')))

        def open(self, path, mode):
            if 'w' in mode:
                class StoreOnClose(io.BytesIO):
                    def close(inner_self):
                        ftp_files[path] = inner_self.getvalue()
                        super().close()
                return StoreOnClose()
            return io.BytesIO(ftp_files[path])

        def copyfileobj(self, source, destination, callback=None):
            while True:
                chunk = source.read(4)
                if not chunk:
                    return
                destination.write(chunk)
                if callback:
                    callback(chunk)

    class MemoryFtpWrapper:
        def __init__(self, remote_url):
            self.path = urlparse(remote_url).path
            self.conn = MemoryFtpHost()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    real_factory = filesystems.connection_for
    filesystems.connection_for = lambda remote_url: (
        MemoryFtpWrapper(remote_url) if remote_url.startswith('ftp://')
        else real_factory(remote_url))
    remote_sftp_url = 'sftp://user@127.0.0.1:%d/folder/from-ftp.txt' % \
        server.port
    copy_to_sftp = sftp_fs._prepare_file_copy(
        'ftp://user@memory.test/source.txt', remote_sftp_url)
    copy_to_sftp()
    with wrapper_type(remote_sftp_url) as wrapper:
        with wrapper.conn.open(wrapper.path, 'rb') as stream:
            assert stream.read() == b'from ftp'

    copy_to_ftp = sftp_fs._prepare_file_copy(
        'sftp://' + path_prefix + '/source.txt',
        'ftp://user@memory.test/from-sftp.txt')
    copy_to_ftp()
    assert ftp_files['/from-sftp.txt'] == b'progress'

    # A URL-supplied password is used once and never persisted.
    stored_passwords.clear()
    prompted_passwords.clear()
    explicit_password_url = url.replace('user@', 'user:secret@')
    with wrapper_type(explicit_password_url):
        pass
    assert stored_passwords == {}, stored_passwords
    assert prompted_passwords == [], prompted_passwords
    filesystems.connection_for = real_factory
    print('ok: SFTP auth, host trust, filesystem and transfer operations')
finally:
    try:
        importlib.import_module('ftpclient.ftp').FtpWrapper.close_all_connections()
    except Exception:
        pass
    if other_server is not None:
        other_server.close()
    server.close()
    shutil.rmtree(temp_root, ignore_errors=True)
