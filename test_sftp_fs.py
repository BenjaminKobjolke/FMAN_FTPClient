"""Offline SFTP integration checks using an in-process Paramiko server.

Run: python test_sftp_fs.py
"""
import importlib
import io
import os
import shutil
import threading
import types
from urllib.parse import urlparse

from sftp_test_support import SftpTestServer, temp_root

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
    assert importlib.import_module('ftpclient.connections').is_ftp(url)
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

    transfers = importlib.import_module('ftpclient.transfers')
    real_factory = transfers.connection_for
    transfers.connection_for = lambda remote_url: (
        MemoryFtpWrapper(remote_url) if remote_url.startswith('ftp://')
        else real_factory(remote_url))
    remote_sftp_url = 'sftp://user@127.0.0.1:%d/folder/from-ftp.txt' % \
        server.port
    copy_to_sftp = transfers.prepare_file_copy(
        'ftp://user@memory.test/source.txt', remote_sftp_url,
        sftp_fs._notify_written)
    copy_to_sftp()
    with wrapper_type(remote_sftp_url) as wrapper:
        with wrapper.conn.open(wrapper.path, 'rb') as stream:
            assert stream.read() == b'from ftp'

    copy_to_ftp = transfers.prepare_file_copy(
        'sftp://' + path_prefix + '/source.txt',
        'ftp://user@memory.test/from-sftp.txt', sftp_fs._notify_written)
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
    transfers.connection_for = real_factory
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
