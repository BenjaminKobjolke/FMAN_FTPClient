"""SFTP connections that share FTP pooling and connection tracking."""
import base64
import hashlib
import os
import sys
import threading
from typing import BinaryIO, Callable

from fman import NO, YES, show_alert
from fman.impl.util.qt.thread import run_in_main_thread

try:
    import paramiko
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), 'vendor-sftp'))
    import paramiko

from PyQt5.QtWidgets import QDialog, QInputDialog, QLineEdit

from . import credentials
from .ftp import FtpConnectionError, FtpWrapper
from .sftp_host import SftpHost


def _known_hosts_path():
    return os.path.expanduser('~/.ssh/known_hosts')


def _host_key_name(host, port):
    return host if port == 22 else '[%s]:%d' % (host, port)


@run_in_main_thread
def _confirm_host_key_dialog(host, port, fingerprint):
    return show_alert(
        'Unknown SFTP server host key for %s:%d.\n\n'
        'SHA256 fingerprint: %s\n\nTrust and save this key to %s?' % (
            host, port, fingerprint, _known_hosts_path()),
        buttons=(YES, NO)) == YES


def _confirm_host_key(host, port, fingerprint):
    return _confirm_host_key_dialog(host, port, fingerprint)


@run_in_main_thread
def _prompt_password_dialog(base_url):
    dialog = QInputDialog()
    dialog.setWindowTitle('SFTP password')
    dialog.setLabelText('Password for %s' % base_url)
    dialog.setTextEchoMode(QLineEdit.Password)
    if dialog.exec_() != QDialog.Accepted:
        return None
    return dialog.textValue()


def _prompt_password(base_url):
    return _prompt_password_dialog(base_url)


@run_in_main_thread
def _show_changed_host_key(host, port, known_hosts_path):
    show_alert(
        'SFTP host key changed for %s:%d.\n\n'
        'Remove or correct its entry in %s before reconnecting.' % (
            host, port, known_hosts_path))


def _credential_target(base_url):
    return 'FMAN_FTPClient:SFTP:' + base_url


class _TrustUnknownHost(paramiko.MissingHostKeyPolicy):
    def __init__(self, hostname, port, known_hosts_path):
        self._hostname = hostname
        self._port = port
        self._known_hosts_path = known_hosts_path

    def missing_host_key(self, client, hostname, key):
        digest = hashlib.sha256(key.asbytes()).digest()
        fingerprint = 'SHA256:' + base64.b64encode(
            digest).decode('ascii').rstrip('=')
        if not _confirm_host_key(self._hostname, self._port, fingerprint):
            raise FtpConnectionError(
                'SFTP host key was not trusted for %s:%d' % (
                    self._hostname, self._port))
        directory = os.path.dirname(self._known_hosts_path)
        os.makedirs(directory, exist_ok=True)
        with open(self._known_hosts_path, 'a+', encoding='utf-8') as known_hosts:
            known_hosts.seek(0, os.SEEK_END)
            if known_hosts.tell():
                known_hosts.seek(known_hosts.tell() - 1)
                if known_hosts.read(1) != '\n':
                    known_hosts.write('\n')
            known_hosts.write('%s %s %s\n' % (
                hostname, key.get_name(), key.get_base64()))
        client.get_host_keys().add(hostname, key.get_name(), key)


class SftpWrapper(FtpWrapper):
    """Use an SFTP host while retaining the shared FTP connection pool."""
    _login_locks = {}
    _login_locks_guard = threading.Lock()

    @classmethod
    def _login_lock(cls, base_url):
        with cls._login_locks_guard:
            return cls._login_locks.setdefault(base_url, threading.Lock())

    def _open_host(self):
        base_url = self._get_base_url()
        with self._login_lock(base_url):
            return self._login()

    def _login(self):
        client = paramiko.SSHClient()
        known_hosts_path = _known_hosts_path()
        if os.path.exists(known_hosts_path):
            client.load_host_keys(known_hosts_path)
        else:
            client._host_keys = paramiko.HostKeys()

        client.set_missing_host_key_policy(
            _TrustUnknownHost(self._host, self._port, known_hosts_path))
        target = _credential_target(self._get_base_url())
        supplied_password = self._passwd or None
        stored_password = None if supplied_password else \
            credentials.read_password(target)
        password = supplied_password or stored_password
        prompted = False

        while True:
            try:
                client.connect(
                    self._host, port=self._port, username=self._user or None,
                    password=password, allow_agent=True, look_for_keys=True,
                    timeout=30, banner_timeout=30, auth_timeout=30)
                host = SftpHost(client)
                if prompted:
                    credentials.save_password(target, password)
                return host
            except paramiko.BadHostKeyException as error:
                client.close()
                _show_changed_host_key(
                    self._host, self._port, known_hosts_path)
                raise FtpConnectionError(
                    'SFTP host key changed for %s:%d. Fix %s.' % (
                        self._host, self._port, known_hosts_path)) from error
            except FtpConnectionError:
                client.close()
                raise
            except (paramiko.AuthenticationException, paramiko.SSHException) as error:
                no_auth_methods = str(error) == 'No authentication methods available'
                if not isinstance(error, paramiko.AuthenticationException) and \
                        not (no_auth_methods and password is None):
                    client.close()
                    raise self._connection_error(error) from error
                client.close()
                if stored_password is not None:
                    credentials.delete_password(target)
                    stored_password = None
                    password = None
                    client = self._new_client(known_hosts_path)
                    continue
                if supplied_password:
                    raise self._connection_error(error) from error
                if prompted:
                    raise self._connection_error(error) from error
                password = _prompt_password(self._get_base_url())
                if password is None:
                    raise FtpConnectionError(
                        'SFTP connection cancelled for %s:%d' % (
                            self._host, self._port)) from error
                prompted = True
                client = self._new_client(known_hosts_path)
            except Exception as error:
                client.close()
                raise self._connection_error(error) from error

    def _new_client(self, known_hosts_path):
        client = paramiko.SSHClient()
        if os.path.exists(known_hosts_path):
            client.load_host_keys(known_hosts_path)
        else:
            client._host_keys = paramiko.HostKeys()
        client.set_missing_host_key_policy(
            _TrustUnknownHost(self._host, self._port, known_hosts_path))
        return client

    def _connection_error(self, error):
        reason = str(error).splitlines()[0] if str(error) else repr(error)
        return FtpConnectionError(
            'Could not connect to %s%s@%s:%d\n\n%s' % (
                self._scheme, self._user, self._host, self._port, reason))

    def _check_alive(self, host):
        return not host.closed

    def _after_use(self, host):
        pass

    def upload(
        self, src_file: BinaryIO,
        on_chunk: Callable[[bytes], None] | None = None,
    ) -> None:
        with self.conn.open(self.path, 'wb') as destination:
            self.conn.copyfileobj(src_file, destination, on_chunk)

    def download(
        self, dst_file: BinaryIO, on_chunk: Callable[[bytes], None],
    ) -> None:
        with self.conn.open(self.path, 'rb') as source:
            self.conn.copyfileobj(source, dst_file, on_chunk)
