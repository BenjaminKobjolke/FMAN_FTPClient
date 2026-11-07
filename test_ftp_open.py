"""Offline FTP, FTPS and SFTP checks for streamed file access.

Run: python test_ftp_open.py
"""
import importlib
import logging
import os
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_VENDOR = os.path.join(HERE, 'test-vendor')
READ_DATA = (b'0123456789abcdef' * 6554)[:100 * 1024]
WRITE_DATA = b'first chunk\0second chunk\xfflast chunk'


def assert_not_in_use(ftp, url):
    wrapper = ftp.FtpWrapper(url)
    assert wrapper.hash not in ftp.FtpWrapper._FtpWrapper__in_use


def check_filesystem(filesystem, ftp, url, path, root):
    remote_file = os.path.join(root, 'folder', 'source.bin')
    os.makedirs(os.path.dirname(remote_file), exist_ok=True)
    with open(remote_file, 'wb') as stream:
        stream.write(READ_DATA)

    with filesystem.open(path + '/source.bin', 'rb') as stream:
        chunks = []
        while True:
            chunk = stream.read(16384)
            assert len(chunk) <= 16384
            if not chunk:
                break
            chunks.append(chunk)
    assert b''.join(chunks) == READ_DATA
    assert_not_in_use(ftp, url)

    with filesystem.open(path + '/written.bin', 'wb') as stream:
        assert stream.write(WRITE_DATA[:12]) == 12
        assert stream.write(WRITE_DATA[12:]) == len(WRITE_DATA) - 12
    written = os.path.join(root, 'folder', 'written.bin')
    with open(written, 'rb') as stream:
        assert stream.read() == WRITE_DATA

    with filesystem.open(path + '/written.bin', 'wb') as stream:
        stream.write(b'replaced')
    with open(written, 'rb') as stream:
        assert stream.read() == b'replaced'
    assert_not_in_use(ftp, url.replace('source.bin', 'written.bin'))

    partial_path = path + '/partial.bin'
    try:
        with filesystem.open(partial_path, 'wb') as stream:
            stream.write(b'partial')
            raise RuntimeError('abort stream')
    except RuntimeError as error:
        assert str(error) == 'abort stream'
    else:
        raise AssertionError('write context should propagate its exception')
    assert not os.path.exists(os.path.join(root, 'folder', 'partial.bin'))
    assert_not_in_use(ftp, url.replace('source.bin', 'partial.bin'))

    missing_url = url.replace('source.bin', 'missing.bin')
    try:
        filesystem.open(path + '/missing.bin', 'rb')
    except FileNotFoundError:
        pass
    else:
        raise AssertionError('missing read should raise FileNotFoundError')
    assert_not_in_use(ftp, missing_url)

    for mode in ('rb', 'wb'):
        try:
            filesystem.open(path, mode)
        except IsADirectoryError:
            pass
        else:
            raise AssertionError('%s on a folder should fail' % mode)
        assert_not_in_use(ftp, url)

    for mode in ('r', 'w', 'ab', 'r+b'):
        try:
            filesystem.open(path + '/source.bin', mode)
        except ValueError:
            pass
        else:
            raise AssertionError('unsupported mode %r should fail' % mode)
        assert_not_in_use(ftp, url)


def main() -> None:
    if not all(os.path.isdir(os.path.join(TEST_VENDOR, package)) for package in (
            'pyftpdlib', 'OpenSSL')):
        print('SKIP: test-vendor/ is missing; run tools\\vendor_test_deps.bat')
        return

    import sftp_test_support
    from ftp_server_support import FTP_TEST_PASSWORD, FTP_TEST_USER, FtpTestServer

    logging.disable(logging.CRITICAL)
    ftp = importlib.import_module('ftpclient.ftp')
    sftp = importlib.import_module('ftpclient.sftp')
    filesystems = importlib.import_module('ftpclient.filesystems')

    sftp.paramiko.SSHClient.connect = _connect_without_local_keys(
        sftp.paramiko.SSHClient.connect)
    known_hosts_file = os.path.join(sftp_test_support.temp_root, 'known_hosts')
    sftp._known_hosts_path = lambda: known_hosts_file
    sftp._confirm_host_key = lambda *_args: True
    sftp._prompt_password = lambda _base_url: 'secret'
    sftp.credentials.read_password = lambda _target: None
    sftp.credentials.save_password = lambda *_args: None
    sftp.credentials.delete_password = lambda *_args: None

    sftp_server = sftp_test_support.SftpTestServer()
    ftp.FtpWrapper.close_all_connections()
    try:
        for scheme, filesystem_type in (
                ('ftp://', filesystems.FtpFs),
                ('ftps://', filesystems.FtpsFs)):
            server = FtpTestServer(tls=scheme == 'ftps://')
            try:
                os.mkdir(os.path.join(server.root, 'folder'))
                url = '%s%s:%s@127.0.0.1:%d/folder/source.bin' % (
                    scheme, FTP_TEST_USER, FTP_TEST_PASSWORD, server.port)
                path = '%s:%s@127.0.0.1:%d/folder' % (
                    FTP_TEST_USER, FTP_TEST_PASSWORD, server.port)
                check_filesystem(
                    filesystem_type(), ftp, url, path, server.root)
            finally:
                ftp.FtpWrapper.close_all_connections()
                server.close()

        sftp_url = 'sftp://user@127.0.0.1:%d/folder/source.bin' % \
            sftp_server.port
        sftp_path = 'user@127.0.0.1:%d/folder' % sftp_server.port
        check_filesystem(
            filesystems.SftpFs(), ftp, sftp_url, sftp_path,
            sftp_test_support.temp_root)
        print('ok: FTP, FTPS and SFTP streamed open operations')
    finally:
        ftp.FtpWrapper.close_all_connections()
        sftp_server.close()
        shutil.rmtree(sftp_test_support.temp_root, ignore_errors=True)


def _connect_without_local_keys(original_connect):
    def connect(client, *args, **kwargs):
        kwargs['allow_agent'] = False
        kwargs['look_for_keys'] = False
        return original_connect(client, *args, **kwargs)
    return connect


if __name__ == '__main__':
    main()
