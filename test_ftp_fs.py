"""Offline FTP and FTPS integration checks using a local server.

Run: python test_ftp_fs.py
"""
import ftplib
import importlib
import io
import os
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_VENDOR = os.path.join(HERE, 'test-vendor')


def main() -> None:
    if not all(os.path.isdir(os.path.join(TEST_VENDOR, package)) for package in (
            'pyftpdlib', 'OpenSSL')):
        print('SKIP: test-vendor/ is missing; run tools\\vendor_test_deps.bat')
        return

    import sftp_test_support
    from ftp_server_support import (
        FTP_TEST_PASSWORD, FTP_TEST_USER, FtpTestServer,
    )

    ftp = importlib.import_module('ftpclient.ftp')
    ftp_session = importlib.import_module('ftpclient.ftp_session')
    filesystems = importlib.import_module('ftpclient.filesystems')

    try:
        for scheme, filesystem_type in (
                ('ftp://', filesystems.FtpFs),
                ('ftps://', filesystems.FtpsFs)):
            server = FtpTestServer(tls=scheme == 'ftps://')
            try:
                os.makedirs(os.path.join(server.root, 'folder'))
                with open(
                    os.path.join(server.root, 'folder', 'source.txt'), 'wb'
                ) as stream:
                    stream.write(b'local ftp server')

                url = '%s%s:%s@127.0.0.1:%d/folder/source.txt' % (
                    scheme, FTP_TEST_USER, FTP_TEST_PASSWORD, server.port)
                with ftp.FtpWrapper(url) as wrapper:
                    assert wrapper.conn.path.getsize(wrapper.path) == 16
                    assert wrapper.conn.listdir('/folder') == ['source.txt']
                    wrapper.conn.makedirs('/folder/new')
                    wrapper.conn.rename('/folder/new', '/folder/renamed')

                    upload_url = url.replace('source.txt', 'uploaded.txt')
                    uploaded_chunks = []
                    with ftp.FtpWrapper(upload_url) as upload_wrapper:
                        upload_wrapper.upload(
                            io.BytesIO(b'round trip'), uploaded_chunks.append)
                    assert uploaded_chunks == [b'round trip'], uploaded_chunks

                    downloaded = io.BytesIO()
                    downloaded_chunks = []
                    with ftp.FtpWrapper(upload_url) as download_wrapper:
                        download_wrapper.download(
                            downloaded, downloaded_chunks.append)
                        if scheme == 'ftps://':
                            assert isinstance(
                                download_wrapper.conn._session, ftplib.FTP_TLS)
                    assert downloaded.getvalue() == b'round trip'
                    assert downloaded_chunks == [b'round trip'], downloaded_chunks

                filesystem = filesystem_type()
                path = '%s:%s@127.0.0.1:%d/folder' % (
                    FTP_TEST_USER, FTP_TEST_PASSWORD, server.port)
                assert sorted(filesystem.iterdir(path)) == [
                    'renamed', 'source.txt', 'uploaded.txt']
                filesystem.mkdir(path + '/created')
                filesystem.touch(path + '/created/empty.txt')
                filesystem.move(
                    scheme + path + '/uploaded.txt',
                    scheme + path + '/moved.txt')
                assert os.path.isfile(
                    os.path.join(server.root, 'folder', 'moved.txt'))
                filesystem.delete(path + '/moved.txt')
                filesystem.delete(path + '/created')
                assert not os.path.exists(
                    os.path.join(server.root, 'folder', 'moved.txt'))
                assert not os.path.exists(
                    os.path.join(server.root, 'folder', 'created'))

                wrong_password_url = url.replace(
                    FTP_TEST_PASSWORD + '@', 'wrong@')
                try:
                    with ftp.FtpWrapper(wrong_password_url):
                        raise AssertionError(
                            'expected the server to reject a password')
                except ftp_session.FtpConnectionError:
                    pass

                print('ok: %s auth, filesystem and transfer operations' % scheme[:-3])
            finally:
                ftp.FtpWrapper.close_all_connections()
                server.close()
    finally:
        ftp.FtpWrapper.close_all_connections()
        shutil.rmtree(sftp_test_support.temp_root, ignore_errors=True)


if __name__ == '__main__':
    main()
