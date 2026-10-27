"""Focused offline checks for the FTP CLI driver.

Run: python test_ftp_cli_driver.py
"""
import os
import subprocess
import sys
import tempfile
import json


HERE = os.path.dirname(os.path.abspath(__file__))
DRIVER = os.path.join(HERE, 'tools', 'ftp_cli_driver.py')
FTP_VENDOR = os.path.join(HERE, 'test-vendor', 'pyftpdlib')
OPENSSL_VENDOR = os.path.join(HERE, 'test-vendor', 'OpenSSL')
FTP_CASES_AVAILABLE = os.path.isdir(FTP_VENDOR) and os.path.isdir(OPENSSL_VENDOR)
UPLOADED_DATA = b'upload test data'


def main() -> None:
    with tempfile.TemporaryDirectory(prefix='ftp-cli-driver-test-') as root:
        state = os.path.join(root, 'state')

        def run(*arguments):
            return subprocess.run(
                [sys.executable, DRIVER, '--state-dir', state, *arguments],
                cwd=HERE, capture_output=True, text=True, timeout=60,
                check=False)

        result = run('reset')
        assert result.returncode == 0, result.stdout + result.stderr
        assert '/folder/source.txt' in result.stdout, result.stdout
        assert 'local/upload.txt' in result.stdout, result.stdout
        assert os.path.isfile(os.path.join(state, 'remote', 'folder', 'source.txt'))

        sys.path.insert(0, os.path.dirname(DRIVER))
        from ftp_cli_driver import DriverProtocol, remote_fs_path, remote_url
        special_path = '/folder/special-#?.txt'
        assert remote_fs_path(21, special_path).endswith(
            '/folder/special-%23%3F.txt')
        assert remote_url(DriverProtocol.SFTP, 22, special_path).endswith(
            '/folder/special-%23%3F.txt')

        protocols = ['sftp']
        if FTP_CASES_AVAILABLE:
            protocols = ['ftp', 'ftps', 'sftp']
        else:
            result = run('connect', 'ftp')
            assert result.returncode == 1, result.stdout + result.stderr
            assert 'error:' in result.stdout and 'test-vendor' in result.stdout, (
                result.stdout)
            print('SKIP: FTP/FTPS driver cases, test-vendor/ is missing')

        for protocol in protocols:
            result = run('connect', protocol)
            assert result.returncode == 0, result.stdout + result.stderr
            assert 'connected ' in result.stdout, result.stdout
            assert 'secret' not in result.stdout, result.stdout

            result = run('list', protocol, '/folder')
            assert result.returncode == 0, result.stdout + result.stderr
            assert 'source.txt' in result.stdout, result.stdout
            assert 'file source.txt 25' in result.stdout, result.stdout

            remote_name = 'up-%s.txt' % protocol
            result = run('upload', protocol, 'upload.txt', '/folder/' + remote_name)
            assert result.returncode == 0, result.stdout + result.stderr
            remote_file = os.path.join(state, 'remote', 'folder', remote_name)
            assert open(remote_file, 'rb').read() == UPLOADED_DATA

            local_name = 'down-%s.txt' % protocol
            result = run('download', protocol, '/folder/' + remote_name, local_name)
            assert result.returncode == 0, result.stdout + result.stderr
            downloaded = open(
                os.path.join(state, 'local', local_name), 'rb').read()
            assert downloaded == UPLOADED_DATA

            result = run('delete', protocol, '/folder/' + remote_name)
            assert result.returncode == 0, result.stdout + result.stderr
            assert not os.path.exists(remote_file)

        result = run('connect', 'sftp', '--password', 'wrong')
        assert result.returncode == 1 and 'error:' in result.stdout, result.stdout
        if FTP_CASES_AVAILABLE:
            result = run('connect', 'ftp', '--password', 'wrong')
            assert result.returncode == 1 and 'error:' in result.stdout, result.stdout

        result = run('upload', 'sftp', '../x.txt', '/x.txt')
        assert result.returncode == 1 and 'error:' in result.stdout, result.stdout
        result = run('list', 'sftp', 'folder')
        assert result.returncode == 1 and 'error:' in result.stdout, result.stdout

        result = run('import-accounts')
        assert result.returncode == 0, result.stdout + result.stderr
        for expected in ('Imported 3 account(s)', 'ftp://Folder - site',
                         'ftps://secure', 'sftp://private'):
            assert expected in result.stdout, result.stdout
        bookmarks_path = os.path.join(state, 'fman_json', 'FTP Bookmarks.json')
        with open(bookmarks_path, encoding='utf-8') as stream:
            bookmarks = json.load(stream)
        assert set(bookmarks) >= {
            'ftp://Folder - site', 'ftps://secure', 'sftp://private'}

        result = run('import-accounts')
        assert result.returncode == 0, result.stdout + result.stderr
        assert 'Skipped 3 duplicate(s)' in result.stdout, result.stdout

        result = run('close')
        assert result.returncode == 0, result.stdout + result.stderr
        assert not os.path.exists(state)

    print('ok: FTP CLI driver commands')


if __name__ == '__main__':
    main()
