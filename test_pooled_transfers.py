"""Offline checks for FTP transfers over the worker's pooled session."""
import importlib
import os
import sys
import tempfile
import types


HERE = os.path.dirname(os.path.abspath(__file__))


class Task:
    def __init__(self, title, size=0, fn=None, args=(), kwargs=None):
        self.title = title
        self.size = size
        self.fn = fn or (lambda: None)
        self.args = args
        self.kwargs = kwargs or {}
        self.progress = 0

    def __call__(self):
        return self.fn(*self.args, **self.kwargs)

    def get_size(self):
        return self.size

    def set_progress(self, value):
        self.progress = value

    def check_canceled(self):
        pass


class FileSystem:
    def __init__(self):
        self.cache = types.SimpleNamespace(clear=lambda path: None)


fman = types.ModuleType('fman')
fman.__path__ = []
fman.Task = Task
fman.submit_task = lambda task: task()
fman.show_status_message = lambda *args, **kwargs: None
fman.load_json = lambda name, default=None, save_on_quit=False: \
    {} if default is None else default
fs = types.ModuleType('fman.fs')
fs.FileSystem = FileSystem
fs.cached = lambda method: method
fs.notify_file_added = lambda url: None
fman.fs = fs
url = types.ModuleType('fman.url')
url.basename = lambda value: value.rsplit('/', 1)[-1]
url.dirname = lambda value: value.rsplit('/', 1)[0]
url.join = lambda *parts: '/'.join(parts)
url.splitscheme = lambda value: (
    value[:value.index('://') + 3], value[value.index('://') + 3:])
fman.url = url
sys.modules.update({'fman': fman, 'fman.fs': fs, 'fman.url': url})

# Import the module without running the plugin package's UI initialization.
package = types.ModuleType('ftpclient')
package.__path__ = [os.path.join(HERE, 'ftpclient')]
sys.modules['ftpclient'] = package
filesystems = importlib.import_module('ftpclient.filesystems')
wrapper_type = importlib.import_module('ftpclient.ftp').FtpWrapper


class FakeSession:
    def __init__(self):
        self.uploads = []
        self.downloads = []

    def storbinary(self, command, file_obj, callback=None):
        self.uploads.append(command)
        while True:
            block = file_obj.read(3)
            if not block:
                break
            if callback is not None:
                callback(block)

    def retrbinary(self, command, callback):
        self.downloads.append(command)
        for block in (b'abc', b'de'):
            callback(block)


class FakeHost:
    def __init__(self):
        self._session = FakeSession()
        self._children = []
        self.path = types.SimpleNamespace(getsize=lambda path: 5)


host = FakeHost()


class FakeWrapper:
    def __init__(self, value):
        self.path = '/' + value.split('://', 1)[1].split('/', 1)[1]
        self.conn = host

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def upload(self, src_file, on_chunk=None):
        kwargs = {'callback': on_chunk} if on_chunk is not None else {}
        host._session.storbinary('STOR ' + self.path, src_file, **kwargs)

    def download(self, dst_file, on_chunk):
        def write_chunk(block):
            dst_file.write(block)
            on_chunk(block)

        host._session.retrbinary('RETR ' + self.path, write_chunk)


filesystems.connection_for = lambda url: FakeWrapper(url)

with tempfile.TemporaryDirectory() as directory:
    src_path = os.path.join(directory, 'upload.bin')
    with open(src_path, 'wb') as output:
        output.write(b'abcdefg')

    ftp_fs = filesystems.FtpFs()
    upload = ftp_fs._prepare_file_copy(
        'file://' + src_path.replace(os.sep, '/'),
        'ftp://example.com/upload.bin')
    upload_chunks = []
    upload._transfer(upload_chunks.append)
    assert host._session.uploads == ['STOR /upload.bin']
    assert upload_chunks == [b'abc', b'def', b'g']

    dst_path = os.path.join(directory, 'download.bin')
    download = ftp_fs._prepare_file_copy(
        'ftp://example.com/download.bin',
        'file://' + dst_path.replace(os.sep, '/'))
    download_chunks = []
    download._transfer(download_chunks.append)
    assert host._session.downloads == ['RETR /download.bin']
    assert download_chunks == [b'abc', b'de']
    with open(dst_path, 'rb') as result:
        assert result.read() == b'abcde'

    wrapper = types.SimpleNamespace(conn=host, path='/direct.bin')
    with open(src_path, 'rb') as src:
        wrapper_type.upload(wrapper, src)
    chunks = []
    with open(src_path, 'rb') as src:
        wrapper_type.upload(wrapper, src, chunks.append)
    assert chunks == [b'abc', b'def', b'g']
    downloaded_chunks = []
    with open(dst_path, 'wb') as dst:
        wrapper_type.download(wrapper, dst, downloaded_chunks.append)
    assert downloaded_chunks == [b'abc', b'de']
    with open(dst_path, 'rb') as result:
        assert result.read() == b'abcde'

assert host._children == [], 'pooled transfers must not create child hosts'
print('ok: pooled upload and download use the worker session')
