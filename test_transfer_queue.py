"""Offline checks for fman++ transfer queue parsing and path resolution.

Run: python test_transfer_queue.py
"""
import importlib
import os
import tempfile
import types
import unittest

from test_fman_setup import install_fman_modules

HERE = os.path.dirname(os.path.abspath(__file__))
fman = types.ModuleType('fman')
fman.__path__ = []
fs = types.ModuleType('fman.fs')
url = install_fman_modules(fman, fs, HERE)
url.as_url = lambda path: 'file://' + os.path.abspath(path).replace(os.sep, '/')
queue = importlib.import_module('ftpclient.transfer_queue')


class TransferQueueTests(unittest.TestCase):
    def test_parse_put_get_comments_and_case_insensitive_operations(self):
        entries = queue.parse_queue(
            '# queue\n\nPuT source.txt /remote/\nget /remote/file.txt dest.txt\n')

        self.assertEqual(
            [(entry.op, entry.src, entry.dst, entry.line) for entry in entries],
            [('put', 'source.txt', '/remote/', 3),
             ('get', '/remote/file.txt', 'dest.txt', 4)])

    def test_parse_quoted_paths_preserves_spaces_and_backslashes(self):
        entries = queue.parse_queue(
            'put "C:\\site files\\index.html" "/public folder/"\n'
            'get /logs/error.log "C:\\logs\\error file.log"')

        self.assertEqual(entries[0].src, 'C:\\site files\\index.html')
        self.assertEqual(entries[0].dst, '/public folder/')
        self.assertEqual(entries[1].dst, 'C:\\logs\\error file.log')

    def test_parse_unquoted_paths(self):
        entries = queue.parse_queue('put assets /public/assets')

        self.assertEqual((entries[0].src, entries[0].dst),
                         ('assets', '/public/assets'))

    def test_parse_reports_every_invalid_line(self):
        with self.assertRaises(queue.QueueError) as caught:
            queue.parse_queue(
                'copy a b\nput only-one\nget a b extra\nput "unclosed b')

        self.assertEqual(len(caught.exception.errors), 4)
        self.assertEqual(
            [error.split(':', 1)[0] for error in caught.exception.errors],
            ['line 1', 'line 2', 'line 3', 'line 4'])

    def test_resolve_folder_destinations_and_file_destinations(self):
        with tempfile.TemporaryDirectory() as folder:
            source = os.path.join(folder, 'source.txt')
            with open(source, 'w', encoding='utf-8') as stream:
                stream.write('data')
            entries = [
                queue.QueueEntry('put', source, '/remote/', 1),
                queue.QueueEntry('get', '/remote/other.txt',
                                 os.path.join(folder, 'downloads') + os.sep, 2),
                queue.QueueEntry('put', source, '/remote/renamed.txt', 3),
            ]

            pairs, missing = queue.resolve(
                entries, folder, 'ftp://user@host:21', '/')

        self.assertEqual(missing, [])
        self.assertEqual(pairs[0][1], 'ftp://user@host:21/remote/source.txt')
        self.assertTrue(pairs[1][1].endswith('/downloads/other.txt'))
        self.assertEqual(pairs[2][1], 'ftp://user@host:21/remote/renamed.txt')

    def test_resolve_relative_paths_and_collect_missing_local_sources(self):
        with tempfile.TemporaryDirectory() as folder:
            source = os.path.join(folder, 'present.txt')
            with open(source, 'w', encoding='utf-8') as stream:
                stream.write('data')
            entries = [
                queue.QueueEntry('put', 'present.txt', 'upload/', 1),
                queue.QueueEntry('put', 'missing.txt', 'upload/', 2),
                queue.QueueEntry('get', 'logs/error.log', 'download/', 3),
            ]

            pairs, missing = queue.resolve(
                entries, folder, 'sftp://host', '/current/path')

        self.assertEqual(missing, [os.path.join(folder, 'missing.txt')])
        self.assertEqual(
            pairs[0][0], url.as_url(source))
        self.assertEqual(pairs[0][1], 'sftp://host/current/path/upload/present.txt')
        self.assertEqual(pairs[2][0], 'sftp://host/current/path/logs/error.log')
        self.assertTrue(pairs[2][1].endswith('/download/error.log'))

    def test_absolute_remote_paths_stay_absolute_and_relative_detection(self):
        entry = queue.QueueEntry('get', '/var/log/error.log', 'error.log', 1)
        relative = queue.QueueEntry('put', 'a.txt', 'relative/path', 2)

        pairs, missing = queue.resolve(
            [entry], '.', 'ftp://host', '/ignored/cwd')

        self.assertEqual(missing, [])
        self.assertEqual(pairs[0][0], 'ftp://host/var/log/error.log')
        self.assertFalse(queue.has_relative_remote([entry]))
        self.assertTrue(queue.has_relative_remote([relative]))

    def test_relative_remote_path_uses_root_when_remote_cwd_is_root(self):
        entry = queue.QueueEntry('get', 'logs/error.log', 'error.log', 1)

        pairs, _ = queue.resolve([entry], '.', 'ftp://host', '/')

        self.assertEqual(pairs[0][0], 'ftp://host/logs/error.log')


if __name__ == '__main__':
    unittest.main()
