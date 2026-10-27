"""Offline regression check for permanent FTP deletion through fman's trash action.

Run: python test_move_to_trash.py
"""
import os

from ftp_test_support import load_filesystems

_, filesystems = load_filesystems(os.path.dirname(os.path.abspath(__file__)))

fs_obj = filesystems.FtpFs()
deleted = []
fs_obj.delete = lambda path: deleted.append(path)

path = 'example.com/public/file.txt'
fs_obj.move_to_trash(path)
assert deleted == [path], deleted
print('ok: move_to_trash permanently deletes the same FTP path')
