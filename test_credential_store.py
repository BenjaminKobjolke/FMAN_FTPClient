"""Offline self-check for Windows Credential Manager storage.

Run: python test_credential_store.py
"""
import importlib.util
import os
import sys
import uuid


if os.name != 'nt':
    print('SKIP: Windows Credential Manager is available only on Windows')
    sys.exit(0)

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    'ftpclient_credentials', os.path.join(HERE, 'ftpclient', 'credentials.py'))
credentials = importlib.util.module_from_spec(spec)
spec.loader.exec_module(credentials)

target = 'FMAN_FTPClient:test:' + str(uuid.uuid4())
try:
    assert credentials.read_password(target) is None

    credentials.save_password(target, 'first password')
    assert credentials.read_password(target) == 'first password'

    credentials.save_password(target, 'replacement password')
    assert credentials.read_password(target) == 'replacement password'

    credentials.delete_password(target)
    assert credentials.read_password(target) is None
    credentials.delete_password(target)
finally:
    credentials.delete_password(target)

print('ok: credential save, overwrite, read, and delete')
