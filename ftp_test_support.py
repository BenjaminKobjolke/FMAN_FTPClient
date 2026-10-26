"""Load FTP code with the fman API stub used by offline self-checks."""
import importlib.util
import os
import sys
import types
from types import ModuleType


def load_ftp_module(test_directory: str) -> ModuleType:
    fake_fman = types.ModuleType('fman')
    fake_fman.__path__ = []
    fake_fman.load_json = lambda name, default=None, save_on_quit=False: \
        {} if default is None else default
    sys.modules['fman'] = fake_fman

    package = types.ModuleType('ftpclient')
    package.__path__ = [os.path.join(test_directory, 'ftpclient')]
    sys.modules['ftpclient'] = package
    spec = importlib.util.spec_from_file_location(
        'ftpclient.ftp', os.path.join(test_directory, 'ftpclient', 'ftp.py'))
    ftp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ftp)
    return ftp
