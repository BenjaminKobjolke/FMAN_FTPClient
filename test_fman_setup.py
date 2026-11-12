"""Shared fman module setup for the offline plugin self-checks."""
import os
import sys
import types


def install_fman_modules(
    fman: types.ModuleType, fs: types.ModuleType, project_dir: str
) -> types.ModuleType:
    """Install fman stubs and a bare ftpclient package for a self-check."""
    url = types.ModuleType('fman.url')
    url.join = lambda *parts: '/'.join(parts)
    url.basename = lambda value: value.rsplit('/', 1)[-1]
    url.dirname = lambda value: value.rsplit('/', 1)[0]
    url.splitscheme = lambda value: (
        value[:value.index('://') + 3], value[value.index('://') + 3:])
    fman.fs = fs
    fman.url = url
    sys.modules.update({'fman': fman, 'fman.fs': fs, 'fman.url': url})

    # Skip plugin startup, which imports commands and listeners.
    package = types.ModuleType('ftpclient')
    package.__path__ = [os.path.join(project_dir, 'ftpclient')]
    sys.modules['ftpclient'] = package
    return url
