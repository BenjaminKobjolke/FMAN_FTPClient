from fman.fs import Column, query

from .connections import is_ftp


def _str(url, method):
    try:
        return str(query(url, method))
    except OSError:
        return ''


class Permissions(Column):
    display_name = 'Permissions'

    def get_str(self, url):
        if is_ftp(url):
            return _str(url, 'get_permissions')


class Owner(Column):
    display_name = 'Owner'

    def get_str(self, url):
        if is_ftp(url):
            return _str(url, 'get_owner')


class Group(Column):
    display_name = 'Group'

    def get_str(self, url):
        if is_ftp(url):
            return _str(url, 'get_group')
