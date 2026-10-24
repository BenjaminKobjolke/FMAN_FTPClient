"""Store SFTP passwords in Windows Credential Manager."""
import ctypes
import sys
from ctypes import wintypes
from typing import Optional


_CRED_TYPE_GENERIC = 1
_CRED_PERSIST_LOCAL_MACHINE = 2
_ERROR_NOT_FOUND = 1168


if sys.platform == 'win32':
    class _FileTime(ctypes.Structure):
        _fields_ = [('low', wintypes.DWORD), ('high', wintypes.DWORD)]


    class _CredentialAttribute(ctypes.Structure):
        _fields_ = [
            ('keyword', wintypes.LPWSTR),
            ('flags', wintypes.DWORD),
            ('value_size', wintypes.DWORD),
            ('value', ctypes.POINTER(ctypes.c_ubyte)),
        ]


    class _Credential(ctypes.Structure):
        _fields_ = [
            ('flags', wintypes.DWORD),
            ('type', wintypes.DWORD),
            ('target_name', wintypes.LPWSTR),
            ('comment', wintypes.LPWSTR),
            ('last_written', _FileTime),
            ('credential_blob_size', wintypes.DWORD),
            ('credential_blob', ctypes.POINTER(ctypes.c_ubyte)),
            ('persist', wintypes.DWORD),
            ('attribute_count', wintypes.DWORD),
            ('attributes', ctypes.POINTER(_CredentialAttribute)),
            ('target_alias', wintypes.LPWSTR),
            ('user_name', wintypes.LPWSTR),
        ]


    _advapi32 = ctypes.WinDLL('Advapi32.dll', use_last_error=True)
    _advapi32.CredReadW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        ctypes.POINTER(ctypes.POINTER(_Credential)),
    ]
    _advapi32.CredReadW.restype = wintypes.BOOL
    _advapi32.CredWriteW.argtypes = [ctypes.POINTER(_Credential), wintypes.DWORD]
    _advapi32.CredWriteW.restype = wintypes.BOOL
    _advapi32.CredDeleteW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
    _advapi32.CredDeleteW.restype = wintypes.BOOL
    _advapi32.CredFree.argtypes = [ctypes.c_void_p]
    _advapi32.CredFree.restype = None


def read_password(target: str) -> Optional[str]:
    """Return a generic credential's password, or None when it is absent."""
    if sys.platform != 'win32':
        return None

    credential = ctypes.POINTER(_Credential)()
    ctypes.set_last_error(0)
    if not _advapi32.CredReadW(target, _CRED_TYPE_GENERIC, 0,
                               ctypes.byref(credential)):
        error_code = ctypes.get_last_error()
        if error_code == _ERROR_NOT_FOUND:
            return None
        raise OSError(error_code, 'CredReadW failed')

    try:
        blob = ctypes.string_at(
            credential.contents.credential_blob,
            credential.contents.credential_blob_size)
        return blob.decode('utf-16-le')
    finally:
        _advapi32.CredFree(credential)


def save_password(target: str, password: str) -> None:
    """Save or replace a generic credential with local-machine persistence."""
    if sys.platform != 'win32':
        return

    encoded_password = password.encode('utf-16-le')
    blob = ctypes.create_string_buffer(encoded_password, len(encoded_password))
    credential = _Credential(
        flags=0,
        type=_CRED_TYPE_GENERIC,
        target_name=target,
        comment=None,
        last_written=_FileTime(0, 0),
        credential_blob_size=len(encoded_password),
        credential_blob=ctypes.cast(blob, ctypes.POINTER(ctypes.c_ubyte)),
        persist=_CRED_PERSIST_LOCAL_MACHINE,
        attribute_count=0,
        attributes=None,
        target_alias=None,
        user_name=None,
    )
    ctypes.set_last_error(0)
    if not _advapi32.CredWriteW(ctypes.byref(credential), 0):
        error_code = ctypes.get_last_error()
        raise OSError(error_code, 'CredWriteW failed')


def delete_password(target: str) -> None:
    """Delete a generic credential; deleting a missing credential is harmless."""
    if sys.platform != 'win32':
        return

    ctypes.set_last_error(0)
    if not _advapi32.CredDeleteW(target, _CRED_TYPE_GENERIC, 0):
        error_code = ctypes.get_last_error()
        if error_code != _ERROR_NOT_FOUND:
            raise OSError(error_code, 'CredDeleteW failed')
