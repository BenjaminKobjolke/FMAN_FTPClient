"""NppFTP password encryption: DES-CFB64 with the built-in key, hex encoded."""

from typing import Any

from ftp_account_readers import constants


def _odd_parity(key: bytes) -> bytes:
    return bytes((b & 0xFE) | ((bin(b & 0xFE).count("1") + 1) % 2) for b in key)


def _cipher() -> Any:
    from Crypto.Cipher import DES

    key = _odd_parity(constants.NPP_DEFAULT_KEY)
    return DES.new(key, DES.MODE_CFB, iv=key, segment_size=64)


def encrypt(password: str) -> str:
    if not password:
        return ""
    return bytes(_cipher().encrypt(password.encode("utf-8"))).hex().upper()


def decrypt(encrypted: str) -> str:
    if not encrypted:
        return ""
    return bytes(_cipher().decrypt(bytes.fromhex(encrypted))).decode("utf-8")
