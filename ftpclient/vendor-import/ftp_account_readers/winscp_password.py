"""WinSCP's reversible password obfuscation (used when no master password is set).

Port of EncryptPassword/DecryptPassword from WinSCP's Security.cpp. Each byte is stored as
two hex digits of (~byte ^ 0xA3); the plaintext is prefixed with user + host as the key.
"""

import random
import secrets

_MAGIC = 0xA3
_FLAG = 0xFF
_INTERNAL_VERSION = 0x00
_MAX_LEN = 50
_HEX = "0123456789ABCDEF"


def _encode_byte(value: int) -> str:
    value = ~(value ^ _MAGIC) & 0xFF
    return _HEX[value >> 4] + _HEX[value & 0x0F]


def _decode_byte(hex_pair: str) -> int:
    return ~(int(hex_pair, 16) ^ _MAGIC) & 0xFF


def encrypt(host: str, user: str, password: str, rng: random.Random | None = None) -> str:
    def rand_below(limit: int) -> int:
        return rng.randrange(limit) if rng else secrets.randbelow(limit)

    data = (user + host + password).encode("utf-8")
    shift = rand_below(_MAX_LEN - len(data)) if len(data) < _MAX_LEN else 0
    result = [_encode_byte(_FLAG), _encode_byte(_INTERNAL_VERSION)]
    result += [_encode_byte(len(data)), _encode_byte(shift)]
    result += [_encode_byte(rand_below(256)) for _ in range(shift)]
    result += [_encode_byte(b) for b in data]
    encoded = "".join(result)
    while len(encoded) < _MAX_LEN * 2:
        encoded += _encode_byte(rand_below(256))
    return encoded


def decrypt(host: str, user: str, encrypted: str) -> str:
    pairs = [encrypted[i : i + 2] for i in range(0, len(encrypted) - 1, 2)]
    pos = 0

    def next_byte() -> int:
        nonlocal pos
        value = _decode_byte(pairs[pos])
        pos += 1
        return value

    if not pairs:
        return ""
    flag = next_byte()
    if flag == _FLAG:
        next_byte()
        length = next_byte()
    else:
        length = flag
    shift = next_byte()
    pos += shift
    data = bytes(next_byte() for _ in range(length))
    if flag == _FLAG:
        key = (user + host).encode("utf-8")
        if not data.startswith(key):
            raise ValueError("WinSCP password key mismatch")
        data = data[len(key) :]
    return data.decode("utf-8")
