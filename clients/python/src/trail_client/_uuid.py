"""UUIDv7 (RFC 9562), made by hand so the client needs nothing outside the standard library.

48 bits of Unix time in ms, the version (7), 12 bits of `rand_a`, the variant (0b10) and 62
random bits. Within one millisecond `rand_a` counts up from a random start, so ids made by one
process sort in the order they were made; spool file names rely on that.
"""

from __future__ import annotations

import secrets
import threading
import time

_lock = threading.Lock()
_last_ms = 0
_seq = 0


def now_ms() -> int:
    """Milliseconds since 1970, UTC."""
    return time.time_ns() // 1_000_000


def uuid7() -> str:
    """A new UUIDv7 as 36 lowercase characters."""
    global _last_ms, _seq
    with _lock:
        ms = now_ms()
        if ms > _last_ms:
            _last_ms = ms
            _seq = secrets.randbits(11)  # leave room to count up within the millisecond
        else:
            _seq += 1
            if _seq > 0xFFF:
                _last_ms += 1  # borrow the next millisecond rather than repeat
                _seq = 0
        ms, seq = _last_ms, _seq
    value = (ms & 0xFFFF_FFFF_FFFF) << 80
    value |= 0x7 << 76
    value |= seq << 64
    value |= 0b10 << 62
    value |= secrets.randbits(62)
    text = f"{value:032x}"
    return f"{text[0:8]}-{text[8:12]}-{text[12:16]}-{text[16:20]}-{text[20:32]}"


def reset_after_fork() -> None:
    """A child process gets a fresh lock; the parent's may have been held at fork time."""
    global _lock
    _lock = threading.Lock()
