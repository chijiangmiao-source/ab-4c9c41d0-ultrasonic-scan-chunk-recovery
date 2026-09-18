"""Bit-packed received-chunk bitmap persisted in SQLite.

Bit ``i`` of the bitmap is 1 when chunk ``i`` has been accepted. The bitmap is
stored as a BLOB of ``ceil(total_chunks / 8)`` bytes on the session row and is
updated in the same transaction as the chunk-row insert, so the two can never
disagree across a restart.
"""

from __future__ import annotations


def new_bitmap(total_chunks: int) -> bytearray:
    return bytearray((total_chunks + 7) // 8)


def set_bit(bitmap: bytearray, index: int) -> None:
    bitmap[index >> 3] |= 1 << (index & 7)


def get_bit(bitmap: bytes | bytearray, index: int) -> bool:
    return bool(bitmap[index >> 3] & (1 << (index & 7)))


def missing_indices(bitmap: bytes | bytearray, total_chunks: int) -> list[int]:
    """Indices not yet received, in ascending order."""
    return [i for i in range(total_chunks) if not get_bit(bitmap, i)]


def is_complete(bitmap: bytes | bytearray, total_chunks: int) -> bool:
    return not any(not get_bit(bitmap, i) for i in range(total_chunks))
