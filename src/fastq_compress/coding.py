from typing import ByteString
from functools import lru_cache

from more_itertools import grouper


BASE_CODES = {
    b"C"[0]: 0b001,
    b"G"[0]: 0b010,
    b"A"[0]: 0b011,
    b"T"[0]: 0b100,
    b"N"[0]: 0b101,
    b" "[0]: 0b000,
}

BASE_CODES_INV = {v: k for k, v in BASE_CODES.items()}


def _pack_5bases_to_uint16(codes: list[int]) -> int:
    result = 0
    for code in codes:
        result = (result << 3) | code
    return result


@lru_cache
def _encode_bases(bases):
    binary_bases = [BASE_CODES[base] for base in bases]
    return _pack_5bases_to_uint16(binary_bases)


def encode_dna(dna: bytearray) -> bytearray:
    coded = bytearray()
    for bases in grouper(dna, 5, fillvalue=b" "[0]):
        coded.extend(_encode_bases(tuple(bases)).to_bytes(2, byteorder="big"))
    return coded


@lru_cache
def _unpack_uint16_to_5bases(packed: int) -> list[int]:
    bases = []
    for shift in (12, 9, 6, 3, 0):  # 5 bases × 3 bits each
        code = (packed >> shift) & 0b111
        bases.append(code)
    return bases


def decode_dna(encoded: ByteString) -> bytearray:
    dna = bytearray()
    for i in range(0, len(encoded), 2):
        packed = int.from_bytes(encoded[i : i + 2], byteorder="big")
        codes = _unpack_uint16_to_5bases(packed)
        dna.extend(BASE_CODES_INV[c] for c in codes)
    return dna.rstrip(b" ")
