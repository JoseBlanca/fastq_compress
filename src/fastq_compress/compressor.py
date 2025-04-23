import lzma
import gzip
from enum import StrEnum
from typing import BinaryIO
import struct

import zstandard


class Compression(StrEnum):
    ZSTD = "zstd"
    GZIP = "gzip"
    LZMA = "lzma"


DEFAULT_ALGORITHM = Compression.LZMA

COMPRESSION_ALGORITHMS = {
    Compression.GZIP: {"compressor": gzip.compress},
    Compression.LZMA: {"compressor": lzma.compress},
    Compression.ZSTD: {"compressor": zstandard.compress},
}

N_COLS_STRUCT_FMT = "I"


def compress_chunk(
    columns: list[list[bytes]], algorithms: list[Compression]
) -> list[bytearray]:
    compressed = []
    for col, algorithm in zip(columns, algorithms):
        compression_funct = COMPRESSION_ALGORITHMS[algorithm]["compressor"]
        if any(map(lambda item: b"\n" in item, col)):
            raise ValueError("carriage returns not allowed in items to compress")

        to_compress = b"\n".join(col)
        compressed.append(compression_funct(to_compress))
    return compressed


def compress_chunks(
    fhand: BinaryIO, chunks, algorithms: list[Compression] | None = None
):
    first_chunk = True
    for chunk in chunks:
        n_cols = len(chunk)
        if first_chunk:
            fhand.write(struct.pack(N_COLS_STRUCT_FMT, n_cols))
            first_chunk = False
            if algorithms is None:
                algorithms = [DEFAULT_ALGORITHM] * n_cols
        if n_cols != len(algorithms):
            raise RuntimeError("Different chunks have a different number of columns")

        compressed_chunk = compress_chunk(chunk, algorithms)
        to_write = {"algorithms": algorithms, "chunks": compressed_chunk}
