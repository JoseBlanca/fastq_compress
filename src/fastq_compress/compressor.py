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

MAGIC = b"FQC"  # "FASTQ Compressed"
VERSION1 = b"\x01"  # file format version v1

N_COLS_STRUCT_FMT = "I"
N_COLS_SIZE = struct.calcsize(N_COLS_STRUCT_FMT)


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


def write_compressed_file(
    fhand: BinaryIO, chunks, algorithms: list[Compression] | None = None
):
    header = MAGIC + VERSION1
    fhand.write(header)

    for chunk in chunks:
        n_cols = len(chunk)
        if algorithms is None:
            algorithms = [DEFAULT_ALGORITHM] * n_cols
        fhand.write(struct.pack(N_COLS_STRUCT_FMT, n_cols))
        if n_cols != len(algorithms):
            raise RuntimeError("Different chunks have a different number of columns")

        compressed_chunk = compress_chunk(chunk, algorithms)
        to_write = {"algorithms": algorithms, "chunks": compressed_chunk}


def _read_binary_struct(fhand, struct_fmt, bytes_size):
    content = fhand.read(bytes_size)
    if content == b"":
        raise EOFError()
    return struct.unpack(struct_fmt, content)


def _read_chunk(fhand):
    n_cols = _read_binary_struct(fhand, N_COLS_STRUCT_FMT, N_COLS_SIZE)[0]
    print(n_cols)


def read_compressed_file(fhand: BinaryIO):
    magic = fhand.read(3)
    if magic == b"":
        raise RuntimeError("The file is empty")
    if magic != MAGIC:
        raise RuntimeError(
            "Wrong magic number, it dos not look like a compressed FASTQ file"
        )
    assert magic == MAGIC
    version = fhand.read(1)
    if version != VERSION1:
        raise RuntimeError("We only know how to read version 1")

    while True:
        try:
            _read_chunk(fhand)
        except EOFError:
            break
