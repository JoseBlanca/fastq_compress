import lzma
import gzip
from enum import IntEnum
from typing import BinaryIO, Iterator
import struct
from collections import defaultdict

import zstandard


class CompressionAlgorithm(IntEnum):
    GZIP = 0
    LZMA = 1
    ZSTD = 2


DEFAULT_ALGORITHM = CompressionAlgorithm.LZMA

COMPRESSION_ALGORITHMS = {
    CompressionAlgorithm.GZIP: {
        "compressor": gzip.compress,
        "decompressor": gzip.decompress,
    },
    CompressionAlgorithm.LZMA: {
        "compressor": lzma.compress,
        "decompressor": lzma.decompress,
    },
    CompressionAlgorithm.ZSTD: {
        "compressor": zstandard.compress,
        "decompressor": zstandard.decompress,
    },
}

MAGIC = b"FQC"  # "FASTQ Compressed"
VERSION1 = b"\x01"  # file format version v1

N_COLS_STRUCT_FMT = "I"
N_COLS_SIZE = struct.calcsize(N_COLS_STRUCT_FMT)


def compress_chunk(
    columns: list[list[bytes]], algorithms: list[CompressionAlgorithm]
) -> list[bytearray]:
    compressed = []
    for col, algorithm in zip(columns, algorithms):
        compression_funct = COMPRESSION_ALGORITHMS[algorithm]["compressor"]
        if any(map(lambda item: b"\n" in item, col)):
            raise ValueError("carriage returns not allowed in items to compress")

        to_compress = b"\n".join(col)
        compressed.append(compression_funct(to_compress))
    return compressed


def _create_fmt_for_algoritms(n_cols):
    return f"{n_cols}B"


def _write_blob(fhand, data: bytes):
    fhand.write(struct.pack("I", len(data)))
    fhand.write(data)


def _read_blob(fhand):
    blob_size = _read_binary_struct(fhand, "I", 4)[0]
    data = fhand.read(blob_size)
    if len(data) < blob_size:
        raise EOFError("Unexpected EOF when reading blob data")
    return data


def _get_best_compression_algorithm(chunk):
    compression_sizes = defaultdict(dict)
    for algorithm in COMPRESSION_ALGORITHMS:
        compression_funct = COMPRESSION_ALGORITHMS[algorithm]["compressor"]
        for idx, col in enumerate(chunk):
            compression_sizes[idx][algorithm] = len(compression_funct(b"\n".join(col)))

    best_algorithms = []
    for col_idx in range(len(compression_sizes)):
        sizes = compression_sizes[col_idx]
        best_algorithm = None
        smallest_size = None
        for algorithm, size in sizes.items():
            if best_algorithm is None:
                best_algorithm = algorithm
                smallest_size = size
            else:
                if smallest_size > size:
                    best_algorithm = algorithm
                    smallest_size = size
        best_algorithms.append(CompressionAlgorithm(algorithm))
    return best_algorithms


def write_compressed_file(
    fhand: BinaryIO, chunks, algorithms: list[CompressionAlgorithm] | None = None
):
    header = MAGIC + VERSION1
    fhand.write(header)

    for chunk in chunks:
        # chunk struct
        # I n_cols
        # {n_cols}B list of algorithms
        n_cols = len(chunk)
        if algorithms is None:
            algorithms = _get_best_compression_algorithm(chunk)
            int_algorithms = [algorithm.value for algorithm in algorithms]
            algorithms_fmt = _create_fmt_for_algoritms(n_cols)

        fhand.write(struct.pack(N_COLS_STRUCT_FMT, n_cols))

        if n_cols != len(algorithms):
            raise RuntimeError("Different chunks have a different number of columns")

        fhand.write(struct.pack(algorithms_fmt, *int_algorithms))

        compressed_cols = compress_chunk(chunk, algorithms)
        for col in compressed_cols:
            _write_blob(fhand, col)


def _read_binary_struct(fhand, struct_fmt, bytes_size):
    content = fhand.read(bytes_size)
    if content == b"":
        raise EOFError()
    unpacked = struct.unpack(struct_fmt, content)
    return unpacked


def _read_chunk(fhand):
    n_cols = _read_binary_struct(fhand, N_COLS_STRUCT_FMT, N_COLS_SIZE)[0]

    algorithms_fmt = _create_fmt_for_algoritms(n_cols)
    algorithms_size = struct.calcsize(algorithms_fmt)
    algorithms_ints = _read_binary_struct(fhand, algorithms_fmt, algorithms_size)
    algorithms = [CompressionAlgorithm(algorithm) for algorithm in algorithms_ints]

    cols = []
    for col_idx in range(n_cols):
        compressed_col = _read_blob(fhand)
        decompressor_funct = COMPRESSION_ALGORITHMS[algorithms[col_idx]]["decompressor"]
        col = decompressor_funct(compressed_col).split(b"\n")
        cols.append(col)

    return {"n_cols": n_cols, "algorithms": algorithms, "columns": cols}


def read_compressed_file(fhand: BinaryIO) -> Iterator:
    magic = fhand.read(3)
    if magic == b"":
        raise RuntimeError("The file is empty")
    if magic != MAGIC:
        raise RuntimeError(
            "Wrong magic number, it does not look like a compressed FASTQ file"
        )
    assert magic == MAGIC
    version = fhand.read(1)
    if version != VERSION1:
        raise RuntimeError("We only know how to read version 1")

    while True:
        try:
            chunk = _read_chunk(fhand)
        except EOFError:
            break
        yield chunk["columns"]
