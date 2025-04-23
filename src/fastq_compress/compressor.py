import lzma
import gzip
import zstandard
from enum import StrEnum


class Compression(StrEnum):
    ZSTD = "zstd"
    GZIP = "gzip"
    LZMA = "lzma"


COMPRESSION_ALGORITHMS = {
    Compression.GZIP: {"compressor": gzip.compress},
    Compression.LZMA: {"compressor": lzma.compress},
    Compression.ZSTD: {"compressor": zstandard.compress},
}


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
