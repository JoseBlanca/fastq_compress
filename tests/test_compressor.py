from io import BytesIO
import struct

import pytest

from fastq_compress.compressor import (
    compress_chunk,
    CompressionAlgorithm,
    write_compressed_file,
    read_compressed_file,
)


def test_chunk_compressor():
    chunk = [b"1\n1"]
    with pytest.raises(ValueError):
        compress_chunk([chunk], [CompressionAlgorithm.GZIP])

    col1 = [b"1", b"2", b"3", b"4"]
    col2 = [b"a", b"b", b"c", b"d"]
    chunk = [col1, col2]
    compressed = compress_chunk(
        chunk, [CompressionAlgorithm.ZSTD, CompressionAlgorithm.GZIP]
    )
    assert len(compressed) == 2


def test_chunks_compressor():
    col1 = [b"1", b"2", b"3", b"4"]
    col2 = [b"a", b"b", b"c", b"d"]
    chunks = [[col1, col2], [col1, col2]]
    fhand = BytesIO()
    write_compressed_file(fhand, chunks)

    fhand.seek(0)
    read_compressed_file(fhand)
