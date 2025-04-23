from io import BytesIO
import struct

import pytest

from fastq_compress.compressor import (
    compress_chunk,
    Compression,
    compress_chunks,
    N_COLS_STRUCT_FMT,
)


def test_chunk_compressor():
    chunk = [b"1\n1"]
    with pytest.raises(ValueError):
        compress_chunk([chunk], [Compression.GZIP])

    col1 = [b"1", b"2", b"3", b"4"]
    col2 = [b"a", b"b", b"c", b"d"]
    chunk = [col1, col2]
    compressed = compress_chunk(chunk, [Compression.ZSTD, Compression.GZIP])
    assert len(compressed) == 2


def test_chunks_compressor():
    col1 = [b"1", b"2", b"3", b"4"]
    col2 = [b"a", b"b", b"c", b"d"]
    chunks = [[col1, col2], [col1, col2]]
    fhand = BytesIO()
    compress_chunks(fhand, chunks)
    fhand.seek(0)
    content = fhand.read(struct.calcsize(N_COLS_STRUCT_FMT))
    n_cols = 2
    assert struct.unpack(N_COLS_STRUCT_FMT, content)[0] == n_cols
