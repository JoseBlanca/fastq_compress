from typing import BinaryIO, Iterator
from collections import namedtuple

from more_itertools import chunked

from fastq_compress.compressor import write_compressed_file, read_compressed_file

Seq = namedtuple("Seq", ["name_line", "seq", "qual"])


def _group_seqs(fhand: BinaryIO) -> Iterator[Seq]:
    for seq_lines in chunked(fhand, 4):
        if seq_lines[0][0] != 64 or seq_lines[2] != b"+\n" or len(seq_lines) != 4:
            raise RuntimeError(f"Malformed seq: {seq_lines}")
        yield Seq(seq_lines[0], seq_lines[1], seq_lines[3])


def iterate_fastq_chunks(fhand: BinaryIO, n_seqs: int) -> Iterator[Seq]:
    seqs = _group_seqs(fhand)
    return chunked(seqs, n_seqs)


def _columnize_fastq_chunk(chunk):
    name_lines, seqs, quals = [], [], []
    for seq in chunk:
        name_lines.append(seq.name_line.rstrip(b"\n"))
        seqs.append(seq.seq.rstrip(b"\n"))
        quals.append(seq.qual.rstrip(b"\n"))
    return name_lines, seqs, quals


def _decolumnize_fastq_chunk(chunk):
    seqs = []
    for name, seq, qual in zip(*chunk):
        seqs.append(Seq(name, seq, qual))
    return seqs


def compress_fastq(fhand: BinaryIO, compressed_fhand: BinaryIO, n_seqs_in_chunk: int):
    chunks = iterate_fastq_chunks(fhand, n_seqs_in_chunk)
    chunks = map(_columnize_fastq_chunk, chunks)
    write_compressed_file(compressed_fhand, chunks)


def decompress_fastq(compressed_fhand: BinaryIO, decompressed_fhand: BinaryIO):
    chunks = read_compressed_file(compressed_fhand)
    chunks = map(_decolumnize_fastq_chunk, chunks)

    for chunk in chunks:
        for seq in chunk:
            to_write = b"\n".join([seq.name_line, seq.seq, b"+", seq.qual])
            decompressed_fhand.write(to_write)
            decompressed_fhand.write(b"\n")
