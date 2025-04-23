from typing import BinaryIO, Iterator
from collections import namedtuple

from more_itertools import chunked

Seq = namedtuple("Seq", ["name_line", "seq", "qual"])


def _group_seqs(fhand: BinaryIO) -> Iterator[Seq]:
    for seq_lines in chunked(fhand, 4):
        if seq_lines[0][0] != 64 or seq_lines[2] != b"+\n" or len(seq_lines) != 4:
            raise RuntimeError(f"Malformed seq: {seq_lines}")
        yield Seq(seq_lines[0], seq_lines[1], seq_lines[3])


def iterate_fastq_chunks(fhand: BinaryIO, n_seqs: int) -> Iterator[Seq]:
    seqs = _group_seqs(fhand)
    return chunked(seqs, n_seqs)
