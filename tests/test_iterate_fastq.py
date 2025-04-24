from io import BytesIO

from fastq_compress.fastq import iterate_fastq_chunks, compress_fastq, decompress_fastq

FASTQ1 = b"""@SEQ1
AATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT
+
!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65
@SEQ2
GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT
+
!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65
@SEQ3
CATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT
+
!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65
"""


def test_fastq_chunker():
    fhand = BytesIO(FASTQ1)
    chunks = iterate_fastq_chunks(fhand, 2)
    chunks = list(chunks)
    assert len(chunks) == 2
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 1


def test_compress_fastq():
    fhand = BytesIO(FASTQ1)
    compressed_fhand = BytesIO()
    compress_fastq(fhand, compressed_fhand, n_seqs_in_chunk=2)

    compressed_fhand.seek(0)
    decompressed_fhand = BytesIO()
    decompress_fastq(compressed_fhand, decompressed_fhand)
    decompressed_fhand.seek(0)
    assert decompressed_fhand.read() == FASTQ1
