from fastq_compress.coding import encode_dna, decode_dna


def test_dna_coding():
    orig_dna = b"ATCGATCGTAGTCGTATCATGCTAGCTGTCTAGCTGTA"
    coded_dna = encode_dna(orig_dna)
    decoded_dna = decode_dna(coded_dna)
    assert decoded_dna == orig_dna
