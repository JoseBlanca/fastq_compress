CHUNKS_N_SEQS = 10000

lines = open("file_name.fastq", "rb")
chunks = iterate_fastq_chunks(lines)
compress_chunk = compress_fastq_chunk(chunk)
