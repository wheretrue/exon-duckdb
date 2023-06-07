#include <arrow/c/abi.h>

#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

struct ReaderResult {
  const char *error;
};

struct ReplacementScanResult {
  const char *file_type;
};

struct CResult {
  const char *value;
  const char *error;
};

struct CExtractResponse {
  uintptr_t sequence_start;
  uintptr_t sequence_len;
  const char *extracted_sequence;
  const char *error;
};

struct NoodlesWriter {
  void *writer;
  const char *error;
};

struct FastqWriter {
  void *writer;
  const char *error;
};

struct GffWriter {
  void *writer;
  const char *error;
};

struct GffWriterResult {
  int32_t result;
  const char *error;
};

extern "C" {

ReaderResult new_reader(ArrowArrayStream *stream_ptr,
                        const char *uri,
                        uintptr_t batch_size,
                        const char *compression,
                        const char *file_format);

ReplacementScanResult replacement_scan(const char *uri);

bool is_segmented(uint16_t flag);

bool is_unmapped(uint16_t flag);

bool is_properly_aligned(uint16_t flag);

bool is_mate_unmapped(uint16_t flag);

bool is_reverse_complemented(uint16_t flag);

bool is_mate_reverse_complemented(uint16_t flag);

bool is_first_segment(uint16_t flag);

bool is_last_segment(uint16_t flag);

bool is_secondary(uint16_t flag);

bool is_quality_control_failed(uint16_t flag);

bool is_duplicate(uint16_t flag);

bool is_supplementary(uint16_t flag);

CResult parse_cigar(const char *cigar);

CExtractResponse extract_from_cigar(const char *sequence_str, const char *cigar_str);

NoodlesWriter fasta_writer_new(const char *filename, const char *compression);

int32_t fasta_writer_write(void *writer, const char *id, const char *description, const char *seq);

void destroy_writer(void *writer);

FastqWriter fastq_writer_new(const char *filename, const char *compression);

int32_t fastq_writer_write(void *writer,
                           const char *id,
                           const char *description,
                           const char *seq,
                           const char *quality_scores);

void fastq_writer_destroy(void *writer);

GffWriter gff_writer_new(const char *filename, const char *compression);

GffWriterResult gff_writer_write(void *writer,
                                 const char *reference_sequence_name,
                                 const char *source,
                                 const char *feature_type,
                                 int32_t start,
                                 int32_t end,
                                 float score,
                                 const char *strand,
                                 const char *phase,
                                 const char *attributes);

void gff_writer_destroy(void *writer);

} // extern "C"
