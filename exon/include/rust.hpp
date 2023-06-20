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

extern "C" {

ReaderResult new_reader(ArrowArrayStream *stream_ptr,
                        const char *uri,
                        uintptr_t batch_size,
                        const char *compression,
                        const char *file_format,
                        const char *filters);

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

} // extern "C"
