#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

struct BAMReaderC {
  void *bam_reader;
  void *bam_header;
  const char *error;
};

struct BcfReaderC {
  void *bcf_reader;
  void *bcf_header;
  void *bcf_string_maps;
  const char *error;
};

struct BEDReaderC {
  void *inner_reader;
  uint8_t n_columns;
};

struct BEDRecordC {
  const char *reference_sequence_name;
  uintptr_t start;
  uintptr_t end;
  const char *name;
  int64_t score;
  const char *strand;
  uintptr_t thick_start;
  uintptr_t thick_end;
  const char *color;
  uintptr_t block_count;
  const char *block_sizes;
  const char *block_starts;
};

struct FASTAReaderC {
  void *inner_reader;
  const char *error;
};

struct FASTQReaderC {
  void *inner_reader;
};

struct GenbankReader {
  void *inner_reader;
  const char *error;
};

struct GenbankRecord {
  char *seq;
  char *accession;
  char *comments;
  char *contig;
  char *date;
  char *dblink;
  char *definition;
  char *division;
  char *keywords;
  char *molecule_type;
  char *name;
  char *titles;
  char *source;
  char *version;
  char *topology;
  char *features_json;
};

struct GFFReaderC {
  void *inner_reader;
};

struct GFFResult {
  char *error;
  bool done;
};

struct SamHeaderReaderC {
  void *inner_reader;
};

struct HeaderRecordC {
  const char *record_type;
  const char *tag;
  const char *value;
};

struct SamRecordReaderC {
  void *sam_reader;
  const void *sam_header;
};

struct VCFReaderC {
  void *inner_reader;
  void *header;
  const char *error;
};

struct CResult {
  const char *value;
  const char *error;
};

struct NoodlesWriter {
  void *writer;
  const char *error;
};

extern "C" {

BAMReaderC bam_new(const char *filename);

void bam_next(BAMReaderC *bam_reader, void *chunk_ptr, bool *done, uintptr_t chunk_size);

BcfReaderC bcf_new(const char *filename);

void bcf_next(BcfReaderC *bcf_reader, void *chunk_ptr, bool *done, uintptr_t chunk_size);

BEDReaderC bed_new(const char *filename, uint8_t n_columns, const char *compression);

BEDRecordC bed_next(const BEDReaderC *bam_reader, uint8_t n_columns);

FASTAReaderC fasta_new(const char *filename, const char *compression);

void fasta_next(const FASTAReaderC *fasta_reader,
                void *chunk_ptr,
                bool *done,
                uintptr_t batch_size);

void fasta_free(FASTAReaderC *fasta_reader);

FASTQReaderC fastq_new(const char *filename, const char *compression);

void fastq_next(const FASTQReaderC *fastq_reader,
                void *chunk_ptr,
                bool *done,
                uintptr_t batch_size);

void fastq_free(FASTQReaderC fastq_reader);

GenbankReader genbank_new(const char *filename, const char *compression);

void genbank_free(GenbankReader reader);

GenbankRecord genbank_next(const GenbankReader *reader);

GFFReaderC gff_new(const char *filename, const char *compression);

GFFResult gff_insert_record_batch(const GFFReaderC *gff_reader,
                                  void *chunk_ptr,
                                  uintptr_t batch_size);

SamHeaderReaderC sam_header_new_reader(const char *filename, const char *compression);

HeaderRecordC sam_header_read_records(const SamHeaderReaderC *c_reader);

SamRecordReaderC sam_record_new_reader(const char *filename, const char *compression);

void sam_record_read_records_chunk(const SamRecordReaderC *c_reader,
                                   void *ptr,
                                   bool *done,
                                   uintptr_t batch_size);

VCFReaderC vcf_new(const char *filename, const char *compression);

void vcf_next(VCFReaderC *vcf_reader, void *chunk_ptr, bool *done, uintptr_t chunk_size);

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

NoodlesWriter fasta_writer_new(const char *filename, const char *compression);

int32_t fasta_writer_write(void *writer, const char *id, const char *description, const char *seq);

void destroy_writer(void *writer);

void *fastq_writer_new(const char *filename, const char *compression);

int32_t fastq_writer_write(void *writer,
                           const char *id,
                           const char *description,
                           const char *seq,
                           const char *quality_scores);

void fastq_writer_destroy(void *writer);

void *gff_writer_new(const char *filename, const char *compression);

int32_t gff_writer_write(void *writer,
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
