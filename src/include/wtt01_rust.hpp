#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class WTTPhase {
  None,
  Zero,
  One,
  Two,
};

struct BamRecordReaderC {
  void *bam_reader;
  const char *bam_header;
};

struct BamRecordC {
  const char *sequence;
  const char *read_name;
  uint16_t flags;
  int64_t alignment_start;
  int64_t alignment_end;
  const char *cigar_string;
  const char *quality_scores;
  int64_t template_length;
  int64_t mapping_quality;
  int64_t mate_alignment_start;
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

struct Record {
  const char *id;
  const char *description;
  const char *sequence;
  bool done;
};

struct FASTQReaderC {
  void *inner_reader;
};

struct FastqRecord {
  const char *name;
  const char *description;
  const char *sequence;
  const char *quality_scores;
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

struct GFFRecord {
  const char *reference_sequence_name;
  const char *source;
  const char *annotation_type;
  uint64_t start;
  uint64_t end;
  float score;
  const char *strand;
  WTTPhase phase;
  const char *attributes;
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
  const char *header;
};

struct VCFRecord {
  const char *chromosome;
  const char *ids;
  uint64_t position;
  const char *reference_bases;
  const char *alternate_bases;
  float quality_score;
  const char *filters;
  const char *infos;
  const char *genotypes;
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

BamRecordReaderC bam_record_new_reader(const char *filename, const char *compression);

BamRecordC bam_record_read_records(const BamRecordReaderC *c_reader);

BEDReaderC bed_new(const char *filename, uint8_t n_columns, const char *compression);

BEDRecordC bed_next(const BEDReaderC *bam_reader, uint8_t n_columns);

FASTAReaderC fasta_new(const char *filename, const char *compression);

void fasta_next(const FASTAReaderC *fasta_reader, Record *record);

void fasta_free(FASTAReaderC *fasta_reader);

void fasta_record_free(Record record);

FASTQReaderC fastq_new(const char *filename, const char *compression);

FastqRecord fastq_next(const FASTQReaderC *fastq_reader);

void fastq_free(FASTQReaderC fastq_reader);

void fastq_record_free(FastqRecord record);

GenbankReader genbank_new(const char *filename, const char *compression);

void genbank_free(GenbankReader reader);

GenbankRecord genbank_next(const GenbankReader *reader);

GFFReaderC gff_new(const char *filename, const char *compression);

GFFRecord gff_next(const GFFReaderC *gff_reader);

SamHeaderReaderC sam_header_new_reader(const char *filename, const char *compression);

HeaderRecordC sam_header_read_records(const SamHeaderReaderC *c_reader);

SamRecordReaderC sam_record_new_reader(const char *filename, const char *compression);

void sam_record_read_records_chunk(const SamRecordReaderC *c_reader,
                                   void *ptr,
                                   bool *done,
                                   uintptr_t batch_size);

VCFReaderC vcf_new(const char *filename, const char *compression);

VCFRecord vcf_next(const VCFReaderC *vcf_reader);

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
