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
  const char *sam_header;
};

struct SamRecordC {
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

struct CResult {
  const char *value;
  const char *error;
};

struct NoodlesWriter {
  void *writer;
  const char *error;
};

struct WTGenotype {
  void *genotype;
};

struct WTGenotypes {
  void *genotypes;
  int32_t number_of_genotypes;
  int32_t genotype_position;
};

struct WTInfoField {
  const char *id;
  const char *ty;
  const char *number;
};

struct WTInfoFields {
  void *info_fields;
  int32_t info_field_position;
  int32_t total_fields;
};

struct WTTFormat {
  const char *id;
  const char *number;
  const char *ty;
  const char *description;
  const char *error;
};

struct WTFormats {
  void *formats;
  int32_t format_position;
  int32_t total_fields;
};

struct VCFReaderC {
  void *inner_reader;
  const char *header;
  WTFormats formats;
  WTInfoFields info_fields;
};

struct WTInfos {
  void *infos;
  int32_t number_of_infos;
  int32_t info_position;
};

struct VCFRecord {
  const char *chromosome;
  const char *ids;
  uint64_t position;
  const char *reference_bases;
  const char *alternate_bases;
  float quality_score;
  const char *filters;
  WTInfos infos;
  WTGenotypes genotypes;
};

struct WTIntKey {
  const char *error;
  bool none;
  int32_t value;
};

struct WTIntListElement {
  int32_t value;
  bool is_none;
  const char *error;
};

struct WTIntListValue {
  void *list;
};

struct WTIntListKey {
  const char *error;
  uint32_t length;
  uint32_t value_position;
  WTIntListValue value;
};

struct WTStringKey {
  const char *error;
  bool none;
  const char *value;
};

struct WTStringListValue {
  void *list;
};

struct WTStringListKey {
  const char *error;
  int32_t length;
  int32_t value_position;
  WTStringListValue value;
};

struct WTFloatKey {
  const char *error;
  bool none;
  float value;
};

struct WTFloatListValue {
  void *list;
};

struct WTFloatListKey {
  const char *error;
  int32_t length;
  int32_t value_position;
  WTFloatListValue value;
};

struct WTField {
  const char *id;
  const char *ty;
  const char *number;
  void *value;
  const char *error;
};

struct WTFieldList {
  void *fields;
  int32_t number_of_fields;
  int32_t field_position;
};

struct WTFloatValue {
  float value;
  bool is_null;
};

extern "C" {

BamRecordReaderC bam_record_new_reader(const char *filename, const char *compression);

BamRecordC bam_record_read_records(const BamRecordReaderC *c_reader);

FASTAReaderC fasta_new(const char *filename, const char *compression);

void fasta_next(const FASTAReaderC *fasta_reader, Record *record);

void fasta_free(FASTAReaderC *fasta_reader);

void fasta_record_free(Record record);

FASTQReaderC fastq_new(const char *filename, const char *compression);

FastqRecord fastq_next(const FASTQReaderC *fastq_reader);

void fastq_free(FASTQReaderC fastq_reader);

void fastq_record_free(FastqRecord record);

GFFReaderC gff_new(const char *filename, const char *compression);

GFFRecord gff_next(const GFFReaderC *gff_reader);

SamHeaderReaderC sam_header_new_reader(const char *filename, const char *compression);

HeaderRecordC sam_header_read_records(const SamHeaderReaderC *c_reader);

SamRecordReaderC sam_record_new_reader(const char *filename, const char *compression);

SamRecordC sam_record_read_records(const SamRecordReaderC *c_reader);

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

WTGenotype next_genotype(WTGenotypes *wt_genotypes);

WTInfoField next_info_field(WTInfoFields *wt_info_fields);

WTTFormat next_format(WTFormats *wt_formats);

VCFReaderC vcf_new(const char *filename, const char *compression);

const char *get_sample_names(const char *header);

VCFRecord vcf_next(const VCFReaderC *vcf_reader);

WTIntKey next_int_key(WTGenotype *wt_genotype, const char *key_str);

WTIntListElement next_int_list_value(WTIntListKey *wt_int_list_key);

WTIntListKey next_int_list_key(WTGenotype *wt_genotype, const char *key_str);

WTStringKey next_string_key(WTGenotype *wt_genotype, const char *key_str);

const char *next_string_list_value(WTStringListKey *wt_string_list_key);

WTStringListKey next_string_list_key(WTGenotype *wt_genotype, const char *key_str);

WTFloatKey next_float_key(WTGenotype *wt_genotype, const char *key_str);

float next_float_list_value(WTFloatListKey *wt_float_list_key);

WTFloatListKey next_float_list_key(WTGenotype *wt_genotype, const char *key_str);

WTField next_info(const char *key, WTInfos *wt_infos);

WTFieldList info_field_ints(WTField *wt_field);

int32_t info_field_int_value(WTFieldList *wt_field_list);

WTFieldList info_field_strings(WTField *wt_field);

WTFieldList info_field_floats(WTField *wt_field);

const char *info_field_string_value(WTFieldList *wt_field_list);

WTFloatValue info_field_float_value(WTFieldList *wt_field_list);

int32_t info_field_int(WTField *wt_field);

float info_field_float(WTField *wt_field);

const char *info_field_string(WTField *wt_field);

bool info_field_bool(WTField *wt_field);

} // extern "C"
