#define DUCKDB_EXTENSION_MAIN

#include "exondb_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "alignment_functions.hpp"
#include "gff_io.hpp"
#include "sam_io.hpp"
#include "fastq_io.hpp"
#include "sequence_functions.hpp"
#include "wt_arrow_table_function.hpp"

#include "wtt01_functions.hpp"

namespace duckdb
{

    static void LoadInternal(DatabaseInstance &instance)
    {
        Connection con(instance);
        con.BeginTransaction();

        auto &context = *con.context;

        auto &catalog = Catalog::GetSystemCatalog(context);
        auto &config = DBConfig::GetConfig(context);

        auto sequence_functions = wtt01::SequenceFunctions::GetSequenceFunctions();
        for (auto &fun : sequence_functions)
        {
            catalog.CreateFunction(context, fun);
        }

        wtt01::WTArrowTableFunction::Register("read_gff", "gff", context);
        wtt01::WTArrowTableFunction::Register("read_fasta", "fasta", context);
        wtt01::WTArrowTableFunction::Register("read_fastq", "fastq", context);
        wtt01::WTArrowTableFunction::Register("read_sam_file_records", "sam", context);
        wtt01::WTArrowTableFunction::Register("read_bam_file_records", "bam", context);
        wtt01::WTArrowTableFunction::Register("read_bed_file", "bed", context);
        wtt01::WTArrowTableFunction::Register("read_vcf_file_records", "vcf", context);
        wtt01::WTArrowTableFunction::Register("read_bcf_file_records", "bcf", context);
        wtt01::WTArrowTableFunction::Register("read_genbank", "genbank", context);
        wtt01::WTArrowTableFunction::Register("read_hmm_dom_tbl_out", "hmmdomtab", context);

        auto get_quality_scores_string_to_list = wtt01::FastqFunctions::GetQualityScoreStringToList();
        catalog.CreateFunction(context, *get_quality_scores_string_to_list);

        auto gff_parse_attributes = wtt01::GFFunctions::GetGFFParseAttributesFunction();
        catalog.CreateFunction(context, gff_parse_attributes);

        auto parse_cigar_string = wtt01::SamFunctions::GetParseCIGARStringFunction();
        catalog.CreateFunction(context, *parse_cigar_string);

        auto extract_sequence_from_cigar = wtt01::SamFunctions::GetExtractFromCIGARFunction();
        catalog.CreateFunction(context, *extract_sequence_from_cigar);

        auto third_party_acks = wtt01::Wtt01Functions::GetThirdPartyAcknowledgementTable();
        catalog.CreateTableFunction(context, &third_party_acks);

        auto get_sam_functions = wtt01::SamFunctions::GetSamFunctions();
        for (auto &func : get_sam_functions)
        {
            catalog.CreateFunction(context, *func);
        }

        auto get_wtt01_version_function = wtt01::Wtt01Functions::GetWtt01VersionFunction();
        catalog.CreateFunction(context, get_wtt01_version_function);

        config.replacement_scans.emplace_back(wtt01::WTArrowTableFunction::ReplacementScan);

#if defined(WFA2_ENABLED)
        auto get_align_function = wtt01::AlignmentFunctions::GetAlignmentStringFunction("alignment_string_wfa_gap_affine");
        catalog.CreateFunction(context, get_align_function);

        auto get_align_function_default = wtt01::AlignmentFunctions::GetAlignmentStringFunction("alignment_string");
        catalog.CreateFunction(context, get_align_function_default);

        auto get_align_score_function = wtt01::AlignmentFunctions::GetAlignmentScoreFunction("alignment_score_wfa_gap_affine");
        catalog.CreateFunction(context, get_align_score_function);

        auto get_align_score_function_default = wtt01::AlignmentFunctions::GetAlignmentScoreFunction("alignment_score");
        catalog.CreateFunction(context, get_align_score_function_default);
#endif

        con.Commit();
    }

    void ExondbExtension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }
    std::string ExondbExtension::Name()
    {
        return "exondb";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void exondb_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
    }

    DUCKDB_EXTENSION_API const char *exondb_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
