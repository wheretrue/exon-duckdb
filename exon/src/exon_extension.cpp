#define DUCKDB_EXTENSION_MAIN

#include "exon_extension.hpp"
#include "exon/sam_functions/module.hpp"
#include "exon/arrow_table_function/module.hpp"
#include "exon/sequence_functions/module.hpp"
#include "exon/gff_functions/module.hpp"
#include "exon/fastq_functions/module.hpp"
#include "exon/core/module.hpp"

#if defined(WFA2_ENABLED)
#include "exon/alignment_functions/module.hpp"
#endif

#include "duckdb.hpp"

using namespace duckdb;

namespace duckdb
{

	static void LoadInternal(DatabaseInstance &instance)
	{
		Connection con(instance);
		con.BeginTransaction();

		auto &context = *con.context;
		auto &catalog = Catalog::GetSystemCatalog(context);

		auto &config = DBConfig::GetConfig(context);

		auto get_sam_functions = exon::SamFunctions::GetSamFunctions();
		for (auto &func : get_sam_functions)
		{
			catalog.CreateFunction(context, *func);
		}

		auto sequence_functions = exon::SequenceFunctions::GetSequenceFunctions();
		for (auto &fun : sequence_functions)
		{
			catalog.CreateFunction(context, fun);
		}

		exon::WTArrowTableFunction::Register("read_gff", "gff", context);
		exon::WTArrowTableFunction::Register("read_fasta", "fasta", context);
		exon::WTArrowTableFunction::Register("read_fastq", "fastq", context);
		exon::WTArrowTableFunction::Register("read_sam_file_records", "sam", context);
		exon::WTArrowTableFunction::Register("read_bam_file_records", "bam", context);
		exon::WTArrowTableFunction::Register("read_bed_file", "bed", context);
		exon::WTArrowTableFunction::Register("read_vcf_file_records", "vcf", context);
		exon::WTArrowTableFunction::Register("read_bcf_file_records", "bcf", context);
		exon::WTArrowTableFunction::Register("read_genbank", "genbank", context);
		exon::WTArrowTableFunction::Register("read_hmm_dom_tbl_out", "hmmdomtab", context);

		auto get_quality_scores_string_to_list = exon::FastqFunctions::GetQualityScoreStringToList();
		catalog.CreateFunction(context, *get_quality_scores_string_to_list);

		auto gff_parse_attributes = exon::GFFunctions::GetGFFParseAttributesFunction();
		catalog.CreateFunction(context, gff_parse_attributes);

		auto parse_cigar_string = exon::SamFunctions::GetParseCIGARStringFunction();
		catalog.CreateFunction(context, *parse_cigar_string);

		auto extract_sequence_from_cigar = exon::SamFunctions::GetExtractFromCIGARFunction();
		catalog.CreateFunction(context, *extract_sequence_from_cigar);

		// auto third_party_acks = exon::ExonDbFunctions::GetThirdPartyAcknowledgementTable();
		// catalog.CreateTableFunction(context, &third_party_acks);

		auto get_wtt01_version_function = exon::ExonDbFunctions::GetExonDbVersionFunction();
		catalog.CreateFunction(context, get_wtt01_version_function);

		config.replacement_scans.emplace_back(exon::WTArrowTableFunction::ReplacementScan);

#if defined(WFA2_ENABLED)
		auto get_align_function = exondb::AlignmentFunctions::GetAlignmentStringFunction("alignment_string_wfa_gap_affine");
		catalog.CreateFunction(context, get_align_function);

		auto get_align_function_default = exondb::AlignmentFunctions::GetAlignmentStringFunction("alignment_string");
		catalog.CreateFunction(context, get_align_function_default);

		auto get_align_score_function = exondb::AlignmentFunctions::GetAlignmentScoreFunction("alignment_score_wfa_gap_affine");
		catalog.CreateFunction(context, get_align_score_function);

		auto get_align_score_function_default = exondb::AlignmentFunctions::GetAlignmentScoreFunction("alignment_score");
		catalog.CreateFunction(context, get_align_score_function_default);
#endif

		con.Commit();
	}

	void ExonExtension::Load(DuckDB &db)
	{
		LoadInternal(*db.instance);
	}

	std::string ExonExtension::Name()
	{
		return "exon";
	}

} // namespace duckdb

extern "C"
{

	DUCKDB_EXTENSION_API void exon_init(duckdb::DatabaseInstance &db)
	{
		LoadInternal(db);
	}

	DUCKDB_EXTENSION_API const char *exon_version()
	{
		return duckdb::DuckDB::LibraryVersion();
	}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
