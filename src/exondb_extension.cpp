// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
#include "arrow_table_function.hpp"

#include "exondb_functions.hpp"

namespace duckdb
{

    static void LoadInternal(DatabaseInstance &instance)
    {
        Connection con(instance);
        con.BeginTransaction();

        auto &context = *con.context;

        auto &catalog = Catalog::GetSystemCatalog(context);
        auto &config = DBConfig::GetConfig(context);

        auto sequence_functions = exondb::SequenceFunctions::GetSequenceFunctions();
        for (auto &fun : sequence_functions)
        {
            catalog.CreateFunction(context, fun);
        }

        exondb::WTArrowTableFunction::Register("read_gff", "gff", context);
        exondb::WTArrowTableFunction::Register("read_fasta", "fasta", context);
        exondb::WTArrowTableFunction::Register("read_fastq", "fastq", context);
        exondb::WTArrowTableFunction::Register("read_sam_file_records", "sam", context);
        exondb::WTArrowTableFunction::Register("read_bam_file_records", "bam", context);
        exondb::WTArrowTableFunction::Register("read_bed_file", "bed", context);
        exondb::WTArrowTableFunction::Register("read_vcf_file_records", "vcf", context);
        exondb::WTArrowTableFunction::Register("read_bcf_file_records", "bcf", context);
        exondb::WTArrowTableFunction::Register("read_genbank", "genbank", context);
        exondb::WTArrowTableFunction::Register("read_hmm_dom_tbl_out", "hmmdomtab", context);

        auto get_quality_scores_string_to_list = exondb::FastqFunctions::GetQualityScoreStringToList();
        catalog.CreateFunction(context, *get_quality_scores_string_to_list);

        auto gff_parse_attributes = exondb::GFFunctions::GetGFFParseAttributesFunction();
        catalog.CreateFunction(context, gff_parse_attributes);

        auto parse_cigar_string = exondb::SamFunctions::GetParseCIGARStringFunction();
        catalog.CreateFunction(context, *parse_cigar_string);

        auto extract_sequence_from_cigar = exondb::SamFunctions::GetExtractFromCIGARFunction();
        catalog.CreateFunction(context, *extract_sequence_from_cigar);

        auto third_party_acks = exondb::Wtt01Functions::GetThirdPartyAcknowledgementTable();
        catalog.CreateTableFunction(context, &third_party_acks);

        auto get_sam_functions = exondb::SamFunctions::GetSamFunctions();
        for (auto &func : get_sam_functions)
        {
            catalog.CreateFunction(context, *func);
        }

        auto get_wtt01_version_function = exondb::Wtt01Functions::GetWtt01VersionFunction();
        catalog.CreateFunction(context, get_wtt01_version_function);

        config.replacement_scans.emplace_back(exondb::WTArrowTableFunction::ReplacementScan);

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
