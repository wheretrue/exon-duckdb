#define DUCKDB_EXTENSION_MAIN

#include "wtt01_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "bam_io.hpp"
#include "bed_io.hpp"
#include "fasta_io.hpp"
#include "fastq_io.hpp"
#include "genbank_io.hpp"
#include "gff_io.hpp"
#include "hmm_io.hpp"
#include "sam_io.hpp"
#include "sequence_functions.hpp"
#include "vcf_io.hpp"

#include "wtt01_functions.hpp"

#include "check_license.hpp"

namespace duckdb
{

    static void LoadInternal(DatabaseInstance &instance)
    {
#if defined(CHECK_LICENSE)
        try
        {
            wtt01::LicenseCheck::ValidateLicense();
        }
        catch (std::exception &e)
        {
            throw InvalidInputException(std::string("License verification failed: " + std::string(e.what())));
        }
#endif
        Connection con(instance);
        con.BeginTransaction();

        auto &context = *con.context;

        auto &catalog = Catalog::GetSystemCatalog(context);
        auto &config = DBConfig::GetConfig(context);

        auto sequence_functions = wtt01::SequenceFunctions::GetSequenceFunctions();
        for (auto &fun : sequence_functions)
        {
            catalog.CreateFunction(context, &fun);
        }

        auto fasta_scan = wtt01::FastaIO::GetFastaTableFunction();
        catalog.CreateTableFunction(context, fasta_scan.get());

        auto fasta_copy = wtt01::FastaIO::GetFastaCopyFunction();
        catalog.CreateCopyFunction(context, fasta_copy.get());

        auto fasta_replacement_scan = wtt01::FastaIO::GetFastaReplacementScanFunction;
        config.replacement_scans.emplace_back(fasta_replacement_scan);

        auto get_quality_scores_string_to_list = wtt01::FastqFunctions::GetQualityScoreStringToList();
        catalog.CreateFunction(*con.context, get_quality_scores_string_to_list.get());

        auto fastq_scan = wtt01::FastqFunctions::GetFastqTableFunction();
        catalog.CreateTableFunction(context, fastq_scan.get());

        auto fastq_copy = wtt01::FastqFunctions::GetFastqCopyFunction();
        catalog.CreateCopyFunction(context, fastq_copy.get());
        config.replacement_scans.emplace_back(wtt01::FastqFunctions::GetFastqReplacementScanFunction);

        auto genbank_table = wtt01::GenbankFunctions::GetGenbankTableFunction();
        catalog.CreateTableFunction(context, genbank_table.get());
        config.replacement_scans.emplace_back(wtt01::GenbankFunctions::GetGenbankReplacementScanFunction);

        auto gff_scan = wtt01::GFFunctions::GetGffTableFunction();
        catalog.CreateTableFunction(context, gff_scan.get());

        auto gff_copy = wtt01::GFFunctions::GetGffCopyFunction();
        catalog.CreateCopyFunction(context, gff_copy.get());

        auto gff_raw_scan = wtt01::GFFunctions::GetGFFRawTableFunction();
        catalog.CreateTableFunction(context, gff_raw_scan.get());

        auto gff_parse_attributes = wtt01::GFFunctions::GetGFFParseAttributesFunction();
        catalog.CreateFunction(context, gff_parse_attributes.get());

        config.replacement_scans.emplace_back(wtt01::GFFunctions::GetGffReplacementScanFunction);

        auto vcf_scan = wtt01::VCFFunctions::GetVCFRecordScanFunction();
        catalog.CreateTableFunction(context, vcf_scan.get());

        config.replacement_scans.emplace_back(wtt01::VCFFunctions::GetVcfReplacementScanFunction);

        auto parse_cigar_string = wtt01::SamFunctions::GetParseCIGARStringFunction();
        catalog.CreateFunction(context, parse_cigar_string.get());

        auto sam_record_scan = wtt01::SamFunctions::GetSamRecordScanFunction();
        catalog.CreateTableFunction(context, sam_record_scan.get());

        auto sam_header_scan = wtt01::SamFunctions::GetSamHeaderScanFunction();
        catalog.CreateTableFunction(context, sam_header_scan.get());

        auto third_party_acks = wtt01::Wtt01Functions::GetThirdPartyAcknowledgementTable();
        catalog.CreateTableFunction(context, &third_party_acks);

        auto get_sam_functions = wtt01::SamFunctions::GetSamFunctions();
        for (auto &func : get_sam_functions)
        {
            catalog.CreateFunction(*con.context, func.get());
        }

        auto get_bam_record_scan = wtt01::BamFunctions::GetBamRecordScanFunction();
        catalog.CreateTableFunction(context, get_bam_record_scan.get());

        auto get_wtt01_version_function = wtt01::Wtt01Functions::GetWtt01VersionFunction();
        catalog.CreateFunction(*con.context, &get_wtt01_version_function);

        auto get_hmm_scan_function = wtt01::HMMFunctions::GetHMMScanFunction();
        catalog.CreateTableFunction(*con.context, get_hmm_scan_function.get());

        auto get_bed_scan_function = wtt01::BEDFunctions::GetBEDTableFunction();
        catalog.CreateTableFunction(*con.context, get_bed_scan_function.get());

        auto bed_replacement_scan = wtt01::BEDFunctions::GetBEDReplacementScanFunction;
        config.replacement_scans.emplace_back(bed_replacement_scan);

        con.Commit();
    }

    void Wtt01Extension::Load(DuckDB &db)
    {
        LoadInternal(*db.instance);
    }
    std::string Wtt01Extension::Name()
    {
        return "wtt01";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void wtt01_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
    }

    DUCKDB_EXTENSION_API const char *wtt01_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
