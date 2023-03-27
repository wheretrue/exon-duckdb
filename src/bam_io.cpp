#include <iostream>

#include <duckdb.hpp>
#include <duckdb.h>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/main/capi/capi_internal.hpp>

#include "wtt01_rust.hpp"
#include "bam_io.hpp"

namespace wtt01
{
    struct BamRecordScanOptions
    {
    };

    struct BamRecordScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        BamRecordScanOptions options;
        BAMReaderC reader;
    };

    struct BamRecordScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        BAMReaderC reader;
    };

    struct BamRecordScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        BamRecordScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> BamRecordBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                           std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<BamRecordScanBindData>();

        auto filepath = input.inputs[0].GetValue<std::string>();
        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        if (!fs.FileExists(filepath))
        {
            throw duckdb::IOException("File does not exist: " + filepath);
        }

        result->file_path = filepath;

        auto reader = bam_new(result->file_path.c_str());
        if (reader.error)
        {
            auto error_msg = std::string(reader.error);
            throw duckdb::IOException("Error opening BAM file: " + filepath + " " + error_msg);
        }

        result->reader = reader;

        names.push_back("sequence");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("read_name");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("flags");
        return_types.push_back(duckdb::LogicalType::INTEGER);

        names.push_back("alignment_start");
        return_types.push_back(duckdb::LogicalType::BIGINT);

        names.push_back("alignment_end");
        return_types.push_back(duckdb::LogicalType::BIGINT);

        names.push_back("cigar_string");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("quality_scores");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("template_length");
        return_types.push_back(duckdb::LogicalType::BIGINT);

        names.push_back("mapping_quality");
        return_types.push_back(duckdb::LogicalType::INTEGER);

        names.push_back("mate_alignment_start");
        return_types.push_back(duckdb::LogicalType::BIGINT);

        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> BamRecordInitGlobalState(duckdb::ClientContext &context,
                                                                                  duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<BamRecordScanGlobalState>();
        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> BamRecordInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                                duckdb::GlobalTableFunctionState *global_state)
    {
        // auto bind_data = (const BamRecordScanBindData *)input.bind_data;
        // auto &gstate = (BamRecordScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<BamRecordScanLocalState>();

        return std::move(local_state);
    }

    void BamRecordScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (BamRecordScanBindData *)data.bind_data;
        auto local_state = (BamRecordScanLocalState *)data.local_state;
        auto gstate = (BamRecordScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        bam_next(&bind_data->reader, &output, &local_state->done, STANDARD_VECTOR_SIZE);
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> BamFunctions::GetBamRecordScanFunction()
    {
        auto sam_table_function = duckdb::TableFunction("read_bam_file_records", {duckdb::LogicalType::VARCHAR}, BamRecordScan, BamRecordBind, BamRecordInitGlobalState, BamRecordInitLocalState);

        duckdb::CreateTableFunctionInfo sam_table_function_info(sam_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(sam_table_function_info);
    }
}
