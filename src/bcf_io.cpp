#include <iostream>

#include <duckdb.hpp>
#include <duckdb.h>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/main/capi/capi_internal.hpp>

#include "wtt01_rust.hpp"
#include "bcf_io.hpp"

namespace wtt01
{
    struct BcfRecordScanOptions
    {
    };

    struct BcfRecordScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        BcfRecordScanOptions options;
        BcfReaderC reader;
    };

    struct BcfRecordScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        BcfReaderC reader;
    };

    struct BcfRecordScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        BcfRecordScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> BcfRecordBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                           std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<BcfRecordScanBindData>();

        auto filepath = input.inputs[0].GetValue<std::string>();
        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        if (!fs.FileExists(filepath))
        {
            throw duckdb::IOException("File does not exist: " + filepath);
        }

        result->file_path = filepath;

        auto reader = bcf_new(result->file_path.c_str());
        if (reader.error)
        {
            auto error_msg = std::string(reader.error);
            throw duckdb::IOException("Error opening Bcf file: " + filepath + " " + error_msg);
        }

        result->reader = reader;

        names.push_back("chromosome");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("ids");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("position");
        return_types.push_back(duckdb::LogicalType::BIGINT);

        names.push_back("reference_bases");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("alternate_bases");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("quality_score");
        return_types.push_back(duckdb::LogicalType::FLOAT);

        names.push_back("filter");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("info");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("genotypes");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> BcfRecordInitGlobalState(duckdb::ClientContext &context,
                                                                                  duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<BcfRecordScanGlobalState>();
        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> BcfRecordInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                                duckdb::GlobalTableFunctionState *global_state)
    {
        auto local_state = duckdb::make_unique<BcfRecordScanLocalState>();
        return std::move(local_state);
    }

    void BcfRecordScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (BcfRecordScanBindData *)data.bind_data;
        auto local_state = (BcfRecordScanLocalState *)data.local_state;
        auto gstate = (BcfRecordScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        bcf_next(&bind_data->reader, &output, &local_state->done, STANDARD_VECTOR_SIZE);
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> BcfFunctions::GetBcfRecordScanFunction()
    {
        auto sam_table_function = duckdb::TableFunction("read_bcf_file_records", {duckdb::LogicalType::VARCHAR}, BcfRecordScan, BcfRecordBind, BcfRecordInitGlobalState, BcfRecordInitLocalState);

        duckdb::CreateTableFunctionInfo sam_table_function_info(sam_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(sam_table_function_info);
    }
}
