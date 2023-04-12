#include <iostream>
#include <cmath>

#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include <duckdb/function/table/arrow.hpp>
#include "duckdb/common/unordered_map.hpp"

#include "gtf_io.hpp"
#include "wtt01_rust.hpp"

namespace wtt01
{

    struct GtfScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct GtfScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        GtfScanOptions options;
        GTFReaderC reader;

        std::unordered_map<idx_t, std::unique_ptr<duckdb::ArrowConvertData>> arrow_convert_data;
    };

    struct GtfScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        GTFReaderC reader;
    };

    struct GtfScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        GtfScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> GtfBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                     std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<GtfScanBindData>();
        auto file_name = input.inputs[0].GetValue<std::string>();

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        if (!fs.FileExists(file_name))
        {
            throw duckdb::IOException("File does not exist: " + file_name);
        }

        result->file_path = file_name;

        for (auto &kv : input.named_parameters)
        {
            if (kv.first == "compression")
            {
                result->options.compression = kv.second.GetValue<std::string>();
            }
            else
            {
                throw std::runtime_error("Unknown named parameter: " + kv.first);
            }
        };

        auto reader = gtf_new(result->file_path.c_str(), result->options.compression.c_str());
        result->reader = reader;

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("reference_sequence_name");

        result->arrow_convert_data[0] = duckdb::make_unique<duckdb::ArrowConvertData>(duckdb::LogicalType::VARCHAR);

        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> GtfInitGlobalState(duckdb::ClientContext &context,
                                                                            duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<GtfScanGlobalState>();
        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> GtfInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                          duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const GtfScanBindData *)input.bind_data;
        auto &gstate = (GtfScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<GtfScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return std::move(local_state);
    }

    void GtfScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const GtfScanBindData *)data.bind_data;
        auto local_state = (GtfScanLocalState *)data.local_state;
        auto gstate = (GtfScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        auto result = gtf_insert_record_batch(&bind_data->reader, STANDARD_VECTOR_SIZE);

        auto array_len = result.array.length;
        printf("array_len: %d\n", array_len);

        if (result.done)
        {
            local_state->done = true;
        }

        if (result.error)
        {
            throw std::runtime_error(result.error);
        }

        for (int i = 0; i < array_len; i++)
        {
            auto record = result.array;

            auto child = record.children[0];

            auto cdata = (char *)record.buffers[2];
            // printf("record: %d\n", record.length);

            // printf("record: %s\n", record.children[0]);

            // output.SetValue(0, i, reference_sequence_name_str);
        }
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GTFunctions::GetGtfTableFunction()
    {
        auto gff_table_function = duckdb::TableFunction("read_gtf", {duckdb::LogicalType::VARCHAR}, GtfScan, GtfBind, GtfInitGlobalState, GtfInitLocalState);
        gff_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo gff_table_function_info(gff_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(gff_table_function_info);
    }

}
