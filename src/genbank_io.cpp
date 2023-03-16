#include <iostream>
#include <cmath>

#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/function/table/read_csv.hpp>

#include "genbank_io.hpp"
#include "wtt01_rust.hpp"

namespace wtt01
{

    struct GenbankScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct GenbankScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        GenbankScanOptions options;
        GenbankReader reader;
    };

    struct GenbankScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        GenbankReader reader;
    };

    struct GenbankScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        GenbankScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> GenbankBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                         std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<GenbankScanBindData>();
        auto file_name = input.inputs[0].GetValue<std::string>();

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        if (!fs.FileExists(file_name))
        {
            throw duckdb::IOException("File does not exist: " + file_name);
        }

        result->file_path = file_name;

        auto reader = genbank_new(result->file_path.c_str());
        result->reader = reader;

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("sequence");

        return move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> GenbankInitGlobalState(duckdb::ClientContext &context,
                                                                                duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<GenbankScanGlobalState>();
        return move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> GenbankInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                              duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const GenbankScanBindData *)input.bind_data;
        auto &gstate = (GenbankScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<GenbankScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return move(local_state);
    }

    void GenbankScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const GenbankScanBindData *)data.bind_data;
        auto local_state = (GenbankScanLocalState *)data.local_state;
        auto gstate = (GenbankScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        while (output.size() < STANDARD_VECTOR_SIZE)
        {
            auto record = genbank_next(&bind_data->reader);

            if (record == nullptr)
            {
                local_state->done = true;
                break;
            }

            output.SetValue(0, output.size(), duckdb::Value(record));
            output.SetCardinality(output.size() + 1);
        }
    }

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GenbankFunctions::GetGenbankTableFunction()
    {
        auto genbank_table_function = duckdb::TableFunction("read_genbank", {duckdb::LogicalType::VARCHAR}, GenbankScan, GenbankBind, GenbankInitGlobalState, GenbankInitLocalState);

        duckdb::CreateTableFunctionInfo genbank_table_function_info(genbank_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(genbank_table_function_info);
    }
}
