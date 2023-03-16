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

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("accession");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("comments");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("contig");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("date");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("dblink");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("definition");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("division");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("keywords");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("molecule_type");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("name");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("titles");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("source");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("version");

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        names.push_back("topology");

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

            if (record.seq == nullptr)
            {
                local_state->done = true;
                break;
            }

            output.SetValue(0, output.size(), duckdb::Value(record.seq));

            if (record.accession == nullptr)
            {
                output.SetValue(1, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(1, output.size(), duckdb::Value(record.accession));
            }

            if (record.comments == nullptr)
            {
                output.SetValue(2, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(2, output.size(), duckdb::Value(record.comments));
            }

            if (record.contig == nullptr)
            {
                output.SetValue(3, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(3, output.size(), duckdb::Value(record.contig));
            }

            if (record.date == nullptr)
            {
                output.SetValue(4, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(4, output.size(), duckdb::Value(record.date));
            }

            if (record.dblink == nullptr)
            {
                output.SetValue(5, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(5, output.size(), duckdb::Value(record.dblink));
            }

            if (record.definition == nullptr)
            {
                output.SetValue(6, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(6, output.size(), duckdb::Value(record.definition));
            }

            if (record.division == nullptr)
            {
                output.SetValue(7, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(7, output.size(), duckdb::Value(record.division));
            }

            if (record.keywords == nullptr)
            {
                output.SetValue(8, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(8, output.size(), duckdb::Value(record.keywords));
            }

            if (record.molecule_type == nullptr)
            {
                output.SetValue(9, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(9, output.size(), duckdb::Value(record.molecule_type));
            }

            if (record.name == nullptr)
            {
                output.SetValue(10, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(10, output.size(), duckdb::Value(record.name));
            }

            if (record.titles == nullptr)
            {
                output.SetValue(11, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(11, output.size(), duckdb::Value(record.titles));
            }

            if (record.source == nullptr)
            {
                output.SetValue(12, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(12, output.size(), duckdb::Value(record.source));
            }

            if (record.version == nullptr)
            {
                output.SetValue(13, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(13, output.size(), duckdb::Value(record.version));
            }

            if (record.topology == nullptr)
            {
                output.SetValue(14, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(14, output.size(), duckdb::Value(record.topology));
            }

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
