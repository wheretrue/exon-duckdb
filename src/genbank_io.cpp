#include <iostream>
#include <cmath>

#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/function/table/read_csv.hpp>

#include <nlohmann/json.hpp>
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
        std::vector<std::string> file_paths;
        int nth_file = 0;

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
        auto glob = input.inputs[0].GetValue<std::string>();

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        auto glob_result = fs.Glob(glob);

        if (glob_result.empty())
        {
            throw duckdb::IOException("No files found for glob: " + glob);
        }

        result->file_paths = glob_result;

        for (auto &kv : input.named_parameters)
        {
            if (kv.first == "compression")
            {
                result->options.compression = kv.second.GetValue<std::string>();
            }
            else
            {
                throw duckdb::Exception("Unknown parameter: " + kv.first);
            }
        }

        auto reader = genbank_new(result->file_paths[0].c_str(), result->options.compression.c_str());
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

        duckdb::child_list_t<duckdb::LogicalType> features_children;

        features_children.push_back(std::make_pair("kind", duckdb::LogicalType::VARCHAR));
        features_children.push_back(std::make_pair("location", duckdb::LogicalType::VARCHAR));

        features_children.push_back(std::make_pair("qualifiers", duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR)));

        names.push_back("features");
        return_types.push_back(
            duckdb::LogicalType::LIST(duckdb::LogicalType::STRUCT(features_children)));

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
        auto bind_data = (GenbankScanBindData *)data.bind_data;
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
                if (bind_data->nth_file + 1 < bind_data->file_paths.size())
                {
                    bind_data->nth_file++;
                    genbank_free(bind_data->reader);
                    bind_data->reader = genbank_new(bind_data->file_paths[bind_data->nth_file].c_str(), bind_data->options.compression.c_str());
                    continue;
                }

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

            auto feature_json = record.features_json;
            if (feature_json == nullptr)
            {
                output.SetValue(15, output.size(), duckdb::Value());
            }
            else
            {
                std::vector<duckdb::Value> features_structs;

                auto doc = nlohmann::json::parse(feature_json);

                for (nlohmann::json::iterator it = doc.begin(); it != doc.end(); ++it)
                {
                    auto doc_i = it.value();

                    auto kind = doc_i["kind"];

                    auto location = doc_i["location"];

                    auto qualifiers = doc_i["qualifiers"];

                    duckdb::child_list_t<duckdb::Value> struct_values;

                    struct_values.push_back(std::make_pair("kind", duckdb::Value(kind.get<std::string>())));
                    struct_values.push_back(std::make_pair("location", duckdb::Value(location.get<std::string>())));

                    // Collect the individual qualifiers into a map
                    std::vector<duckdb::Value> qualifiers_structs;
                    for (nlohmann::json::iterator it = qualifiers.begin(); it != qualifiers.end(); ++it)
                    {
                        auto qualifier_i = it.value();

                        auto key = qualifier_i["key"];
                        auto value = qualifier_i["value"];

                        duckdb::child_list_t<duckdb::Value> qualifier_struct_values;

                        qualifier_struct_values.push_back(std::make_pair("key", duckdb::Value(key.get<std::string>())));

                        if (value.is_null())
                        {
                            qualifier_struct_values.push_back(std::make_pair("value", duckdb::Value()));
                        }
                        else
                        {
                            qualifier_struct_values.push_back(std::make_pair("value", duckdb::Value(value.get<std::string>())));
                        }

                        qualifiers_structs.push_back(duckdb::Value::STRUCT(qualifier_struct_values));
                    }

                    duckdb::LogicalType map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR);
                    auto outputValue = duckdb::Value::MAP(duckdb::ListType::GetChildType(map_type), std::move(qualifiers_structs));

                    struct_values.push_back(std::make_pair("qualifiers", outputValue));

                    features_structs.push_back(duckdb::Value::STRUCT(struct_values));
                }

                output.SetValue(15, output.size(), duckdb::Value::LIST(features_structs));
            }

            output.SetCardinality(output.size() + 1);
        }
    }

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GenbankFunctions::GetGenbankTableFunction()
    {
        auto genbank_table_function = duckdb::TableFunction("read_genbank", {duckdb::LogicalType::VARCHAR}, GenbankScan, GenbankBind, GenbankInitGlobalState, GenbankInitLocalState);
        genbank_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo genbank_table_function_info(genbank_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(genbank_table_function_info);
    }

    duckdb::unique_ptr<duckdb::TableRef> GenbankFunctions::GetGenbankReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data)
    {
        auto table_function = duckdb::make_unique<duckdb::TableFunctionRef>();

        auto valid_fasta_filename = duckdb::StringUtil::EndsWith(table_name, ".genbank") || duckdb::StringUtil::EndsWith(table_name, ".gb");

        if (!valid_fasta_filename)
        {
            return nullptr;
        };

        auto &fs = duckdb::FileSystem::GetFileSystem(context);

        if (!(fs.FileExists(table_name)))
        {
            return nullptr;
        };

        std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
        children.push_back(duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value(table_name)));

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_genbank", move(children));

        return table_function;
    }
}
