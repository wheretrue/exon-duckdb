#include <iostream>
#include <cmath>

#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/function/table/read_csv.hpp>

#include "gff_io.hpp"
#include "wtt01_rust.hpp"

namespace wtt01
{

    struct GFFRawScanOptions
    {
    };

    struct GFFRawSchemaKeyValue
    {
        const char *function_name;
        duckdb::LogicalType return_type;
    };

    duckdb::unique_ptr<duckdb::FunctionData> GFFRawScanBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                            duckdb::vector<duckdb::LogicalType> &return_types, duckdb::vector<std::string> &names)
    {
        auto result = duckdb::make_uniq<duckdb::ReadCSVData>();

        std::vector<std::string> file_patterns;
        auto file_name = input.inputs[0].GetValue<std::string>();

        // result->InitializeFiles(context, file_patterns);
        result->files.push_back(file_name);

        result->options.delimiter = '\t';
        result->options.auto_detect = false;
        result->options.has_header = true;

        // result->options.include_file_name = false;
        // result->options.include_parsed_hive_partitions = false;
        result->options.file_path = file_name;
        result->options.null_str = ".";

        result->options.ignore_errors = true;

        auto &options = result->options;

        duckdb::vector<GFFRawSchemaKeyValue> key_values = {
            {"reference_sequence_name", duckdb::LogicalType::VARCHAR},
            {"source", duckdb::LogicalType::VARCHAR},
            {"annotation_type", duckdb::LogicalType::VARCHAR},
            {"start", duckdb::LogicalType::BIGINT},
            {"end", duckdb::LogicalType::BIGINT},
            {"score", duckdb::LogicalType::FLOAT},
            {"strand", duckdb::LogicalType::VARCHAR},
            {"phase", duckdb::LogicalType::VARCHAR},
            {"attributes", duckdb::LogicalType::VARCHAR},
        };

        for (auto &key_value : key_values)
        {
            names.push_back(key_value.function_name);
            return_types.push_back(key_value.return_type);

            // result->sql_types.emplace_back(key_value.return_type);
        }

        result->FinalizeRead(context);

        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GFFunctions::GetGFFRawTableFunction()
    {
        duckdb::ReadCSVTableFunction read_csv;
        auto func = read_csv.GetFunction();

        auto gff_raw_table_function = duckdb::TableFunction("read_gff_raw",
                                                            {duckdb::LogicalType::VARCHAR},
                                                            func.function,
                                                            GFFRawScanBind,
                                                            func.init_global,
                                                            func.init_local);

        duckdb::CreateTableFunctionInfo info(gff_raw_table_function);

        return duckdb::make_uniq<duckdb::CreateTableFunctionInfo>(info);
    };

    duckdb::CreateScalarFunctionInfo GFFunctions::GetGFFParseAttributesFunction()
    {
        duckdb::ScalarFunctionSet set("gff_parse_attributes");

        auto duckdb_function = [](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
        {
            result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

            // TODO: add soft failure
            for (duckdb::idx_t i = 0; i < args.size(); i++)
            {
                auto value = args.data[0].GetValue(i);
                auto string_value = duckdb::StringValue::Get(value);

                auto last_semicolon_pos = string_value.find_last_of(";");
                auto string_len = string_value.size();
                if (last_semicolon_pos == string_len)
                {
                    string_value = string_value.substr(0, string_len - 1);
                }

                auto attributes = duckdb::StringUtil::Split(string_value, ";");

                duckdb::vector<duckdb::Value> items;

                for (auto &attribute : attributes)
                {
                    duckdb::StringUtil::Trim(attribute);
                    auto key_value = duckdb::StringUtil::Split(attribute, "=");

                    if (key_value.size() != 2)
                    {
                        throw duckdb::Exception("Invalid attribute: '" + attribute + "' expected 'key=value;key2=value2'");
                    }

                    duckdb::child_list_t<duckdb::Value> map_struct;

                    auto new_key = duckdb::Value(key_value[0]);
                    auto new_value = duckdb::Value(key_value[1]);

                    map_struct.emplace_back(std::make_pair("key", std::move(new_key)));
                    map_struct.emplace_back(std::make_pair("value", std::move(new_value)));

                    items.push_back(duckdb::Value::STRUCT(std::move(map_struct)));
                }

                duckdb::LogicalType map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR);
                result.SetValue(i, duckdb::Value::MAP(duckdb::ListType::GetChildType(map_type), std::move(items)));
            }
        };

        auto return_type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR);
        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, return_type, duckdb_function));

        return duckdb::CreateScalarFunctionInfo(set);
    }

}
