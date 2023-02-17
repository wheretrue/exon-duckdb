#include <iostream>

#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "wtt01_rust.hpp"
#include "sam_io.hpp"

namespace wtt01
{
    struct SamRecordScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct SamRecordScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        SamRecordScanOptions options;
        SamRecordReaderC reader;
    };

    struct SamRecordScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        SamRecordReaderC reader;
    };

    struct SamRecordScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        SamRecordScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> SamRecordBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                           std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<SamRecordScanBindData>();

        auto filepath = input.inputs[0].GetValue<std::string>();
        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        if (!fs.FileExists(filepath))
        {
            throw duckdb::IOException("File does not exist: " + filepath);
        }

        result->file_path = filepath;

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

        auto reader = sam_record_new_reader(result->file_path.c_str(), result->options.compression.c_str());
        result->reader = reader;

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::BIGINT);
        return_types.push_back(duckdb::LogicalType::BIGINT);
        return_types.push_back(duckdb::LogicalType::BIGINT);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::BIGINT);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::BIGINT);

        names.push_back("sequence");
        names.push_back("read_name");
        names.push_back("flags");
        names.push_back("alignment_start");
        names.push_back("alignment_end");
        names.push_back("cigar_string");
        names.push_back("quality_scores");
        names.push_back("template_length");
        names.push_back("mapping_quality");
        names.push_back("mate_alignment_start");

        return move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SamRecordInitGlobalState(duckdb::ClientContext &context,
                                                                                  duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<SamRecordScanGlobalState>();
        return move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> SamRecordInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                                duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const SamRecordScanBindData *)input.bind_data;
        auto &gstate = (SamRecordScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<SamRecordScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return move(local_state);
    }

    void SamRecordScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const SamRecordScanBindData *)data.bind_data;
        auto local_state = (SamRecordScanLocalState *)data.local_state;
        auto gstate = (SamRecordScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        while (output.size() < STANDARD_VECTOR_SIZE)
        {
            SamRecordC record = sam_record_read_records(&bind_data->reader);

            if (record.sequence == NULL)
            {
                local_state->done = true;
                break;
            }

            output.SetValue(0, output.size(), duckdb::Value(record.sequence));
            output.SetValue(1, output.size(), duckdb::Value(record.read_name));
            output.SetValue(2, output.size(), duckdb::Value(record.flags));
            output.SetValue(3, output.size(), duckdb::Value::BIGINT(record.alignment_start));
            output.SetValue(4, output.size(), duckdb::Value::BIGINT(record.alignment_end));
            output.SetValue(5, output.size(), duckdb::Value(record.cigar_string));
            output.SetValue(6, output.size(), duckdb::Value(record.quality_scores));
            output.SetValue(7, output.size(), duckdb::Value(record.template_length));
            output.SetValue(8, output.size(), duckdb::Value(record.mapping_quality));
            output.SetValue(9, output.size(), duckdb::Value(record.mate_alignment_start));

            output.SetCardinality(output.size() + 1);
        }
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> SamFunctions::GetSamRecordScanFunction()
    {
        auto sam_table_function = duckdb::TableFunction("read_sam_file_records", {duckdb::LogicalType::VARCHAR}, SamRecordScan, SamRecordBind, SamRecordInitGlobalState, SamRecordInitLocalState);
        sam_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo sam_table_function_info(sam_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(sam_table_function_info);
    }

    struct SamHeaderScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct SamHeaderScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        SamHeaderScanOptions options;
        SamHeaderReaderC reader;
    };

    struct SamHeaderScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        SamHeaderReaderC reader;
    };

    struct SamHeaderScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        SamHeaderScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> SamHeaderBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                           std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<SamHeaderScanBindData>();
        result->file_path = input.inputs[0].GetValue<std::string>();

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

        auto reader = sam_header_new_reader(result->file_path.c_str(), result->options.compression.c_str());
        result->reader = reader;

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("record_type");
        names.push_back("tag");
        names.push_back("value");

        return move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SamHeaderInitGlobalState(duckdb::ClientContext &context,
                                                                                  duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<SamHeaderScanGlobalState>();
        return move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> SamHeaderInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                                duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const SamHeaderScanBindData *)input.bind_data;
        auto &gstate = (SamHeaderScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<SamHeaderScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return move(local_state);
    }

    void SamHeaderScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const SamHeaderScanBindData *)data.bind_data;
        auto local_state = (SamHeaderScanLocalState *)data.local_state;
        auto gstate = (SamHeaderScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        while (output.size() < STANDARD_VECTOR_SIZE)
        {
            HeaderRecordC record = sam_header_read_records(&bind_data->reader);

            if (record.record_type == NULL)
            {
                local_state->done = true;
                break;
            }

            output.SetValue(0, output.size(), duckdb::Value(record.record_type));

            if (record.tag == NULL)
            {
                output.SetValue(1, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(1, output.size(), duckdb::Value(record.tag));
            }

            output.SetValue(2, output.size(), duckdb::Value(record.value));
            output.SetCardinality(output.size() + 1);
        }
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> SamFunctions::GetSamHeaderScanFunction()
    {
        auto sam_header_table_function = duckdb::TableFunction("read_sam_file_header", {duckdb::LogicalType::VARCHAR}, SamHeaderScan, SamHeaderBind, SamHeaderInitGlobalState, SamHeaderInitLocalState);
        sam_header_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo sam_header_table_function_info(sam_header_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(sam_header_table_function_info);
    }

    void ParseCIGARString(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

        for (duckdb::idx_t i = 0; i < args.size(); i++)
        {
            auto string_value = args.data[0].GetValue(i);
            auto ss = string_value.ToString();

            CResult cigar = parse_cigar(ss.c_str());
            if (cigar.error)
            {
                throw std::runtime_error("Invalid CIGAR string: " + ss);
            }

            auto ops = duckdb::StringUtil::Split(cigar.value, ';');

            std::vector<duckdb::Value> op_values;

            for (auto op : ops)
            {
                duckdb::child_list_t<duckdb::Value> struct_values;
                auto op_parts = duckdb::StringUtil::Split(op, '=');

                if (op_parts.size() != 2)
                {
                    throw std::runtime_error("Invalid CIGAR string");
                }

                auto op_type = op_parts[0];
                auto op_length = op_parts[1];

                auto op_type_value = duckdb::Value(op_type);
                auto op_length_value = duckdb::Value::INTEGER(std::atoi(op_length.c_str()));

                struct_values.push_back(std::make_pair("op", op_type_value));
                struct_values.push_back(std::make_pair("len", op_length_value));

                op_values.push_back(duckdb::Value::STRUCT(struct_values));
            }

            result.SetValue(i, duckdb::Value::LIST(op_values));
        }
    }

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> SamFunctions::GetParseCIGARStringFunction()
    {
        duckdb::ScalarFunctionSet set("parse_cigar");

        duckdb::child_list_t<duckdb::LogicalType> struct_children;
        struct_children.push_back(std::make_pair("op", duckdb::LogicalType::VARCHAR));
        struct_children.push_back(std::make_pair("len", duckdb::LogicalType::INTEGER));

        auto record_type = duckdb::LogicalType::STRUCT(std::move(struct_children));
        auto row_type = duckdb::LogicalType::LIST(move(record_type));

        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, row_type, ParseCIGARString));

        return duckdb::make_unique<duckdb::CreateScalarFunctionInfo>(set);
    }

    std::vector<duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo>> SamFunctions::GetSamFunctions()
    {

        struct SamFunction
        {
            std::string name;
            std::function<bool(int32_t)> func;
        };

        std::vector<SamFunction> sam_functions = {
            {"is_segmented", is_segmented},
            {"is_unmapped", is_unmapped},
            {"is_properly_aligned", is_properly_aligned},
            {"is_mate_unmapped", is_mate_unmapped},
            {"is_reverse_complemented", is_reverse_complemented},
            {"is_mate_reverse_complemented", is_mate_reverse_complemented},
            {"is_first_segment", is_first_segment},
            {"is_last_segment", is_last_segment},
            {"is_secondary", is_secondary},
            {"is_quality_control_failed", is_quality_control_failed},
            {"is_duplicate", is_duplicate},
            {"is_supplementary", is_supplementary}};

        std::vector<duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo>> sam_scalar_functions;

        for (auto &sam_function : sam_functions)
        {
            duckdb::ScalarFunctionSet set(sam_function.name);

            auto duckdb_function = [sam_function](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
            {
                result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
                for (duckdb::idx_t i = 0; i < args.size(); i++)
                {
                    auto value = args.data[0].GetValue(i);
                    auto int_value = duckdb::IntegerValue::Get(value);

                    auto bool_value = sam_function.func(int_value);

                    result.SetValue(i, duckdb::Value::BOOLEAN(bool_value));
                }
            };

            set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::INTEGER}, duckdb::LogicalType::BOOLEAN, duckdb_function));

            sam_scalar_functions.emplace_back(duckdb::make_unique<duckdb::CreateScalarFunctionInfo>(set));
        }

        return sam_scalar_functions;
    }

}
