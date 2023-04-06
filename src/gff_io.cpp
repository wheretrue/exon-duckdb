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

    struct GffScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct GffScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        GffScanOptions options;
        GFFReaderC reader;
    };

    struct GffScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        GFFReaderC reader;
    };

    struct GffScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        GffScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> GffBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                     std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<GffScanBindData>();
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

        auto reader = gff_new(result->file_path.c_str(), result->options.compression.c_str());
        result->reader = reader;

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::BIGINT);
        return_types.push_back(duckdb::LogicalType::BIGINT);
        return_types.push_back(duckdb::LogicalType::FLOAT);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        // return_types.push_back(duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR));
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("reference_sequence_name");
        names.push_back("source");
        names.push_back("annotation_type");
        names.push_back("start");
        names.push_back("end");
        names.push_back("score");
        names.push_back("strand");
        names.push_back("phase");
        names.push_back("attributes");

        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> GffInitGlobalState(duckdb::ClientContext &context,
                                                                            duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<GffScanGlobalState>();
        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> GffInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                          duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const GffScanBindData *)input.bind_data;
        auto &gstate = (GffScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<GffScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return std::move(local_state);
    }

    void GffScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const GffScanBindData *)data.bind_data;
        auto local_state = (GffScanLocalState *)data.local_state;
        auto gstate = (GffScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        auto result = gff_insert_record_batch(&bind_data->reader, &output, STANDARD_VECTOR_SIZE);

        if (result.done)
        {
            local_state->done = true;
        }

        if (result.error)
        {
            throw std::runtime_error(result.error);
        }
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GFFunctions::GetGffTableFunction()
    {
        auto gff_table_function = duckdb::TableFunction("read_gff", {duckdb::LogicalType::VARCHAR}, GffScan, GffBind, GffInitGlobalState, GffInitLocalState);
        gff_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo gff_table_function_info(gff_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(gff_table_function_info);
    }

    duckdb::unique_ptr<duckdb::TableRef> GFFunctions::GetGffReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data)
    {
        auto table_function = duckdb::make_unique<duckdb::TableFunctionRef>();

        auto valid_gff_filename = duckdb::StringUtil::EndsWith(table_name, ".gff");
        valid_gff_filename = valid_gff_filename || duckdb::StringUtil::EndsWith(table_name, ".gff.gz");
        valid_gff_filename = valid_gff_filename || duckdb::StringUtil::EndsWith(table_name, ".gff.zst");

        if (!valid_gff_filename)
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

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_gff", std::move(children));

        return table_function;
    }

    struct GffCopyScanOptions
    {
        std::string compression = "auto_detect";
        bool force = false;
    };

    struct GffWriteBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
        std::string compression = "auto_detect";
        bool force = false;
    };

    struct GffWriteGlobalState : public duckdb::GlobalFunctionData
    {
        void *writer;
    };

    struct GffCopyBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
        GFFReaderC reader;
        GffCopyScanOptions options;
    };

    duckdb::unique_ptr<duckdb::FunctionData> GffCopyToBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<GffWriteBindData>();
        result->file_name = info.file_path;

        for (auto &option : info.options)
        {
            auto loption = duckdb::StringUtil::Lower(option.first);
            auto &value = option.second;

            if (loption == "compression")
            {
                result->compression = value[0].GetValue<std::string>();
            }
            else if (loption == "force")
            {
                auto raw_value = value[0].GetValue<std::string>();
                result->force = duckdb::StringUtil::Lower(raw_value) == "true";
            }
            else
            {
                throw duckdb::NotImplementedException("Unrecognized option \"%s\"", option.first.c_str());
            }
        }

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        auto copy_to_file_exists = fs.FileExists(result->file_name);

        if (copy_to_file_exists && !result->force)
        {
            throw std::runtime_error("File already exists: " + result->file_name + ". Use FORCE equal true to overwrite.");
        }

        if (copy_to_file_exists && result->force)
        {
            fs.RemoveFile(result->file_name);
        }

        return std::move(result);
    }

    static duckdb::unique_ptr<duckdb::GlobalFunctionData> GffWriteInitializeGlobal(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, const std::string &file_path)
    {
        auto &gff_write_bind = (GffWriteBindData &)bind_data;

        auto global_state = duckdb::make_unique<GffWriteGlobalState>();
        auto new_writer = gff_writer_new(gff_write_bind.file_name.c_str(), "auto_detect");
        if (new_writer.error)
        {
            throw std::runtime_error("Could not create GFF writer for file " + gff_write_bind.file_name + ": " + new_writer.error);
        }

        global_state->writer = new_writer.writer;

        return std::move(global_state);
    }

    static duckdb::unique_ptr<duckdb::LocalFunctionData> GffWriteInitializeLocal(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data)
    {
        auto local_data = duckdb::make_unique<duckdb::LocalFunctionData>();
        return std::move(local_data);
    }

    static void GffWriteSink(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data_p, duckdb::GlobalFunctionData &gstate,
                             duckdb::LocalFunctionData &lstate, duckdb::DataChunk &input)
    {
        auto &bind_data = (GffWriteBindData &)bind_data_p;
        auto &global_state = (GffWriteGlobalState &)gstate;
        // auto &local_state = (GffWriteLocalState &)lstate;

        auto &reference_sequence_name = input.data[0];
        auto &source = input.data[1];
        auto &feature_type = input.data[2];
        auto &start = input.data[3];
        auto &end = input.data[4];
        auto &score = input.data[5];
        auto &strand = input.data[6];
        auto &phase = input.data[7];

        auto &attributes = input.data[8];

        for (duckdb::idx_t i = 0; i < input.size(); i++)
        {
            auto reference_sequence_name_str = reference_sequence_name.GetValue(i).ToString();
            auto source_str = source.GetValue(i).ToString();
            auto feature_type_str = feature_type.GetValue(i).ToString();

            auto start_value = start.GetValue(i);
            auto start_int = duckdb::IntegerValue::Get(start_value);

            auto end_value = end.GetValue(i);
            auto end_int = duckdb::IntegerValue::Get(end_value);

            auto score_value = score.GetValue(i);
            auto score_string = score_value.ToString();

            auto score_float_value = duckdb::FloatValue::Get(score_value);
            if (score_value.IsNull())
            {
                // set score_float_value equal to nan
                score_float_value = std::numeric_limits<float>::quiet_NaN();
            }

            auto strand_str = strand.GetValue(i).ToString();

            auto phase_value = phase.GetValue(i).ToString();

            auto attributes_str = attributes.GetValue(i).ToString();

            // auto attr_children = duckdb::ListValue::GetChildren(attributes.GetValue(i));
            // auto n_children = attr_children.size();

            // std::string attributes_str = "";
            // for (duckdb::idx_t j = 0; j < n_children; j++)
            // {
            //     auto attr = attr_children[j];
            //     auto attr_kids = duckdb::StructValue::GetChildren(attr);

            //     auto attr_key = attr_kids[0].ToString();
            //     auto attr_value = attr_kids[1].ToString();

            //     attributes_str += attr_key + "=" + attr_value;
            //     if (j < n_children - 1)
            //     {
            //         attributes_str += ";";
            //     }
            // }

            auto write_response = gff_writer_write(global_state.writer, reference_sequence_name_str.c_str(), source_str.c_str(), feature_type_str.c_str(), start_int, end_int, score_float_value, strand_str.c_str(), phase_value.c_str(), attributes_str.c_str());

            if (write_response.result != 0)
            {
                throw duckdb::Exception("Error writing to GFF file");
            }
        }
    };

    static void GffWriteCombine(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate, duckdb::LocalFunctionData &lstate)
    {
    }

    void GffWriteFinalize(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate)
    {
        auto &global_state = (GffWriteGlobalState &)gstate;

        destroy_writer(global_state.writer);
    };

    static duckdb::unique_ptr<duckdb::FunctionData> GffCopyBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<GffCopyBindData>();

        // TODO: Add compression to options
        result->file_name = info.file_path;
        result->reader = gff_new(info.file_path.c_str(), "auto_detect");

        return std::move(result);
    }

    duckdb::CopyFunction CreateGffCopyFunction()
    {

        duckdb::CopyFunction function("gff");

        function.copy_to_bind = GffCopyToBind;
        function.copy_to_initialize_global = GffWriteInitializeGlobal;
        function.copy_to_initialize_local = GffWriteInitializeLocal;

        function.copy_to_sink = GffWriteSink;
        function.copy_to_combine = GffWriteCombine;
        function.copy_to_finalize = GffWriteFinalize;

        function.copy_from_bind = GffCopyBind;

        auto gff_scan_function = duckdb::TableFunction("read_gff", {duckdb::LogicalType::VARCHAR}, GffScan, GffBind, GffInitGlobalState, GffInitLocalState);
        gff_scan_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;
        gff_scan_function.named_parameters["force"] = duckdb::LogicalType::BOOLEAN;

        function.copy_from_function = gff_scan_function;

        function.extension = "gff";
        return function;
    };

    duckdb::unique_ptr<duckdb::CreateCopyFunctionInfo> GFFunctions::GetGffCopyFunction()
    {
        auto function = CreateGffCopyFunction();

        duckdb::CreateCopyFunctionInfo info(function);
        return duckdb::make_unique<duckdb::CreateCopyFunctionInfo>(info);
    };

    struct GFFRawScanOptions
    {
    };

    struct GFFRawSchemaKeyValue
    {
        const char *function_name;
        duckdb::LogicalType return_type;
    };

    duckdb::unique_ptr<duckdb::FunctionData> GFFRawScanBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                            std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<duckdb::ReadCSVData>();

        std::vector<std::string> file_patterns;
        auto file_name = input.inputs[0].GetValue<std::string>();

        result->InitializeFiles(context, file_patterns);
        result->files.push_back(file_name);

        result->options.delimiter = '\t';
        result->options.auto_detect = false;
        result->options.has_header = true;

        result->options.include_file_name = false;
        result->options.include_parsed_hive_partitions = false;
        result->options.file_path = file_name;
        result->options.null_str = ".";

        result->options.ignore_errors = true;

        auto &options = result->options;

        std::vector<GFFRawSchemaKeyValue> key_values = {
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

            result->sql_types.emplace_back(key_value.return_type);
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

        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(info);
    };

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> GFFunctions::GetGFFParseAttributesFunction()
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

                std::vector<duckdb::Value> items;

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

        return duckdb::make_unique<duckdb::CreateScalarFunctionInfo>(set);
    }

}
