#include <iostream>

#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "fastq_io.hpp"
#include "wtt01_rust.hpp"

namespace wtt01
{

    struct FastqScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct FastqScanBindData : public duckdb::TableFunctionData
    {
        std::vector<std::string> file_paths;
        int nth_file = 0;

        FastqScanOptions options;
        FASTQReaderC reader;
    };

    struct FastqScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        FASTQReaderC reader;
    };

    struct FastqScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        FastqScanGlobalState() : GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> FastqBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                       std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<FastqScanBindData>();

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        auto glob = input.inputs[0].GetValue<std::string>();

        auto glob_result = fs.Glob(glob);
        if (glob_result.empty())
        {
            throw std::runtime_error("No files found for glob: " + glob);
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
                throw std::runtime_error("Unknown named parameter: " + kv.first);
            }
        };

        auto reader = fastq_new(result->file_paths[0].c_str(), result->options.compression.c_str());
        result->reader = reader;

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("name");
        names.push_back("description");
        names.push_back("sequence");
        names.push_back("quality_scores");

        return move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> FastqInitGlobalState(duckdb::ClientContext &context,
                                                                              duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<FastqScanGlobalState>();
        return move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> FastqInitLocalState(duckdb::ExecutionContext &context,
                                                                            duckdb::TableFunctionInitInput &input,
                                                                            duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const FastqScanBindData *)input.bind_data;
        auto &gstate = (FastqScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<FastqScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return move(local_state);
    }

    void FastqScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (FastqScanBindData *)data.bind_data;
        auto local_state = (FastqScanLocalState *)data.local_state;
        auto gstate = (FastqScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            if (bind_data->nth_file + 1 < bind_data->file_paths.size())
            {
                bind_data->nth_file += 1;
                bind_data->reader = fastq_new(bind_data->file_paths[bind_data->nth_file].c_str(), bind_data->options.compression.c_str());
                local_state->done = false;
            }
            else
            {
                local_state->done = true;
                return;
            }
        }

        fastq_next(&bind_data->reader, &output, &local_state->done, STANDARD_VECTOR_SIZE);
    }

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> FastqFunctions::GetFastqTableFunction()
    {
        auto fastq_table_function = duckdb::TableFunction("read_fastq", {duckdb::LogicalType::VARCHAR}, FastqScan, FastqBind, FastqInitGlobalState, FastqInitLocalState);
        fastq_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo fastq_table_function_info(fastq_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(fastq_table_function_info);
    }

    struct FastqCopyScanOptions
    {
        std::string compression = "auto_detect";
        bool force = false;
    };

    struct FastqCopyWriteBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
        std::string compression = "auto_detect";
        bool force = false;
    };

    struct FastqCopyWriteGlobalState : public duckdb::GlobalFunctionData
    {
        // unique_ptr<FASTAWriterC> writer;
        // FASTAWriterC writer;
        void *writer;
    };

    struct FastqCopyBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
        FASTQReaderC reader;
        FastqCopyScanOptions options;
        // options should go here too
    };

    duckdb::unique_ptr<duckdb::FunctionData> FastqCopyToBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<FastqCopyWriteBindData>();
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
                // Throw a runtime error for a bad compression value
                throw std::runtime_error("Invalid option for FASTQ COPY: " + option.first);
            }
        }

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        auto copy_to_file_exists = fs.FileExists(result->file_name);

        if (copy_to_file_exists)
        {
            if (result->force)
            {
                fs.RemoveFile(result->file_name);
            }
            else
            {
                throw std::runtime_error("File already exists: " + result->file_name + ". Set FORCE to true to overwrite.");
            }
        }

        return move(result);
    }

    static duckdb::unique_ptr<duckdb::GlobalFunctionData> FastqWriteInitializeGlobal(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, const std::string &file_path)
    {
        auto &fastq_write_bind = (FastqCopyWriteBindData &)bind_data;

        auto global_state = duckdb::make_unique<FastqCopyWriteGlobalState>();
        auto new_writer = fastq_writer_new(fastq_write_bind.file_name.c_str(), fastq_write_bind.compression.c_str());
        if (!new_writer)
        {
            throw std::runtime_error("Could not create FASTQ writer");
        }

        global_state->writer = new_writer;

        return move(global_state);
    }

    static duckdb::unique_ptr<duckdb::LocalFunctionData> FastqWriteInitializeLocal(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data)
    {
        auto local_data = duckdb::make_unique<duckdb::LocalFunctionData>();
        return move(local_data);
    }

    static void FastqWriteSink(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data_p, duckdb::GlobalFunctionData &gstate,
                               duckdb::LocalFunctionData &lstate, duckdb::DataChunk &input)
    {
        auto &bind_data = (FastqCopyWriteBindData &)bind_data_p;
        auto &global_state = (FastqCopyWriteGlobalState &)gstate;

        auto &id = input.data[0];
        auto &description = input.data[1];
        auto &seq = input.data[2];
        auto &qual = input.data[3];

        for (duckdb::idx_t i = 0; i < input.size(); i++)
        {
            auto id_str = id.GetValue(i).ToString();
            auto description_str = description.GetValue(i).ToString();
            auto seq_str = seq.GetValue(i).ToString();
            auto qual_str = qual.GetValue(i).ToString();

            auto write_value = fastq_writer_write(global_state.writer, id_str.c_str(), description_str.c_str(), seq_str.c_str(), qual_str.c_str());

            if (write_value != 0)
            {
                throw duckdb::Exception("Error writing to FASTQ file");
            }
        }
    };

    static void FastqWriteCombine(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate, duckdb::LocalFunctionData &lstate)
    {
    }

    void FastqWriteFinalize(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate)
    {
        // TODO: Add other writer destroys in the relevant files
        auto &global_state = (FastqCopyWriteGlobalState &)gstate;

        destroy_writer(global_state.writer);
    };

    static duckdb::unique_ptr<duckdb::FunctionData> FastqCopyBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<FastqCopyBindData>();

        result->file_name = info.file_path;

        for (auto &option : info.options)
        {
            auto loption = duckdb::StringUtil::Lower(option.first);
            auto &value = option.second;

            if (loption == "compression")
            {
                result->options.compression = value[0].GetValue<std::string>();
            }
            else if (loption == "force")
            {
                result->options.force = value[0].GetValue<bool>();
            }
            else
            {
                // Throw a runtime error for a bad compression value
                throw std::runtime_error("Invalid option for FASTQ COPY: " + option.first);
            }
        }

        result->reader = fastq_new(info.file_path.c_str(), result->options.compression.c_str());

        return move(result);
    }

    duckdb::CopyFunction CreateFastqCopyFunction()
    {
        duckdb::CopyFunction function("fastq");

        function.copy_to_bind = FastqCopyToBind;
        function.copy_to_initialize_global = FastqWriteInitializeGlobal;
        function.copy_to_initialize_local = FastqWriteInitializeLocal;

        function.copy_to_sink = FastqWriteSink;
        function.copy_to_combine = FastqWriteCombine;
        function.copy_to_finalize = FastqWriteFinalize;

        function.copy_from_bind = FastqCopyBind;

        auto fastq_scan_function = duckdb::TableFunction("read_fasta", {duckdb::LogicalType::VARCHAR}, FastqScan, FastqBind, FastqInitGlobalState, FastqInitLocalState);
        fastq_scan_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        function.copy_from_function = fastq_scan_function;

        function.extension = "fastq";
        return function;
    };

    duckdb::unique_ptr<duckdb::CreateCopyFunctionInfo> FastqFunctions::GetFastqCopyFunction()
    {
        auto function = CreateFastqCopyFunction();

        duckdb::CreateCopyFunctionInfo info(function);
        return duckdb::make_unique<duckdb::CreateCopyFunctionInfo>(info);
    };

    duckdb::unique_ptr<duckdb::TableRef> FastqFunctions::GetFastqReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data)
    {
        auto table_function = duckdb::make_unique<duckdb::TableFunctionRef>();

        auto valid_fastq_filename = duckdb::StringUtil::EndsWith(table_name, ".fq") || duckdb::StringUtil::EndsWith(table_name, ".fastq");
        valid_fastq_filename = valid_fastq_filename || duckdb::StringUtil::EndsWith(table_name, ".fq.gz") || duckdb::StringUtil::EndsWith(table_name, ".fastq.gz");
        valid_fastq_filename = valid_fastq_filename || duckdb::StringUtil::EndsWith(table_name, ".fq.zst") || duckdb::StringUtil::EndsWith(table_name, ".fastq.zst");

        if (!valid_fastq_filename)
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

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_fastq", move(children));

        return table_function;
    }

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> FastqFunctions::GetQualityScoreStringToList()
    {
        duckdb::ScalarFunctionSet set("quality_score_string_to_list");

        auto duckdb_function = [](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
        {
            result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

            for (duckdb::idx_t i = 0; i < args.size(); i++)
            {
                auto value = args.data[0].GetValue(i);
                auto string_value = duckdb::StringValue::Get(value);

                std::vector<duckdb::Value> quality_scores;

                for (auto c : string_value)
                {
                    quality_scores.push_back(duckdb::Value::INTEGER(c - 33));
                }

                result.SetValue(i, duckdb::Value::LIST(quality_scores));
            }
        };

        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), duckdb_function));
        return duckdb::make_unique<duckdb::CreateScalarFunctionInfo>(set);
    }
}
