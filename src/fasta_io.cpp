#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "string"
#include <vector>

#include "fasta_io.hpp"

#include "wtt01_rust.hpp"

#include "spdlog/spdlog.h"

namespace wtt01
{

    struct FastaScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct FastaScanBindData : public duckdb::TableFunctionData
    {
        std::vector<std::string> file_paths;
        int nth_file = 0;

        FastaScanOptions options;
        FASTAReaderC reader;
    };

    struct FastaScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        FASTAReaderC reader;
    };

    struct FastaScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        FastaScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> FastaBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                       std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto result = duckdb::make_unique<FastaScanBindData>();

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        auto glob = input.inputs[0].GetValue<std::string>();

        std::vector<std::string> glob_result = fs.Glob(glob);
        if (glob_result.size() == 0)
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
                throw std::runtime_error("Unknown named parameter: " + kv.first);
            }
        };

        auto reader = fasta_new(result->file_paths[0].c_str(), result->options.compression.c_str());
        result->reader = reader;

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("id");
        names.push_back("description");
        names.push_back("sequence");

        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> FastaInitGlobalState(duckdb::ClientContext &context,
                                                                              duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<FastaScanGlobalState>();
        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> FastaInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                            duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const FastaScanBindData *)input.bind_data;
        auto &gstate = (FastaScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<FastaScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return std::move(local_state);
    }

    void FastaScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (FastaScanBindData *)data.bind_data;
        auto local_state = (FastaScanLocalState *)data.local_state;

        if (local_state->done)
        {
            if (bind_data->nth_file + 1 < bind_data->file_paths.size())
            {
                bind_data->nth_file += 1;
                bind_data->reader = fasta_new(bind_data->file_paths[bind_data->nth_file].c_str(), bind_data->options.compression.c_str());
            }
            else
            {
                local_state->done = true;
                return;
            }
        }

        // set an error char pointer
        fasta_next(&bind_data->reader, &output, &local_state->done, STANDARD_VECTOR_SIZE);
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> FastaIO::GetFastaTableFunction()
    {
        auto fasta_table_function = duckdb::TableFunction("read_fasta", {duckdb::LogicalType::VARCHAR}, FastaScan, FastaBind, FastaInitGlobalState, FastaInitLocalState);
        fasta_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo fasta_table_function_info(fasta_table_function);

        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(fasta_table_function_info);
    }

    struct FastaCopyScanOptions
    {
        std::string compression = "auto_detect";
        bool force = false;
    };

    struct FastaWriteBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
        std::string compression = "auto_detect";
        bool force = false;
    };

    struct FastaWriteGlobalState : public duckdb::GlobalFunctionData
    {
        void *writer;
    };

    struct FastaCopyBindData : public duckdb::TableFunctionData
    {
        std::string file_name;
        FASTAReaderC reader;
        FastaCopyScanOptions options;
        // options should go here too
    };

    duckdb::unique_ptr<duckdb::FunctionData> FastaCopyToBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<FastaWriteBindData>();
        result->file_name = info.file_path;

        SPDLOG_INFO("FastaCopyToBind: file_name: {}", result->file_name);

        // for each option print the value to the console
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

    static duckdb::unique_ptr<duckdb::GlobalFunctionData> FastaWriteInitializeGlobal(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, const std::string &file_path)
    {
        auto &fasta_write_bind = (FastaWriteBindData &)bind_data;

        auto global_state = duckdb::make_unique<FastaWriteGlobalState>();
        auto new_writer = fasta_writer_new(fasta_write_bind.file_name.c_str(), fasta_write_bind.compression.c_str());
        if (new_writer.error != NULL)
        {
            throw std::runtime_error("Could not open file: " + fasta_write_bind.file_name + " with error: " + new_writer.error);
        }

        global_state->writer = new_writer.writer;

        return std::move(global_state);
    }

    static duckdb::unique_ptr<duckdb::LocalFunctionData> FastaWriteInitializeLocal(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data)
    {
        auto local_data = duckdb::make_unique<duckdb::LocalFunctionData>();
        return std::move(local_data);
    }

    static void FastaWriteSink3Columns(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data_p, duckdb::GlobalFunctionData &gstate,
                                       duckdb::LocalFunctionData &lstate, duckdb::DataChunk &input)
    {
        auto &bind_data = (FastaWriteBindData &)bind_data_p;
        auto &global_state = (FastaWriteGlobalState &)gstate;

        auto &id = input.data[0];
        auto &description = input.data[1];
        auto &seq = input.data[2];

        for (duckdb::idx_t i = 0; i < input.size(); i++)
        {
            auto id_str = id.GetValue(i).ToString();
            auto seq_str = seq.GetValue(i).ToString();

            auto description_value = description.GetValue(i);

            if (description_value.IsNull())
            {
                auto write_value = fasta_writer_write(global_state.writer, id_str.c_str(), nullptr, seq_str.c_str());
                if (write_value != 0)
                {
                    throw duckdb::Exception("Error writing to FASTA file");
                }
            }
            else
            {
                auto description_str = description_value.ToString();
                auto write_value = fasta_writer_write(global_state.writer, id_str.c_str(), description_str.c_str(), seq_str.c_str());
                if (write_value != 0)
                {
                    throw duckdb::Exception("Error writing to FASTA file");
                }
            }
        }
    }

    static void FastaWriteSink2Columns(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data_p, FastaWriteGlobalState global_state,
                                       duckdb::LocalFunctionData &lstate, duckdb::DataChunk &input)
    {
        SPDLOG_INFO("FastaWriteSink2Columns: input.size: {}", input.size());
        auto &id = input.data[0];
        auto &seq = input.data[1];

        for (duckdb::idx_t i = 0; i < input.size(); i++)
        {
            auto id_str = id.GetValue(i).ToString();
            auto seq_str = seq.GetValue(i).ToString();

            auto write_value = fasta_writer_write(global_state.writer, id_str.c_str(), nullptr, seq_str.c_str());
            if (write_value != 0)
            {
                throw duckdb::Exception("Error writing to FASTA file");
            }
        }
    }

    static void FastaWriteSink(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data_p, duckdb::GlobalFunctionData &gstate,
                               duckdb::LocalFunctionData &lstate, duckdb::DataChunk &input)
    {
        SPDLOG_INFO("FastaWriteSink: input.size: {}", input.size());
        auto &bind_data = (FastaWriteBindData &)bind_data_p;
        auto &global_state = (FastaWriteGlobalState &)gstate;

        if (input.data.size() == 2)
        {
            FastaWriteSink2Columns(context, bind_data_p, global_state, lstate, input);
            return;
        }
        else if (input.data.size() == 3)
        {
            FastaWriteSink3Columns(context, bind_data_p, gstate, lstate, input);
            return;
        }
    };

    static void FastaWriteCombine(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate, duckdb::LocalFunctionData &lstate)
    {
    }

    void FastaWriteFinalize(duckdb::ClientContext &context, duckdb::FunctionData &bind_data, duckdb::GlobalFunctionData &gstate)
    {
        auto &global_state = (FastaWriteGlobalState &)gstate;

        destroy_writer(global_state.writer);
    };

    static duckdb::unique_ptr<duckdb::FunctionData> FastaCopyBind(duckdb::ClientContext &context, duckdb::CopyInfo &info, std::vector<std::string> &names, std::vector<duckdb::LogicalType> &sql_types)
    {
        auto result = duckdb::make_unique<FastaCopyBindData>();

        SPDLOG_INFO("FASTA COPY: {}", info.file_path);

        for (auto &option : info.options)
        {
            auto loption = duckdb::StringUtil::Lower(option.first);
            auto &value = option.second;

            if (loption == "compression")
            {
                result->options.compression = value[0].GetValue<std::string>();
            }
            else
            {
                // Throw a runtime error for a bad compression value
                throw std::runtime_error("Invalid option for FASTA COPY: " + option.first);
            }
        }

        // Check that the input names are correct
        if (names.size() == 3)
        {
            if (names[0] != "id" || names[1] != "description" || names[2] != "sequence")
            {
                throw std::runtime_error("Invalid column names for FASTA COPY. Expected (id, description, sequence)");
            }
        }
        else if (names.size() == 2)
        {
            if (names[0] != "id" || names[1] != "sequence")
            {
                throw std::runtime_error("Invalid column names for FASTA COPY. Expected (id, sequence)");
            }
        }
        else
        {
            throw std::runtime_error("Invalid column names for FASTA COPY. Expected (id, description, sequence) or (id, sequence)");
        }

        // Check the input types are correct, if 2 or 3 length is allowed and all must be varchars
        if (sql_types.size() == 3)
        {
            if (sql_types[0] != duckdb::LogicalType::VARCHAR || sql_types[1] != duckdb::LogicalType::VARCHAR || sql_types[2] != duckdb::LogicalType::VARCHAR)
            {
                throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR, VARCHAR)");
            }
        }
        else if (sql_types.size() == 2)
        {
            if (sql_types[0] != duckdb::LogicalType::VARCHAR || sql_types[1] != duckdb::LogicalType::VARCHAR)
            {
                throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR)");
            }
        }
        else
        {
            throw std::runtime_error("Invalid column types for FASTA COPY. Expected (VARCHAR, VARCHAR, VARCHAR) or (VARCHAR, VARCHAR)");
        }

        result->file_name = info.file_path;
        result->reader = fasta_new(info.file_path.c_str(), result->options.compression.c_str());

        return std::move(result);
    }

    duckdb::CopyFunction CreateFastaCopyFunction()
    {

        duckdb::CopyFunction function("fasta");

        function.copy_to_bind = FastaCopyToBind;
        function.copy_to_initialize_global = FastaWriteInitializeGlobal;
        function.copy_to_initialize_local = FastaWriteInitializeLocal;

        function.copy_to_sink = FastaWriteSink;
        function.copy_to_combine = FastaWriteCombine;
        function.copy_to_finalize = FastaWriteFinalize;

        function.copy_from_bind = FastaCopyBind;

        auto fasta_scan_function = duckdb::TableFunction("read_fasta", {duckdb::LogicalType::VARCHAR}, FastaScan, FastaBind, FastaInitGlobalState, FastaInitLocalState);
        // fasta_table_function.named_parameters["compression"] = LogicalType::VARCHAR;

        function.copy_from_function = fasta_scan_function;

        function.extension = "fasta";
        return function;
    }

    duckdb::unique_ptr<duckdb::CreateCopyFunctionInfo> FastaIO::GetFastaCopyFunction()
    {
        auto function = CreateFastaCopyFunction();
        duckdb::CreateCopyFunctionInfo info(function);

        return duckdb::make_unique<duckdb::CreateCopyFunctionInfo>(info);
    };

    duckdb::unique_ptr<duckdb::TableRef> FastaIO::GetFastaReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data)
    {
        auto table_function = duckdb::make_unique<duckdb::TableFunctionRef>();

        auto valid_fasta_filename = duckdb::StringUtil::EndsWith(table_name, ".fa") || duckdb::StringUtil::EndsWith(table_name, ".fasta");
        valid_fasta_filename = valid_fasta_filename || duckdb::StringUtil::EndsWith(table_name, ".fa.gz") || duckdb::StringUtil::EndsWith(table_name, ".fasta.gz");
        valid_fasta_filename = valid_fasta_filename || duckdb::StringUtil::EndsWith(table_name, ".fa.zst") || duckdb::StringUtil::EndsWith(table_name, ".fasta.zst");

        if (!valid_fasta_filename)
        {
            return nullptr;
        };

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        auto glob = fs.Glob(table_name);
        if (glob.size() == 0)
        {
            return nullptr;
        };

        std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
        children.push_back(duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value(table_name)));

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_fasta", std::move(children));

        return table_function;
    }

}
