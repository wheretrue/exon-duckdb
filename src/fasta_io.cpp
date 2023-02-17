
#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include <string>
#include <vector>

#include "fasta_io.hpp"

#include "wtt01_rust.hpp"

namespace wtt01
{

    struct FastaScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct FastaScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
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

        auto reader = fasta_new(result->file_path.c_str(), result->options.compression.c_str());
        result->reader = reader;

        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("id");
        names.push_back("description");
        names.push_back("sequence");

        return move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> FastaInitGlobalState(duckdb::ClientContext &context,
                                                                              duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<FastaScanGlobalState>();
        return move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> FastaInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                            duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const FastaScanBindData *)input.bind_data;
        auto &gstate = (FastaScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<FastaScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return move(local_state);
    }

    void FastaScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const FastaScanBindData *)data.bind_data;
        auto local_state = (FastaScanLocalState *)data.local_state;
        // auto gstate = (FastaScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        Record record;

        while (output.size() < STANDARD_VECTOR_SIZE)
        {
            // Record record = fasta_next(&bind_data->reader);

            fasta_next(&bind_data->reader, &record);

            if (record.done == true)
            {
                local_state->done = true;
                break;
            }

            output.SetValue(0, output.size(), duckdb::Value(record.id));

            if (record.description == NULL || strlen(record.description) == 0)
            {
                output.SetValue(1, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(1, output.size(), duckdb::Value(record.description));
            }

            output.SetValue(2, output.size(), duckdb::Value(record.sequence));
            output.SetCardinality(output.size() + 1);
            fasta_record_free(record);
        }
        // Free up the memory on the record pointers.
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

        return move(result);
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

        return move(global_state);
    }

    static duckdb::unique_ptr<duckdb::LocalFunctionData> FastaWriteInitializeLocal(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data)
    {
        auto local_data = duckdb::make_unique<duckdb::LocalFunctionData>();
        return move(local_data);
    }

    static void FastaWriteSink(duckdb::ExecutionContext &context, duckdb::FunctionData &bind_data_p, duckdb::GlobalFunctionData &gstate,
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

        result->file_name = info.file_path;
        result->reader = fasta_new(info.file_path.c_str(), result->options.compression.c_str());

        return move(result);
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

        if (!(fs.FileExists(table_name)))
        {
            return nullptr;
        };

        std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
        children.push_back(duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value(table_name)));

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_fasta", move(children));

        return table_function;
    }

}
