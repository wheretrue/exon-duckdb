
#include <duckdb.hpp>
#include <duckdb/function/table/read_csv.hpp>

#include "hmm_io.hpp"

namespace wtt01
{

    struct HMMScanOptions
    {
    };

    struct SchemaKeyValue
    {
        const char *function_name;
        duckdb::LogicalType return_type;
    };

    duckdb::unique_ptr<duckdb::FunctionData> HMMBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                     std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {

        auto result = duckdb::make_unique<duckdb::ReadCSVData>();

        std::vector<std::string> patterns;
        auto file_name = input.inputs[0].GetValue<std::string>();

        result->InitializeFiles(context, patterns);
        result->files.push_back(file_name);

        result->options.delimiter = '\t';
        result->options.auto_detect = false;
        result->options.has_header = true;
        result->options.include_file_name = false;
        result->options.include_parsed_hive_partitions = false;
        result->options.file_path = file_name;
        result->options.null_str = "-";

        result->options.ignore_errors = true;

        auto &options = result->options;

        // A c++ struct literal with name and type fields
        std::vector<SchemaKeyValue> key_values = {
            {"target_name", duckdb::LogicalType::VARCHAR},
            {"target_accession", duckdb::LogicalType::VARCHAR},
            {"tlen", duckdb::LogicalType::BIGINT},
            {"query_name", duckdb::LogicalType::VARCHAR},
            {"accession", duckdb::LogicalType::VARCHAR},
            {"qlen", duckdb::LogicalType::BIGINT},
            {"evalue", duckdb::LogicalType::FLOAT},
            {"sequence_score", duckdb::LogicalType::FLOAT},
            {"bias", duckdb::LogicalType::FLOAT},
            {"domain_number", duckdb::LogicalType::INTEGER},
            {"ndom", duckdb::LogicalType::INTEGER},
            {"conditional_evalue", duckdb::LogicalType::FLOAT},
            {"independent_evalue", duckdb::LogicalType::FLOAT},
            {"domain_score", duckdb::LogicalType::FLOAT},
            {"domain_bias", duckdb::LogicalType::FLOAT},
            {"hmm_from", duckdb::LogicalType::INTEGER},
            {"hmm_to", duckdb::LogicalType::INTEGER},
            {"ali_from", duckdb::LogicalType::INTEGER},
            {"ali_to", duckdb::LogicalType::INTEGER},
            {"env_from", duckdb::LogicalType::INTEGER},
            {"env_to", duckdb::LogicalType::INTEGER},
            {"accuracy", duckdb::LogicalType::FLOAT},
            {"description", duckdb::LogicalType::VARCHAR}};

        for (auto &kv : key_values)
        {
            names.push_back(kv.function_name);
            return_types.push_back(kv.return_type);

            result->sql_types.emplace_back(kv.return_type);
        };

        result->FinalizeRead(context);

        return move(result);
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> HMMFunctions::GetHMMScanFunction()
    {
        duckdb::ReadCSVTableFunction read_csv;
        auto func = read_csv.GetFunction();

        auto hmm_table_function = duckdb::TableFunction("read_hmm_dom_tbl_out",
                                                        {duckdb::LogicalType::VARCHAR},
                                                        func.function,
                                                        HMMBind,
                                                        func.init_global,
                                                        func.init_local);

        duckdb::CreateTableFunctionInfo info(hmm_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(info);
    };
}
