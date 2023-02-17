#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

namespace wtt01
{

    class FastaIO
    {

    public:
        static std::unique_ptr<duckdb::CreateTableFunctionInfo> GetFastaTableFunction();
        static std::unique_ptr<duckdb::CreateCopyFunctionInfo> GetFastaCopyFunction();
        static std::unique_ptr<duckdb::TableRef> GetFastaReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data);
    };

} // namespace wtt01
