#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

namespace wtt01
{

    class GenbankFunctions
    {
    public:
        static duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GetGenbankTableFunction();
        static duckdb::unique_ptr<duckdb::TableRef> GetGenbankReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data);
    };
}
