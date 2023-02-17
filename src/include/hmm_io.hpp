#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

namespace wtt01
{

    class HMMFunctions
    {
    public:
        static duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GetHMMScanFunction();
    };

} // namespace wtt01
