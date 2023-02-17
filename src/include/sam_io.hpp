#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

namespace wtt01
{

    class SamFunctions
    {
    public:
        static duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GetSamRecordScanFunction();
        static duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GetSamHeaderScanFunction();
        static duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> GetParseCIGARStringFunction();
        static std::vector<duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo>> GetSamFunctions();
    };

} // namespace wtt01
