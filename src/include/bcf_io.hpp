#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

namespace wtt01
{

    class BcfFunctions
    {
    public:
        static duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> GetBcfRecordScanFunction();
    };

} // namespace wtt01
