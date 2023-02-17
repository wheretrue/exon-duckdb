#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

namespace wtt01
{

    class GFFunctions
    {
    public:
        static std::unique_ptr<duckdb::CreateTableFunctionInfo> GetGFFRawTableFunction();
        static std::unique_ptr<duckdb::CreateTableFunctionInfo> GetGffTableFunction();
        static std::unique_ptr<duckdb::CreateCopyFunctionInfo> GetGffCopyFunction();

        static std::unique_ptr<duckdb::CreateScalarFunctionInfo> GetGFFParseAttributesFunction();

        static std::unique_ptr<duckdb::TableRef> GetGffReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data);
    };
}
