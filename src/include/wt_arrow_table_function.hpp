#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include "duckdb/function/table/arrow.hpp"

namespace wtt01
{

    using namespace duckdb;

    struct WTArrowTableScanInfo : public TableFunctionInfo
    {
    public:
        WTArrowTableScanInfo(std::string file_type_p) : file_type(file_type_p) {}

        std::string file_type;
    };

    struct WTArrowTableFunction : duckdb::ArrowTableFunction
    {
    private:
        static duckdb::unique_ptr<FunctionData> FileTypeBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types, vector<string> &names);

        static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobal(duckdb::ClientContext &context,
                                                                               duckdb::TableFunctionInitInput &input);

        static void Scan(duckdb::ClientContext &context, duckdb::TableFunctionInput &input, duckdb::DataChunk &output);

        static unique_ptr<LocalTableFunctionState> ArrowScanInitLocalInternal(ClientContext &context,
                                                                              TableFunctionInitInput &input,
                                                                              GlobalTableFunctionState *global_state);

        static unique_ptr<LocalTableFunctionState> ArrowScanInitLocal(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state);

    public:
        static void Register(std::string name, std::string file_type, duckdb::ClientContext &context);
        static unique_ptr<TableRef> ReplacementScan(ClientContext &context, const string &table_name,
                                                    ReplacementScanData *data);
    };
}
