// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include "duckdb/function/table/arrow.hpp"

using namespace duckdb;

namespace exon
{

    struct VCFQueryTableScanInfo : public TableFunctionInfo
    {
    };

    struct VCFQueryTableFunction : duckdb::ArrowTableFunction
    {
    private:
        static duckdb::unique_ptr<FunctionData> TableBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names);

        static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitGlobal(duckdb::ClientContext &context,
                                                                               duckdb::TableFunctionInitInput &input);

        static void Scan(duckdb::ClientContext &context, duckdb::TableFunctionInput &input, duckdb::DataChunk &output);

        static unique_ptr<LocalTableFunctionState> VCFQueryScanInitLocalInternal(ClientContext &context,
                                                                                 TableFunctionInitInput &input,
                                                                                 GlobalTableFunctionState *global_state);

        static unique_ptr<LocalTableFunctionState> VCFQueryScanInitLocal(ExecutionContext &context,
                                                                         TableFunctionInitInput &input,
                                                                         GlobalTableFunctionState *global_state);

    public:
        static void Register(duckdb::ClientContext &context);
    };
}
