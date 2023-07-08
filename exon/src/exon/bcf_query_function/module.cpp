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

#include <iostream>
#include <cmath>

#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "exon/arrow_table_function/module.hpp"
#include "exon/bcf_query_function/module.hpp"
#include "rust.hpp"

namespace exon
{
    struct BCFQueryScanFunctionData : public TableFunctionData
    {
        string file_name;
        string query;

        unordered_map<idx_t, unique_ptr<ArrowConvertData>> arrow_convert_data;
        idx_t max_threads = 6;

        vector<string> all_names;

        atomic<idx_t> lines_read;
    };

    duckdb::unique_ptr<FunctionData> BCFQueryTableFunction::TableBind(ClientContext &context,
                                                                      TableFunctionBindInput &input,
                                                                      vector<LogicalType> &return_types,
                                                                      vector<string> &names)
    {
        auto result = make_uniq<BCFQueryScanFunctionData>();

        auto file_name = input.inputs[0].GetValue<std::string>();
        auto query = input.inputs[1].GetValue<std::string>();

        struct ArrowArrayStream stream;
        auto vector_size = STANDARD_VECTOR_SIZE;

        // BCFQueryReaderResult bcf_query_reader_result
        auto bcf_query_reader_result = bcf_query_reader(&stream, file_name.c_str(), query.c_str(), vector_size);

        if (bcf_query_reader_result.error != NULL)
        {
            throw std::runtime_error(bcf_query_reader_result.error);
        }

        struct ArrowSchema arrow_schema;

        if (stream.get_schema(&stream, &arrow_schema) != 0)
        {
            if (stream.release)
            {
                stream.release(&stream);
            }
            throw std::runtime_error("Failed to get schema");
        }

        result->all_names.reserve(arrow_schema.n_children);

        auto n_children = arrow_schema.n_children;
        for (idx_t col_idx = 0; col_idx < n_children; col_idx++)
        {
            auto &schema = *arrow_schema.children[col_idx];

            if (!schema.release)
            {
                throw InvalidInputException("arrow_scan: released schema passed");
            }

            // TODO: handle dictionary
            return_types.emplace_back(GetArrowLogicalType(schema, result->arrow_convert_data, col_idx));

            auto format = string(schema.format);
            auto name = string(schema.name);
            if (name.empty())
            {
                name = string("v") + to_string(col_idx);
            }
            names.push_back(name);

            result->all_names.push_back(name);
        }

        RenameArrowColumns(names);

        result->file_name = file_name;
        result->query = query;

        return std::move(result);
    };

    unique_ptr<GlobalTableFunctionState> BCFQueryTableFunction::InitGlobal(ClientContext &context,
                                                                           TableFunctionInitInput &input)
    {
        auto &data = (BCFQueryScanFunctionData &)*input.bind_data;

        auto global_state = make_uniq<ArrowScanGlobalState>();

        struct ArrowArrayStream stream;

        auto file_name = data.file_name;
        auto query = data.query;
        auto vector_size = STANDARD_VECTOR_SIZE;

        auto bcf_query_reader_result = bcf_query_reader(&stream, file_name.c_str(), query.c_str(), vector_size);
        if (bcf_query_reader_result.error != NULL)
        {
            throw std::runtime_error(bcf_query_reader_result.error);
        }

        global_state->stream = make_uniq<ArrowArrayStreamWrapper>();
        global_state->stream->arrow_array_stream = std::move(stream);

        return std::move(global_state);
    }

    void BCFQueryTableFunction::Scan(ClientContext &context, TableFunctionInput &input, DataChunk &output)
    {
        if (!input.local_state)
        {
            return;
        }
        auto &data = (BCFQueryScanFunctionData &)*input.bind_data;
        auto &state = (ArrowScanLocalState &)*input.local_state;
        auto &global_state = (ArrowScanGlobalState &)*input.global_state;

        //! Out of tuples in this chunk
        if (state.chunk_offset >= (idx_t)state.chunk->arrow_array.length)
        {
            if (!ArrowScanParallelStateNext(context, input.bind_data.get(), state, global_state))
            {
                return;
            }
        }
        auto output_size = MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->arrow_array.length - state.chunk_offset);
        data.lines_read += output_size;

        if (global_state.CanRemoveFilterColumns())
        {
            state.all_columns.Reset();
            state.all_columns.SetCardinality(output_size);
            ArrowToDuckDB(state, data.arrow_convert_data, state.all_columns, data.lines_read - output_size, false);
            output.ReferenceColumns(state.all_columns, global_state.projection_ids);
        }
        else
        {
            output.SetCardinality(output_size);

            ArrowToDuckDB(state, data.arrow_convert_data, output, data.lines_read - output_size, false);
        }

        output.Verify();
        state.chunk_offset += output.size();
    }

    void BCFQueryTableFunction::Register(duckdb::ClientContext &context)
    {

        TableFunction scan;
        scan = TableFunction("bcf_query", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                             BCFQueryTableFunction::Scan,
                             BCFQueryTableFunction::TableBind,
                             BCFQueryTableFunction::InitGlobal,
                             ArrowTableFunction::ArrowScanInitLocal);

        scan.cardinality = ArrowTableFunction::ArrowScanCardinality;
        scan.get_batch_index = ArrowTableFunction::ArrowGetBatchIndex;

        scan.projection_pushdown = true;
        scan.filter_pushdown = true;

        auto &catalog = Catalog::GetSystemCatalog(context);

        CreateTableFunctionInfo info(scan);

        catalog.CreateTableFunction(context, &info);
    };

}
