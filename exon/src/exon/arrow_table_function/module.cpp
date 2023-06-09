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
#include <duckdb/function/table/read_csv.hpp>

#include "exon/arrow_table_function/module.hpp"
#include "rust.hpp"

namespace exon
{
    struct GFFScanFunctionData : public TableFunctionData
    {
        ArrowArrayStream stream;

        unordered_map<idx_t, unique_ptr<ArrowConvertData>> arrow_convert_data;
        idx_t max_threads = 6;

        atomic<idx_t> lines_read;
    };

    struct GFFScanGlobalState : ArrowScanGlobalState
    {
    };

    struct GFFScanLocalState : ArrowScanLocalState
    {
    };

    unique_ptr<LocalTableFunctionState>
    WTArrowTableFunction::ArrowScanInitLocalInternal(ClientContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state_p)
    {

        auto &global_state = global_state_p->Cast<ArrowScanGlobalState>();
        auto current_chunk = make_uniq<ArrowArrayWrapper>();
        auto result = make_uniq<ArrowScanLocalState>(std::move(current_chunk));
        result->column_ids = input.column_ids;
        result->filters = input.filters.get();

        if (input.CanRemoveFilterColumns())
        {
            auto &asgs = global_state_p->Cast<ArrowScanGlobalState>();
            result->all_columns.Initialize(context, asgs.scanned_types);
        }

        if (!ArrowScanParallelStateNext(context, input.bind_data.get(), *result, global_state))
        {
            return nullptr;
        }
        return std::move(result);
    }

    duckdb::unique_ptr<FunctionData> WTArrowTableFunction::FileTypeBind(ClientContext &context, TableFunctionBindInput &input,
                                                                        vector<LogicalType> &return_types, vector<string> &names)
    {
        auto &info = input.info->Cast<WTArrowTableScanInfo>();

        auto file_name = input.inputs[0].GetValue<std::string>();

        struct ArrowArrayStream stream;
        auto vector_size = STANDARD_VECTOR_SIZE;

        ReaderResult read_stream_result;
        auto compression = string("auto_detect");

        for (auto &kv : input.named_parameters)
        {
            if (kv.first == "compression")
            {
                compression = kv.second.GetValue<string>();
            };
        }

        if (compression != "auto_detect")
        {
            read_stream_result = new_reader(&stream, file_name.c_str(), vector_size, compression.c_str(), info.file_type.c_str());
        }
        else
        {
            read_stream_result = new_reader(&stream, file_name.c_str(), vector_size, NULL, info.file_type.c_str());
        }

        if (read_stream_result.error != NULL)
        {
            throw std::runtime_error(read_stream_result.error);
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

        auto result = duckdb::make_uniq<GFFScanFunctionData>();

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
        }

        result->stream = std::move(stream);

        RenameArrowColumns(names);

        return std::move(result);
    }

    unique_ptr<GlobalTableFunctionState> WTArrowTableFunction::InitGlobal(ClientContext &context,
                                                                          TableFunctionInitInput &input)
    {
        auto &data = (GFFScanFunctionData &)*input.bind_data;
        auto global_state = make_uniq<GFFScanGlobalState>();

        global_state->stream = make_uniq<ArrowArrayStreamWrapper>();

        global_state->stream->arrow_array_stream = std::move(data.stream);

        return std::move(global_state);
    }

    void WTArrowTableFunction::Scan(ClientContext &context, TableFunctionInput &input, DataChunk &output)
    {
        if (!input.local_state)
        {
            return;
        }
        auto &data = (GFFScanFunctionData &)*input.bind_data;
        auto &state = (ArrowScanLocalState &)*input.local_state;
        auto &global_state = (GFFScanGlobalState &)*input.global_state;

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

    void WTArrowTableFunction::Register(std::string name, std::string file_type, duckdb::ClientContext &context)
    {
        TableFunction scan;
        scan = TableFunction(name, {LogicalType::VARCHAR}, WTArrowTableFunction::Scan, WTArrowTableFunction::FileTypeBind,
                             WTArrowTableFunction::InitGlobal, ArrowTableFunction::ArrowScanInitLocal);

        auto function_info = make_uniq<WTArrowTableScanInfo>(file_type);
        scan.function_info = std::move(function_info);

        scan.named_parameters["compression"] = LogicalType::VARCHAR;

        scan.cardinality = ArrowTableFunction::ArrowScanCardinality;
        scan.get_batch_index = ArrowTableFunction::ArrowGetBatchIndex;

        scan.projection_pushdown = true;
        scan.filter_pushdown = true;

        auto &catalog = Catalog::GetSystemCatalog(context);

        CreateTableFunctionInfo info(scan);

        catalog.CreateTableFunction(context, &info);
    }

    unique_ptr<TableRef> WTArrowTableFunction::ReplacementScan(ClientContext &context, const string &table_name,
                                                               ReplacementScanData *data)
    {
        auto lower_name = StringUtil::Lower(table_name);
        auto replacement_result = replacement_scan(lower_name.c_str());

        if (replacement_result.file_type == NULL)
        {
            return nullptr;
        }
        auto file_type = string(replacement_result.file_type);

        auto table_function = make_uniq<TableFunctionRef>();
        vector<unique_ptr<ParsedExpression>> children;
        children.push_back(make_uniq<ConstantExpression>(Value(table_name)));

        if (file_type == "FASTA")
        {
            table_function->function = make_uniq<FunctionExpression>("read_fasta", std::move(children));
        }
        else if (file_type == "FASTQ")
        {
            table_function->function = make_uniq<FunctionExpression>("read_fastq", std::move(children));
        }
        else if (file_type == "GFF")
        {
            table_function->function = make_uniq<FunctionExpression>("read_gff", std::move(children));
        }
        else if (file_type == "SAM")
        {
            table_function->function = make_uniq<FunctionExpression>("read_sam_file_records", std::move(children));
        }
        else if (file_type == "BAM")
        {
            table_function->function = make_uniq<FunctionExpression>("read_bam_file_records", std::move(children));
        }
        else if (file_type == "VCF")
        {
            table_function->function = make_uniq<FunctionExpression>("read_vcf_file_records", std::move(children));
        }
        else if (file_type == "BCF")
        {
            table_function->function = make_uniq<FunctionExpression>("read_bcf_file_records", std::move(children));
        }
        else if (file_type == "GENBANK")
        {
            table_function->function = make_uniq<FunctionExpression>("read_genbank", std::move(children));
        }
        else if (file_type == "HMMDOMTAB")
        {
            table_function->function = make_uniq<FunctionExpression>("read_hmm_dom_tbl_out", std::move(children));
        }
        else
        {
            throw std::runtime_error("Unknown file type");
        }

        return std::move(table_function);
    }

}