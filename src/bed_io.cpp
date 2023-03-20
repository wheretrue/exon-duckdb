#include <math.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "wtt01_rust.hpp"
#include "bed_io.hpp"

namespace wtt01
{

    struct BEDRecordScanOptions
    {
        uint8_t n_columns = 12;
        std::string compression = "auto_detect";
    };

    struct BEDRecordScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        uint8_t n_columns = 12;

        BEDRecordScanOptions options;
        BEDReaderC reader;
    };

    struct BEDRecordScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        BEDReaderC reader;
    };

    struct BEDRecordScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        BEDRecordScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> BEDRecordBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                           std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto filepath = input.inputs[0].GetValue<std::string>();
        auto &fs = duckdb::FileSystem::GetFileSystem(context);

        if (!fs.FileExists(filepath))
        {
            throw duckdb::IOException("File does not exist: " + filepath);
        }

        auto result = duckdb::make_unique<BEDRecordScanBindData>();
        result->file_path = input.inputs[0].GetValue<std::string>();

        for (auto &kv : input.named_parameters)
        {
            if (kv.first == "compression")
            {
                result->options.compression = kv.second.GetValue<std::string>();
            }
            if (kv.first == "n_columns")
            {
                result->options.n_columns = kv.second.GetValue<int>();

                auto ok_n_columns = {3, 4, 5, 6, 7, 8, 9, 12};
                if (std::find(ok_n_columns.begin(), ok_n_columns.end(), result->options.n_columns) == ok_n_columns.end())
                {
                    throw std::runtime_error("n_columns must be one of 3, 4, 5, 6, 7, 8, 9, 12");
                }
            }
            else
            {
                throw std::runtime_error("Unknown named parameter: " + kv.first);
            }
        };

        names.push_back("reference_sequence_name");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("start");
        return_types.push_back(duckdb::LogicalType::INTEGER);

        names.push_back("end");
        return_types.push_back(duckdb::LogicalType::INTEGER);

        if (result->options.n_columns > 3)
        {
            names.push_back("name");
            return_types.push_back(duckdb::LogicalType::VARCHAR);
        }

        if (result->options.n_columns > 4)
        {
            names.push_back("score");
            return_types.push_back(duckdb::LogicalType::INTEGER);
        }

        if (result->options.n_columns > 5)
        {
            names.push_back("strand");
            return_types.push_back(duckdb::LogicalType::VARCHAR);
        }

        if (result->options.n_columns > 6)
        {
            names.push_back("thick_start");
            return_types.push_back(duckdb::LogicalType::INTEGER);
        }

        if (result->options.n_columns > 7)
        {
            names.push_back("thick_end");
            return_types.push_back(duckdb::LogicalType::INTEGER);
        }

        auto reader = bed_new(result->file_path.c_str(), result->options.n_columns, result->options.compression.c_str());
        result->reader = reader;

        result->n_columns = result->options.n_columns;

        return move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> BEDRecordInitGlobalState(duckdb::ClientContext &context,
                                                                                  duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<BEDRecordScanGlobalState>();
        return move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> BEDRecordInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                                duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const BEDRecordScanBindData *)input.bind_data;
        auto &gstate = (BEDRecordScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<BEDRecordScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return std::move(local_state);
    }

    void BEDRecordScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const BEDRecordScanBindData *)data.bind_data;
        auto local_state = (BEDRecordScanLocalState *)data.local_state;
        auto gstate = (BEDRecordScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        while (output.size() < STANDARD_VECTOR_SIZE)
        {
            BEDRecordC record = bed_next(&bind_data->reader, bind_data->n_columns);

            if (record.reference_sequence_name == NULL)
            {
                local_state->done = true;
                break;
            }

            output.SetValue(0, output.size(), duckdb::Value(record.reference_sequence_name));
            output.SetValue(1, output.size(), duckdb::Value::BIGINT(record.start));
            output.SetValue(2, output.size(), duckdb::Value::BIGINT(record.end));

            if (bind_data->n_columns > 3)
            {
                if (record.name == NULL)
                {
                    output.SetValue(3, output.size(), duckdb::Value());
                }
                else
                {
                    output.SetValue(3, output.size(), duckdb::Value(record.name));
                }
            }

            if (bind_data->n_columns > 4)
            {
                if (record.score < 0)
                {
                    output.SetValue(4, output.size(), duckdb::Value());
                }
                else
                {
                    output.SetValue(4, output.size(), duckdb::Value::BIGINT(record.score));
                }
            }

            if (bind_data->n_columns > 5)
            {
                if (record.strand == NULL)
                {
                    output.SetValue(5, output.size(), duckdb::Value());
                }
                else
                {
                    output.SetValue(5, output.size(), duckdb::Value(record.strand));
                }
            }

            if (bind_data->n_columns > 6)
            {
                if (record.thick_start < 0)
                {
                    output.SetValue(6, output.size(), duckdb::Value());
                }
                else
                {
                    output.SetValue(6, output.size(), duckdb::Value::BIGINT(record.thick_start));
                }
            }

            if (bind_data->n_columns > 7)
            {
                output.SetValue(7, output.size(), duckdb::Value::BIGINT(record.thick_end));
            }

            output.SetCardinality(output.size() + 1);
        }
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> BEDFunctions::GetBEDTableFunction()
    {
        auto vcf_table_function = duckdb::TableFunction("read_bed_file", {duckdb::LogicalType::VARCHAR}, BEDRecordScan, BEDRecordBind, BEDRecordInitGlobalState, BEDRecordInitLocalState);
        vcf_table_function.named_parameters["n_columns"] = duckdb::LogicalType::INTEGER;
        vcf_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo vcf_table_function_info(vcf_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(vcf_table_function_info);
    }

    duckdb::unique_ptr<duckdb::TableRef> BEDFunctions::GetBEDReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data)
    {

        auto table_function = duckdb::make_unique<duckdb::TableFunctionRef>();

        auto valid_bed_filename = duckdb::StringUtil::EndsWith(table_name, ".bed") || duckdb::StringUtil::EndsWith(table_name, ".bed.gz") || duckdb::StringUtil::EndsWith(table_name, ".bed.zst");

        if (!valid_bed_filename)
        {
            return nullptr;
        }

        auto &fs = duckdb::FileSystem::GetFileSystem(context);
        if (!fs.FileExists(table_name))
        {
            return nullptr;
        }

        std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
        children.push_back(duckdb::make_unique<duckdb::ConstantExpression>(duckdb::Value(table_name)));

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_bed_file", move(children));

        return table_function;
    }

}
