#include <math.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "wtt01_rust.hpp"
#include "vcf_io.hpp"

namespace wtt01
{

    struct VCFRecordScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct VCFRecordScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        VCFRecordScanOptions options;
        VCFReaderC reader;
    };

    struct VCFRecordScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
        VCFReaderC reader;
    };

    struct VCFRecordScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        VCFRecordScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> VCFRecordBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                           std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto filepath = input.inputs[0].GetValue<std::string>();
        auto &fs = duckdb::FileSystem::GetFileSystem(context);

        if (!fs.FileExists(filepath))
        {
            throw duckdb::IOException("File does not exist: " + filepath);
        }

        auto result = duckdb::make_unique<VCFRecordScanBindData>();
        result->file_path = input.inputs[0].GetValue<std::string>();

        for (auto &kv : input.named_parameters)
        {
            if (kv.first == "compression")
            {
                result->options.compression = kv.second.GetValue<std::string>();
            }
            else
            {
                throw std::runtime_error("Unknown named parameter: " + kv.first);
            }
        };

        auto reader = vcf_new(result->file_path.c_str(), result->options.compression.c_str());
        result->reader = reader;

        names.push_back("chromosome");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("ids");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("position");
        return_types.push_back(duckdb::LogicalType::BIGINT);

        names.push_back("reference_bases");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("alternate_bases");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("quality_score");
        return_types.push_back(duckdb::LogicalType::FLOAT);

        names.push_back("filter");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("info");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("genotypes");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        return move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> VCFRecordInitGlobalState(duckdb::ClientContext &context,
                                                                                  duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<VCFRecordScanGlobalState>();
        return move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> VCFRecordInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                                duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const VCFRecordScanBindData *)input.bind_data;
        auto &gstate = (VCFRecordScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<VCFRecordScanLocalState>();

        // should this be init here or use the bind data?
        local_state->reader = bind_data->reader;

        return std::move(local_state);
    }

    void VCFRecordScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const VCFRecordScanBindData *)data.bind_data;
        auto local_state = (VCFRecordScanLocalState *)data.local_state;
        auto gstate = (VCFRecordScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            return;
        }

        vcf_next(&local_state->reader, &output, &local_state->done, STANDARD_VECTOR_SIZE);
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> VCFFunctions::GetVCFRecordScanFunction()
    {
        auto vcf_table_function = duckdb::TableFunction("read_vcf_file_records", {duckdb::LogicalType::VARCHAR}, VCFRecordScan, VCFRecordBind, VCFRecordInitGlobalState, VCFRecordInitLocalState);
        vcf_table_function.named_parameters["compression"] = duckdb::LogicalType::VARCHAR;

        duckdb::CreateTableFunctionInfo vcf_table_function_info(vcf_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(vcf_table_function_info);
    }

    duckdb::unique_ptr<duckdb::TableRef> VCFFunctions::GetVcfReplacementScanFunction(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data)
    {

        auto table_function = duckdb::make_unique<duckdb::TableFunctionRef>();

        auto valid_vcf_filename = duckdb::StringUtil::EndsWith(table_name, ".vcf") || duckdb::StringUtil::EndsWith(table_name, ".vcf.gz") || duckdb::StringUtil::EndsWith(table_name, ".vcf.zst");

        if (!valid_vcf_filename)
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

        table_function->function = duckdb::make_unique<duckdb::FunctionExpression>("read_vcf_file_records", move(children));

        return table_function;
    }

}
