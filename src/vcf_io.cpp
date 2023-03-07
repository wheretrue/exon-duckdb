#include <math.h>

#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "htslib/vcf.h"
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

        htsFile *vcf_file;
        bcf_hdr_t *header;
    };

    struct VCFRecordScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
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

        auto reader = hts_open(result->file_path.c_str(), "r");
        auto header = bcf_hdr_read(reader);

        if (header == nullptr)
        {
            throw std::runtime_error("Could not read header");
        }

        result->vcf_file = reader;
        result->header = header;

        names.push_back("chromosome");
        return_types.push_back(duckdb::LogicalType::INTEGER);

        names.push_back("ids");
        return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));

        names.push_back("position");
        return_types.push_back(duckdb::LogicalType::BIGINT);

        names.push_back("reference_bases");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("alternate_bases");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("quality_score");
        return_types.push_back(duckdb::LogicalType::FLOAT);

        names.push_back("filter");
        // maybe should be a varchar?? Can I get that from the filter
        return_types.push_back(duckdb::LogicalType::INTEGER);

        auto ids = header->id[BCF_DT_ID];
        auto n = header->n[BCF_DT_ID];

        duckdb::child_list_t<duckdb::LogicalType> info_children;
        for (int i = 0; i < n; i++)
        {
            auto bb = bcf_hdr_idinfo_exists(header, BCF_HL_INFO, i);
            if (!bb)
            {
                continue;
            }

            auto key = ids[i].key;
            auto key_str = std::string(key);

            auto length = bcf_hdr_id2length(header, BCF_HL_INFO, i);
            auto number = bcf_hdr_id2number(header, BCF_HL_INFO, i);
            auto bcf_type = bcf_hdr_id2type(header, BCF_HL_INFO, i);
            auto coltype = bcf_hdr_id2coltype(header, BCF_HL_INFO, i);

            if (bcf_type == BCF_HT_FLAG)
            {
                info_children.push_back(std::make_pair(key_str, duckdb::LogicalType::BOOLEAN));
            }
            else if (bcf_type == BCF_HT_INT)
            {
                if (number == BCF_VL_VAR)
                {
                    info_children.push_back(std::make_pair(key_str, duckdb::LogicalType::INTEGER));
                }
                else
                {
                    info_children.push_back(std::make_pair(key_str, duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER)));
                }
            }
            else if (bcf_type == BCF_HT_REAL)
            {
                if (number == BCF_VL_VAR)
                {
                    info_children.push_back(std::make_pair(key_str, duckdb::LogicalType::FLOAT));
                }
                else
                {
                    info_children.push_back(std::make_pair(key_str, duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT)));
                }
            }
            else if (bcf_type == BCF_HT_STR)
            {
                if (number == BCF_VL_VAR)
                {
                    info_children.push_back(std::make_pair(key_str, duckdb::LogicalType::VARCHAR));
                }
                else
                {
                    info_children.push_back(std::make_pair(key_str, duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)));
                }
            }
            else
            {
                throw std::runtime_error("Unknown type");
            }
        }

        names.push_back("info");
        return_types.push_back(duckdb::LogicalType::STRUCT(move(info_children)));

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

        htsFile *fp = bind_data->vcf_file;
        bcf_hdr_t *header = bind_data->header;

        bcf1_t *record = bcf_init();

        while (output.size() < STANDARD_VECTOR_SIZE)
        {

            auto bcf_read_op = bcf_read(fp, header, record);
            if (bcf_read_op < 0)
            {
                local_state->done = true;
                break;
            }

            auto chromosome = record->rid;
            auto position = record->pos;

            auto ref_all_unpack_op = bcf_unpack(record, BCF_UN_STR);
            if (ref_all_unpack_op < 0)
            {
                throw std::runtime_error("Could not unpack record");
            }

            auto id = record->d.id;
            std::vector<duckdb::Value> id_vec;

            std::istringstream iss(id);
            std::string s;
            while (std::getline(iss, s, '\0'))
            {
                if (s != ".")
                {
                    id_vec.push_back(s);
                }
            }

            auto als = record->d.als;
            auto als_vec = duckdb::StringUtil::Split(als, '\0');

            // Get the filter from the record
            auto filter = record->d.flt;

            output.SetValue(0, output.size(), duckdb::Value::INTEGER(chromosome));
            if (id_vec.size() != 0)
            {
                output.SetValue(1, output.size(), duckdb::Value::LIST(id_vec));
            }

            output.SetValue(2, output.size(), duckdb::Value::BIGINT(position));

            output.SetValue(3, output.size(), duckdb::Value(als_vec[0]));
            output.SetValue(4, output.size(), duckdb::Value(als_vec[1]));

            output.SetValue(5, output.size(), duckdb::Value::FLOAT(record->qual));

            if (filter != nullptr)
            {
                output.SetValue(6, output.size(), duckdb::Value::INTEGER(*filter));
            }

            output.SetCardinality(output.size() + 1);
        }
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
