#include <math.h>
#include <map>

#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "htslib/vcf.h"
#include "vcf_io_types.hpp"

namespace wtt01
{

    struct VCFTypesRecordScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct VCFTypesRecordScanBindData : public duckdb::TableFunctionData
    {
        std::string file_path;
        VCFTypesRecordScanOptions options;

        htsFile *vcf_file;
        bcf_hdr_t *header;

        std::vector<int> info_field_ids;
        std::vector<std::string> info_field_names;
        std::vector<uint32_t> info_field_types;
        std::vector<uint32_t> info_field_lengths;

        // genotype metadata
        int g_i;
        int g_j;
        int n_gt;
        int n_sample;

        std::vector<std::string> tags;
    };

    struct VCFTypesRecordScanLocalState : public duckdb::LocalTableFunctionState
    {
        bool done = false;
    };

    struct VCFTypesRecordScanGlobalState : public duckdb::GlobalTableFunctionState
    {
        VCFTypesRecordScanGlobalState() : duckdb::GlobalTableFunctionState() {}
    };

    duckdb::unique_ptr<duckdb::FunctionData> VCFTypesRecordBind(duckdb::ClientContext &context, duckdb::TableFunctionBindInput &input,
                                                                std::vector<duckdb::LogicalType> &return_types, std::vector<std::string> &names)
    {
        auto filepath = input.inputs[0].GetValue<std::string>();
        auto &fs = duckdb::FileSystem::GetFileSystem(context);

        if (!fs.FileExists(filepath))
        {
            throw duckdb::IOException("File does not exist: " + filepath);
        }

        auto result = duckdb::make_unique<VCFTypesRecordScanBindData>();
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

        int g_i, g_j, n_gt, n_sample = bcf_hdr_nsamples(header);
        result->g_i = g_i;
        result->g_j = g_j;
        result->n_gt = n_gt;
        result->n_sample = n_sample;

        names.push_back("chromosome");
        return_types.push_back(duckdb::LogicalType::VARCHAR);

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
        return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR));

        auto ids = header->id[BCF_DT_ID];
        auto n = header->n[BCF_DT_ID];

        std::vector<int> info_field_ids;
        std::vector<std::string> info_field_names;

        std::vector<uint32_t> info_field_types;
        std::vector<uint32_t> info_field_lengths;

        std::vector<std::string> genotype_int_tags;

        duckdb::child_list_t<duckdb::LogicalType> info_children;
        for (int i = 0; i < n; i++)
        {
            auto bb = bcf_hdr_idinfo_exists(header, BCF_HL_INFO, i);
            if (!bb)
            {
                continue;
            }

            info_field_ids.push_back(i);

            auto key = ids[i].key;
            auto key_str = std::string(key);
            info_field_names.push_back(key_str);

            auto length = bcf_hdr_id2length(header, BCF_HL_INFO, i);
            auto number = bcf_hdr_id2number(header, BCF_HL_INFO, i);
            auto bcf_type = bcf_hdr_id2type(header, BCF_HL_INFO, i);
            auto coltype = bcf_hdr_id2coltype(header, BCF_HL_INFO, i);

            info_field_types.push_back(bcf_type);
            info_field_lengths.push_back(number);

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

        result->info_field_ids = info_field_ids;
        result->info_field_names = info_field_names;
        result->info_field_types = info_field_types;
        result->info_field_lengths = info_field_lengths;

        names.push_back("info");
        return_types.push_back(duckdb::LogicalType::STRUCT(std::move(info_children)));

        duckdb::child_list_t<duckdb::LogicalType> format_children;
        std::vector<std::string> tags;

        for (int i = 0; i < n; i++)
        {
            auto bb = bcf_hdr_idinfo_exists(header, BCF_HL_FMT, i);
            if (!bb)
            {
                continue;
            }

            auto key = ids[i].key;
            auto key_str = std::string(key);

            tags.push_back(key_str);

            auto length = bcf_hdr_id2length(header, BCF_HL_FMT, i);
            auto number = bcf_hdr_id2number(header, BCF_HL_FMT, i);
            auto bcf_type = bcf_hdr_id2type(header, BCF_HL_FMT, i);
            auto coltype = bcf_hdr_id2coltype(header, BCF_HL_FMT, i);

            if (bcf_type == BCF_HT_FLAG)
            {
                format_children.push_back(std::make_pair(key_str, duckdb::LogicalType::BOOLEAN));
            }
            else if (bcf_type == BCF_HT_INT)
            {
                if (number == BCF_VL_VAR)
                {
                    format_children.push_back(std::make_pair(key_str, duckdb::LogicalType::INTEGER));
                }
                else
                {
                    format_children.push_back(std::make_pair(key_str, duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER)));
                }
            }
            else if (bcf_type == BCF_HT_REAL)
            {
                if (number == BCF_VL_VAR)
                {
                    format_children.push_back(std::make_pair(key_str, duckdb::LogicalType::FLOAT));
                }
                else
                {
                    format_children.push_back(std::make_pair(key_str, duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT)));
                }
            }
            else if (bcf_type == BCF_HT_STR)
            {
                if (number == BCF_VL_VAR)
                {
                    format_children.push_back(std::make_pair(key_str, duckdb::LogicalType::VARCHAR));
                }
                else
                {
                    format_children.push_back(std::make_pair(key_str, duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)));
                }
            }
            else
            {
                throw std::runtime_error("Unknown type");
            }
        }

        result->tags = tags;

        names.push_back("genotypes");
        return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::STRUCT(format_children)));

        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::GlobalTableFunctionState> VCFTypesRecordInitGlobalState(duckdb::ClientContext &context,
                                                                                       duckdb::TableFunctionInitInput &input)
    {
        auto result = duckdb::make_unique<VCFTypesRecordScanGlobalState>();
        return std::move(result);
    }

    duckdb::unique_ptr<duckdb::LocalTableFunctionState> VCFTypesRecordInitLocalState(duckdb::ExecutionContext &context, duckdb::TableFunctionInitInput &input,
                                                                                     duckdb::GlobalTableFunctionState *global_state)
    {
        auto bind_data = (const VCFTypesRecordScanBindData *)input.bind_data;
        auto &gstate = (VCFTypesRecordScanGlobalState &)*global_state;

        auto local_state = duckdb::make_unique<VCFTypesRecordScanLocalState>();

        return std::move(local_state);
    }

    struct Int32Array
    {
        int32_t *arr;
        int32_t number;
        int32_t get_op_response;
        std::string tag;
    };

    void VCFTypesRecordScan(duckdb::ClientContext &context, duckdb::TableFunctionInput &data, duckdb::DataChunk &output)
    {
        auto bind_data = (const VCFTypesRecordScanBindData *)data.bind_data;
        auto local_state = (VCFTypesRecordScanLocalState *)data.local_state;
        auto gstate = (VCFTypesRecordScanGlobalState *)data.global_state;

        if (local_state->done)
        {
            // we are done, free the file
            bcf_flush(bind_data->vcf_file);
            bcf_close(bind_data->vcf_file);
            return;
        }

        htsFile *fp = bind_data->vcf_file;
        bcf_hdr_t *header = bind_data->header;
        auto info_field_ids = bind_data->info_field_ids;
        auto info_field_names = bind_data->info_field_names;

        auto info_field_types = bind_data->info_field_types;
        auto info_field_lengths = bind_data->info_field_lengths;

        auto genotype_tags = bind_data->tags;

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
            const char *chr_name = bcf_hdr_id2name(header, record->rid);
            std::string chr_name_str = std::string(chr_name);

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
            // TODO: This is not working
            auto n_filter = record->d.n_flt;

            output.SetValue(0, output.size(), duckdb::Value(chr_name_str));
            if (id_vec.size() != 0)
            {
                output.SetValue(1, output.size(), duckdb::Value::LIST(id_vec));
            }

            output.SetValue(2, output.size(), duckdb::Value::BIGINT(position));

            output.SetValue(3, output.size(), duckdb::Value(als_vec[0]));
            output.SetValue(4, output.size(), duckdb::Value(als_vec[1]));

            output.SetValue(5, output.size(), duckdb::Value::FLOAT(record->qual));

            // Set the filter values if possible
            if (n_filter > 0)
            {
                std::vector<duckdb::Value> filter_vec;
                for (int i = 0; i < record->d.n_flt; i++)
                {
                    const char *filter_name = bcf_hdr_int2id(header, BCF_DT_ID, record->d.flt[i]);
                    filter_vec.push_back(duckdb::Value(std::string(filter_name)));
                }
                output.SetValue(6, output.size(), duckdb::Value::LIST(filter_vec));
            }
            else
            {
                output.SetValue(6, output.size(), duckdb::Value());
            }

            duckdb::child_list_t<duckdb::Value> struct_values;

            // bcf_info_t
            for (int i = 0; i < info_field_ids.size(); i++)
            {
                auto name = info_field_names[i];
                auto field_index = info_field_ids[i];
                auto field_type = info_field_types[i];
                auto field_len = info_field_lengths[i];

                if (field_type == BCF_HT_INT)
                {
                    int count = 0;
                    int32_t *field_value = nullptr;

                    auto get_info_op = bcf_get_info_int32(header, record, name.c_str(), &field_value, &count);
                    if (get_info_op == -3)
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value()));
                        continue;
                    }
                    if (get_info_op < 0)
                    {
                        throw std::runtime_error("Could not get int info field " + name);
                    }

                    if (field_len == BCF_VL_VAR)
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value::INTEGER(*field_value)));
                    }
                    else
                    {
                        std::vector<duckdb::Value> field_values;
                        for (int i = 0; i < count; i++)
                        {
                            field_values.push_back(duckdb::Value(field_value[i]));
                        }
                        struct_values.push_back(std::make_pair(name, duckdb::Value::LIST(field_values)));
                    }

                    if (field_value != nullptr)
                    {
                        free(field_value);
                    }
                }
                else if (field_type == BCF_HT_FLAG)
                {
                    int count = 0;
                    bool *field_value = nullptr;

                    auto get_info_op = bcf_get_info_flag(header, record, name.c_str(), &field_value, &count);
                    if (get_info_op == -3)
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value()));
                        continue;
                    }
                    if (get_info_op < 0)
                    {
                        throw std::runtime_error("Could not get flag info field " + name);
                    }

                    if (get_info_op == 1)
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value::BOOLEAN(true)));
                    }
                    else
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value::BOOLEAN(false)));
                    }

                    if (field_value != nullptr)
                    {
                        free(field_value);
                    }
                }
                else if (field_type == BCF_HT_STR)
                {
                    int count = 0;
                    char *info_value = NULL;

                    int ret = bcf_get_info_string(header, record, name.c_str(), &info_value, &count);
                    if (ret == -3) // field not present
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value()));
                        continue;
                    }

                    if (ret < 0)
                    {
                        throw std::runtime_error("Could not get str info field " + name);
                    }

                    if (field_len == BCF_VL_VAR)
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value(info_value)));
                    }
                    else
                    {
                        std::vector<duckdb::Value> field_values;
                        for (int i = 0; i < count; i++)
                        {
                            field_values.push_back(duckdb::Value(info_value[i]));
                        }
                        struct_values.push_back(std::make_pair(name, duckdb::Value::LIST(field_values)));
                    }

                    if (info_value != NULL)
                    {
                        free(info_value);
                    }
                }
                else if (field_type == BCF_HT_REAL)
                {
                    int count = 0;
                    float *field_value = nullptr;

                    auto get_info_op = bcf_get_info_float(header, record, name.c_str(), &field_value, &count);
                    if (get_info_op == -3)
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value()));
                        continue;
                    }
                    if (get_info_op < 0)
                    {
                        throw std::runtime_error("Could not get real info field " + name);
                    }

                    if (field_len == BCF_VL_VAR)
                    {
                        struct_values.push_back(std::make_pair(name, duckdb::Value::FLOAT(*field_value)));
                    }
                    else
                    {
                        std::vector<duckdb::Value> field_values;
                        for (int i = 0; i < count; i++)
                        {
                            field_values.push_back(duckdb::Value::FLOAT(field_value[i]));
                        }
                        struct_values.push_back(std::make_pair(name, duckdb::Value::LIST(field_values)));
                    }

                    if (field_value != nullptr)
                    {
                        free(field_value);
                    }
                }
            }

            output.SetValue(7, output.size(), duckdb::Value::STRUCT(struct_values));

            std::unordered_map<std::string, Int32Array> int_maps;

            // Create the array for the genotypes...
            int32_t *gt_arr = NULL, ngt_arr = 0;
            auto n_gt = bcf_get_genotypes(header, record, &gt_arr, &ngt_arr);

            std::vector<std::string> tags = {"GQ", "DP", "HQ"};
            for (auto tag : tags)
            {
                int32_t *arr = NULL;
                int32_t narr = 0;

                auto n = bcf_get_format_int32(header, record, tag.c_str(), &arr, &narr);

                int_maps[tag] = Int32Array{arr, narr, n, tag};
            }

            std::vector<duckdb::Value> genotype_structs;

            for (int i = 0; i < bind_data->n_sample; i++)
            {
                duckdb::child_list_t<duckdb::Value> struct_values;

                for (auto tag : genotype_tags)
                {

                    if (tag == "GT")
                    {
                        auto gt_value = gt_arr[i];
                        if (gt_value <= bcf_int32_missing)
                        {
                            struct_values.push_back(std::make_pair("GT", duckdb::Value()));
                        }
                        else
                        {
                            // printf("gt value: %d %d %d\n", gt_value, bcf_int32_missing,
                            struct_values.push_back(std::make_pair("GT", duckdb::Value::INTEGER(gt_value)));
                        }
                    };

                    // TODO: other types
                    if (int_maps.count(tag) == 0)
                    {
                        continue;
                    }

                    auto &arr = int_maps[tag];

                    if (arr.get_op_response > 0)
                    {
                        if (arr.number == bind_data->n_sample)
                        {
                            if (arr.arr[i] > bcf_int32_missing)
                            {
                                struct_values.push_back(std::make_pair(tag, duckdb::Value::INTEGER(arr.arr[i])));
                            }
                            else
                            {
                                struct_values.push_back(std::make_pair(tag, duckdb::Value::INTEGER(-1)));
                            }
                        }
                        else
                        {
                            auto per_sample = arr.number / bind_data->n_sample;
                            std::vector<duckdb::Value> field_values;
                            for (int j = 0; j < per_sample; j++)
                            {
                                auto arr_value = arr.arr[i * per_sample + j];

                                if (arr_value <= bcf_int32_missing)
                                {
                                    field_values.push_back(duckdb::Value());
                                }
                                else
                                {
                                    field_values.push_back(duckdb::Value::INTEGER(arr_value));
                                }
                            }
                            struct_values.push_back(std::make_pair(tag, duckdb::Value::LIST(field_values)));
                        }
                    }
                    else
                    {
                        struct_values.push_back(std::make_pair(tag, duckdb::Value()));
                    }
                }

                genotype_structs.push_back(duckdb::Value::STRUCT(struct_values));
            }

            output.SetValue(8, output.size(), duckdb::Value::LIST(genotype_structs));

            if (gt_arr != NULL)
            {
                free(gt_arr);
            }

            output.SetCardinality(output.size() + 1);
        }
    };

    duckdb::unique_ptr<duckdb::CreateTableFunctionInfo> VCFTypeFunctions::GetVCFTypesRecordScanFunction()
    {
        auto vcf_table_function = duckdb::TableFunction("read_vcf_file_records_types", {duckdb::LogicalType::VARCHAR}, VCFTypesRecordScan, VCFTypesRecordBind, VCFTypesRecordInitGlobalState, VCFTypesRecordInitLocalState);

        duckdb::CreateTableFunctionInfo vcf_table_function_info(vcf_table_function);
        return duckdb::make_unique<duckdb::CreateTableFunctionInfo>(vcf_table_function_info);
    }

}
