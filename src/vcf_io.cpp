#include <math.h>
#include <map>

#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "htslib/vcf.h"
#include "htslib/hfile.h"
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
        std::vector<uint32_t> format_field_types;
        std::vector<uint32_t> format_field_lengths;

        std::vector<std::string> genotype_int_tags;
        std::vector<std::string> genotype_float_tags;
        std::vector<std::string> genotype_string_tags;
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
        return_types.push_back(duckdb::LogicalType::STRUCT(move(info_children)));

        duckdb::child_list_t<duckdb::LogicalType> format_children;
        std::vector<std::string> tags;

        std::vector<std::string> genotype_int_tags;
        std::vector<std::string> genotype_float_tags;
        std::vector<std::string> genotype_string_tags;

        std::vector<uint32_t> format_field_types;
        std::vector<uint32_t> format_field_lengths;

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

            format_field_types.push_back(bcf_type);
            format_field_lengths.push_back(number);

            if (bcf_type == BCF_HT_FLAG)
            {
                format_children.push_back(std::make_pair(key_str, duckdb::LogicalType::BOOLEAN));
            }
            else if (bcf_type == BCF_HT_INT)
            {
                genotype_int_tags.push_back(key_str);
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
                genotype_float_tags.push_back(key_str);
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
                genotype_string_tags.push_back(key_str);
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

        names.push_back("genotypes");
        return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::STRUCT(format_children)));

        result->tags = tags;
        result->format_field_types = format_field_types;
        result->format_field_lengths = format_field_lengths;

        result->genotype_int_tags = genotype_int_tags;
        result->genotype_float_tags = genotype_float_tags;
        result->genotype_string_tags = genotype_string_tags;

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

    struct Int32Array
    {
        int32_t *arr;
        int32_t number;
        int32_t get_op_response;
        std::string tag;
    };

    struct FloatArray
    {
        float *arr;
        int32_t number;
        int32_t get_op_response;
        std::string tag;
    };

    struct StringArray
    {
        char **arr;
        int32_t number;
        int32_t get_op_response;
        std::string tag;
    };

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
        auto info_field_ids = bind_data->info_field_ids;
        auto info_field_names = bind_data->info_field_names;

        auto info_field_types = bind_data->info_field_types;
        auto info_field_lengths = bind_data->info_field_lengths;

        auto genotype_tags = bind_data->tags;
        auto genotype_field_types = bind_data->format_field_types;
        auto genotype_field_lengths = bind_data->format_field_lengths;

        bcf1_t *record = bcf_init();

        while (output.size() < STANDARD_VECTOR_SIZE)
        {
            auto bcf_read_op = bcf_read(fp, header, record);
            if (bcf_read_op < 0)
            {
                local_state->done = true;

                bcf_hdr_destroy(bind_data->header);
                bcf_destroy(record);

                auto closed = hts_close(bind_data->vcf_file);
                if (closed < 0)
                {
                    throw std::runtime_error("Could not close file");
                }
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

            auto split_id = duckdb::StringUtil::Split(id, '\0');
            for (auto &s : split_id)
            {
                if (s != "." && s != "")
                {
                    id_vec.push_back(duckdb::Value(s));
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
                }
            }

            output.SetValue(7, output.size(), duckdb::Value::STRUCT(struct_values));

            // Create the array for the genotypes...
            int32_t *gt_arr = NULL, ngt_arr = 0;
            auto n_gt = bcf_get_genotypes(header, record, &gt_arr, &ngt_arr);

            std::vector<std::string> genotype_int_tags = bind_data->genotype_int_tags;

            std::unordered_map<std::string, Int32Array> int_maps;
            for (auto tag : genotype_int_tags)
            {
                int32_t *arr = NULL;
                int32_t narr = 0;

                auto n = bcf_get_format_int32(header, record, tag.c_str(), &arr, &narr);

                int_maps[tag] = Int32Array{arr, narr, n, tag};
            }

            std::unordered_map<std::string, FloatArray> float_maps;
            for (auto tag : bind_data->genotype_float_tags)
            {
                float *arr = NULL;
                int32_t narr = 0;

                auto n = bcf_get_format_float(header, record, tag.c_str(), &arr, &narr);

                float_maps[tag] = FloatArray{arr, narr, n, tag};
            }

            std::unordered_map<std::string, StringArray> string_maps;
            for (auto tag : bind_data->genotype_string_tags)
            {
                char **arr = NULL;
                int32_t narr = 0;

                auto n = bcf_get_format_string(header, record, tag.c_str(), &arr, &narr);

                string_maps[tag] = StringArray{arr, narr, n, tag};

                // free(arr);
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
                            struct_values.push_back(std::make_pair("GT", duckdb::Value::INTEGER(gt_value)));
                        }
                        continue;
                    };

                    if (int_maps.count(tag))
                    {
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

                    if (float_maps.count(tag))
                    {
                        auto &arr = float_maps[tag];

                        if (arr.get_op_response > 0)
                        {
                            if (arr.number == bind_data->n_sample)
                            {
                                if (arr.arr[i] > bcf_float_missing)
                                {
                                    struct_values.push_back(std::make_pair(tag, duckdb::Value::FLOAT(arr.arr[i])));
                                }
                                else
                                {
                                    struct_values.push_back(std::make_pair(tag, duckdb::Value::FLOAT(-1)));
                                }
                            }
                            else
                            {
                                auto per_sample = arr.number / bind_data->n_sample;
                                std::vector<duckdb::Value> field_values;
                                for (int j = 0; j < per_sample; j++)
                                {
                                    auto arr_value = arr.arr[i * per_sample + j];

                                    if (arr_value <= bcf_float_missing)
                                    {
                                        field_values.push_back(duckdb::Value());
                                    }
                                    else
                                    {
                                        field_values.push_back(duckdb::Value::FLOAT(arr_value));
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

                    if (string_maps.count(tag))
                    {
                        auto &arr = string_maps[tag];

                        if (arr.get_op_response > 0)
                        {
                            if (arr.number == bind_data->n_sample)
                            {
                                if (arr.arr[i] != NULL)
                                {
                                    struct_values.push_back(std::make_pair(tag, duckdb::Value(arr.arr[i])));
                                }
                                else
                                {
                                    struct_values.push_back(std::make_pair(tag, duckdb::Value()));
                                }
                            }
                            else
                            {
                                auto per_sample = arr.number / bind_data->n_sample;
                                std::vector<duckdb::Value> field_values;

                                for (int j = 0; j < per_sample; j++)
                                {
                                    auto arr_value = arr.arr[i * per_sample + j];

                                    if (arr_value != NULL)
                                    {
                                        std::string arr_string(arr_value);
                                        if (arr_string == ".")
                                        {
                                            continue;
                                        }
                                        field_values.push_back(duckdb::Value(arr_string));
                                    }
                                }

                                if (field_values.size() > 0)
                                {
                                    struct_values.push_back(std::make_pair(tag, duckdb::Value::LIST(field_values)));
                                }
                                else
                                {
                                    struct_values.push_back(std::make_pair(tag, duckdb::Value()));
                                }
                            }
                        }
                        else
                        {
                            struct_values.push_back(std::make_pair(tag, duckdb::Value()));
                        }
                    }
                }

                genotype_structs.push_back(duckdb::Value::STRUCT(struct_values));
            }

            output.SetValue(8, output.size(), duckdb::Value::LIST(genotype_structs));

            output.SetCardinality(output.size() + 1);

            bcf_empty(record);
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
