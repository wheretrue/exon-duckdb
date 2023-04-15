#include <math.h>
#include <map>

#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "spdlog/spdlog.h"

#include "htslib/hts_log.h"
#include "htslib/vcf.h"

#include "vcf_io_types.hpp"

namespace wtt01
{

    struct VCFTypesRecordScanOptions
    {
        std::string compression = "auto_detect";
    };

    struct GenotypeTag
    {
        std::string tag;
        int type;
        int length;

        // The constructor for genotype tags
        GenotypeTag(std::string tag, int type, int length) : tag(tag), type(type), length(length) {}
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

        std::vector<GenotypeTag> tags;
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

        hts_set_log_level(HTS_LOG_ERROR);

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

        auto reader = hts_open(result->file_path.c_str(), "rb");
        auto header = bcf_hdr_read(reader);

        if (header == nullptr)
        {
            throw std::runtime_error("Could not read header");
        }

        // bcf_flush(reader);
        // bcf_close(reader);

        SPDLOG_INFO("After reader");

        // auto reader2 = hts_open(result->file_path.c_str(), "rb");
        result->vcf_file = reader;
        result->header = header;

        SPDLOG_INFO("Initialized reader");

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

        SPDLOG_INFO("Initialized reader");

        if (header == NULL)
        {
            throw std::runtime_error("Could not read header");
        }

        auto ids = header->id[BCF_DT_ID];
        auto n = header->n[BCF_DT_ID];

        SPDLOG_INFO("Getting ids from header");

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
        return_types.push_back(duckdb::LogicalType::STRUCT(std::move(info_children)));

        duckdb::child_list_t<duckdb::LogicalType> format_children;

        std::vector<GenotypeTag> tags;

        for (int i = 0; i < n; i++)
        {
            auto bb = bcf_hdr_idinfo_exists(header, BCF_HL_FMT, i);
            if (!bb)
            {
                continue;
            }

            auto key = ids[i].key;
            auto key_str = std::string(key);

            auto length = bcf_hdr_id2length(header, BCF_HL_FMT, i);
            auto number = bcf_hdr_id2number(header, BCF_HL_FMT, i);
            auto bcf_type = bcf_hdr_id2type(header, BCF_HL_FMT, i);
            auto coltype = bcf_hdr_id2coltype(header, BCF_HL_FMT, i);

            tags.push_back(GenotypeTag(key_str, bcf_type, length));

            SPDLOG_INFO("creating gt: key: {}, length: {}, number: {}, bcf_type: {}, coltype: {}", key, length, number, bcf_type, coltype);

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

        if (tags.size() > 0)
        {
            names.push_back("genotypes");
            return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::STRUCT(format_children)));
        }

        SPDLOG_INFO("Getting ids from header");
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

    struct FloatArray
    {
        float *arr;
        int32_t number;
        int32_t get_op_response;
        std::string tag;
    };

    struct BoolArray
    {
        bool *arr;
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

        auto n_sample = bind_data->n_sample;

        auto genotype_tags = bind_data->tags;
        auto has_tags = genotype_tags.size() > 0;

        bcf1_t *record = bcf_init();

        SPDLOG_INFO("Reading record");
        while (output.size() < STANDARD_VECTOR_SIZE)
        {
            auto bcf_read_op = bcf_read(fp, header, record);
            if (bcf_read_op < 0)
            {
                local_state->done = true;
                break;
            }
            SPDLOG_INFO("Reading record");

            // bump by one so that we are 1-based same as the rest of the system
            auto position = record->pos + 1;

            auto ref_all_unpack_op = bcf_unpack(record, BCF_UN_SHR);
            if (ref_all_unpack_op < 0)
            {
                throw std::runtime_error("Could not unpack record");
            }
            SPDLOG_INFO("Unpacked record");

            auto chromosome = record->rid;
            SPDLOG_INFO("Chromosome: {}", chromosome);

            const char *chr_name = bcf_hdr_id2name(header, record->rid);
            std::string chr_name_str = std::string(chr_name);

            auto id = record->d.id;
            std::vector<duckdb::Value> id_vec;

            std::istringstream iss(id);
            std::string s;
            while (std::getline(iss, s, ';'))
            {
                if (s != ".")
                {
                    id_vec.push_back(s);
                }
            }

            auto alleles = record->d.allele;
            auto n_allele = record->n_allele;

            output.SetValue(0, output.size(), duckdb::Value(chr_name_str));
            output.SetValue(1, output.size(), duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, id_vec));

            output.SetValue(2, output.size(), duckdb::Value::BIGINT(position));

            output.SetValue(3, output.size(), duckdb::Value(alleles[0]));

            std::vector<duckdb::Value> alt_vec;
            SPDLOG_INFO("Number of alleles: {}", n_allele);

            for (int i = 1; i < n_allele; i++)
            {
                auto a = alleles[i];
                if (a == NULL)
                {
                    break;
                }
                alt_vec.push_back(alleles[i]);
            }

            SPDLOG_INFO("Collected {} alleles", alt_vec.size());

            output.SetValue(3, output.size(), duckdb::Value(alleles[0]));
            output.SetValue(4, output.size(), duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, alt_vec));

            // Check if the qual value is nan, if so set it to null
            if (std::isnan(record->qual))
            {
                output.SetValue(5, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(5, output.size(), duckdb::Value::FLOAT(record->qual));
            }

            SPDLOG_INFO("Number of filters: {}", record->d.n_flt);

            // Set the filter values if possible
            std::vector<duckdb::Value> filter_vec;
            for (int i = 0; i < record->d.n_flt; i++)
            {
                const char *filter_name = bcf_hdr_int2id(header, BCF_DT_ID, record->d.flt[i]);
                auto filter_name_str = std::string(filter_name);
                filter_vec.push_back(duckdb::Value(filter_name_str));
            }
            output.SetValue(6, output.size(), duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, filter_vec));

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
                            field_values.push_back(duckdb::Value::INTEGER(field_value[i]));
                        }
                        struct_values.push_back(std::make_pair(name, duckdb::Value::LIST(duckdb::LogicalType::INTEGER, field_values)));
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
                        struct_values.push_back(std::make_pair(name, duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, field_values)));
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
                        struct_values.push_back(std::make_pair(name, duckdb::Value::LIST(duckdb::LogicalType::FLOAT, field_values)));
                    }

                    if (field_value != nullptr)
                    {
                        free(field_value);
                    }
                }
            }

            output.SetValue(7, output.size(), duckdb::Value::STRUCT(struct_values));

            std::unordered_map<std::string, Int32Array> int_maps;
            std::unordered_map<std::string, FloatArray> float_maps;
            std::unordered_map<std::string, StringArray> string_maps;
            std::unordered_map<std::string, BoolArray> bool_maps;

            // https://github.com/samtools/htslib/blob/07638e1cac22e76c2f59c9988feabd467a15c340/htslib/vcf.h#L1128-L1156
            // Create the array for the genotypes...
            int32_t *gt_arr = NULL, ngt_arr = 0;

            // should I be using n_gt or something else
            auto n_gt = bcf_get_genotypes(header, record, &gt_arr, &ngt_arr);
            if (n_gt < 0)
            {
                output.SetCardinality(output.size() + 1);
                continue;
            }

            SPDLOG_INFO("n_gt: {}, ngt_arr: {}", n_gt, ngt_arr);

            // setting tag arrays.
            for (auto tag_type : genotype_tags)
            {
                SPDLOG_INFO("setting arrays: tag: {}, type: {}", tag_type.tag, tag_type.type);

                auto tag = tag_type.tag;
                if (tag == "GT")
                {
                    continue;
                }

                if (tag_type.type == BCF_HT_INT)
                {
                    int32_t *arr = NULL;
                    int32_t narr = 0;

                    auto n = bcf_get_format_int32(header, record, tag.c_str(), &arr, &narr);
                    if (n < 0)
                    {
                        SPDLOG_INFO("Could not get int32 format field {}", tag);
                    }

                    int_maps[tag] = Int32Array{arr, narr, n, tag};
                }
                else if (tag_type.type == BCF_HT_REAL)
                {
                    float *arr = NULL;
                    int32_t narr = 0;

                    auto n = bcf_get_format_float(header, record, tag.c_str(), &arr, &narr);
                    if (n < 0)
                    {
                        SPDLOG_INFO("Could not get float format field {}", tag);
                    }

                    float_maps[tag] = FloatArray{arr, narr, n, tag};
                }
                else if (tag_type.type == BCF_HT_STR)
                {
                    char **arr = NULL;
                    int32_t narr = 0;

                    auto n = bcf_get_format_string(header, record, tag.c_str(), &arr, &narr);
                    // auto n = bcf_get_format_char(header, record, tag.c_str(), &arr, &narr);

                    SPDLOG_INFO("n: {}, narr: {}, tag: {}", n, narr, tag);
                    if (n < 0)
                    {
                        SPDLOG_INFO("Could not get string format field {}", tag);
                    }

                    // A bit of a hack that will keep the array for iteration the proper size.
                    if (tag_type.length == BCF_VL_VAR)
                    {
                        string_maps[tag] = StringArray{arr, narr, n, tag};
                    }
                    else
                    {
                        string_maps[tag] = StringArray{arr, n_sample, n, tag};
                    }
                }
                else
                {
                    printf("Unsupported type for genotype field %s with type %d\n", tag.c_str(), tag_type.type);
                    throw std::runtime_error("Unsupported type for genotype field " + tag);
                }
            }

            std::vector<duckdb::Value> genotype_structs;

            for (int i = 0; i < bind_data->n_sample; i++)
            {
                duckdb::child_list_t<duckdb::Value> struct_values;

                for (auto tag_type : genotype_tags)
                {
                    auto tag = tag_type.tag;
                    SPDLOG_INFO("Ingesting tag {}", tag);
                    if (tag == "GT")
                    {
                        if (n_gt < 0)
                        {
                            throw std::runtime_error("Could not get genotypes");
                        }

                        // one genotype per sample
                        if (n_gt == bind_data->n_sample)
                        {
                            throw std::runtime_error("gts = nsamples");
                        }
                        else
                        {
                            // per sample
                            int max_ploidy = n_gt / bind_data->n_sample;
                            std::string gt_str;

                            for (int j = 0; j < max_ploidy; j++)
                            {
                                auto idx = i * max_ploidy + j;
                                auto ptr = gt_arr[idx];

                                auto is_missing = bcf_gt_is_missing(ptr);

                                if (is_missing == 1)
                                {
                                    gt_str += ".";
                                    continue;
                                    // break;
                                }

                                if (bcf_int32_vector_end == ptr)
                                {
                                    // end of ploidy
                                    break;
                                }

                                int allele_index = bcf_gt_allele(ptr);
                                int phased = bcf_gt_is_phased(ptr);
                                int phase = bcf_gt_phased(ptr);

                                auto sep = phased == 0 ? "/" : "|";

                                if (j == max_ploidy - 1)
                                {
                                    gt_str += std::to_string(allele_index);
                                }
                                else
                                {
                                    // TODO: fix, this assumes a specific phasing
                                    gt_str += std::to_string(allele_index) + sep;
                                }
                            }

                            struct_values.push_back(std::make_pair(tag, duckdb::Value(gt_str)));
                        }
                    }
                    else if (int_maps.count(tag) != 0)
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
                    else if (string_maps.count(tag) != 0)
                    {
                        auto &arr = string_maps[tag];
                        if (arr.get_op_response > 0)
                        {
                            if (arr.number == bind_data->n_sample)
                            {
                                // SPDLOG_INFO("Ingesting string {} with value {}", tag, arr.arr[i]);
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
                                        field_values.push_back(duckdb::Value(arr_value));
                                    }
                                    else
                                    {
                                        field_values.push_back(duckdb::Value());
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
                    else if (float_maps.count(tag) != 0)
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

                                    if (arr_value > bcf_float_missing)
                                    {
                                        field_values.push_back(duckdb::Value::FLOAT(arr_value));
                                    }
                                    else
                                    {
                                        field_values.push_back(duckdb::Value());
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
                    else if (bool_maps.count(tag) != 0)
                    {
                        auto &arr = bool_maps[tag];
                        if (arr.get_op_response > 0)
                        {
                            if (arr.number == bind_data->n_sample)
                            {
                                struct_values.push_back(std::make_pair(tag, duckdb::Value::BOOLEAN(arr.arr[i])));
                            }
                            else
                            {
                                auto per_sample = arr.number / bind_data->n_sample;
                                std::vector<duckdb::Value> field_values;
                                for (int j = 0; j < per_sample; j++)
                                {
                                    auto arr_value = arr.arr[i * per_sample + j];
                                    field_values.push_back(duckdb::Value::BOOLEAN(arr_value));
                                }
                                struct_values.push_back(std::make_pair(tag, duckdb::Value::LIST(field_values)));
                            }
                        }
                        else
                        {
                            struct_values.push_back(std::make_pair(tag, duckdb::Value()));
                        }
                    }
                    else
                    {
                        throw std::runtime_error("Unknown tag " + tag);
                    }
                }

                SPDLOG_INFO("Struct values size: {}", struct_values.size());
                genotype_structs.push_back(duckdb::Value::STRUCT(struct_values));
            }

            if (genotype_structs.size() == 0)
            {
                output.SetValue(8, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(8, output.size(), duckdb::Value::LIST(genotype_structs));
            }

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
