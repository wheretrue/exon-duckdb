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

        std::vector<std::string> format_strings;
        std::vector<std::string> format_type_strings;
        std::vector<std::string> format_number_strings;

        std::vector<WTInfoField> info_fields;
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
        return_types.push_back(duckdb::LogicalType::VARCHAR);

        names.push_back("info");

        duckdb::child_list_t<duckdb::LogicalType> info_children;

        auto info_fields = &reader.info_fields;
        for (int i = 0; i < info_fields->total_fields; i++)
        {
            auto info_field = next_info_field(info_fields);

            result->info_fields.push_back(info_field);
            // printf("info_field: %s %s %s\n", info_field.id, info_field.ty, info_field.number);

            if (strcmp(info_field.ty, "Integer") == 0 && strcmp(info_field.number, "1") == 0)
            {
                info_children.push_back(std::make_pair(info_field.id, duckdb::LogicalType::BIGINT));
            }
            else if (strcmp(info_field.ty, "Float") == 0 && strcmp(info_field.number, "1") == 0)
            {
                info_children.push_back(std::make_pair(info_field.id, duckdb::LogicalType::FLOAT));
            }
            else if (strcmp(info_field.ty, "Float") == 0 && strcmp(info_field.number, "1") != 0)
            {
                info_children.push_back(std::make_pair(info_field.id, duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT)));
            }
            else if (strcmp(info_field.ty, "String") == 0 && strcmp(info_field.number, "1") == 0)
            {
                info_children.push_back(std::make_pair(info_field.id, duckdb::LogicalType::VARCHAR));
            }
            else if (strcmp(info_field.ty, "String") == 0 && strcmp(info_field.number, "1") != 0)
            {
                info_children.push_back(std::make_pair(info_field.id, duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)));
            }
            else if (strcmp(info_field.ty, "Integer") == 0 && strcmp(info_field.number, "1") != 0)
            {
                info_children.push_back(std::make_pair(info_field.id, duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER)));
            }
            else if (strcmp(info_field.ty, "Flag") == 0)
            {
                info_children.push_back(std::make_pair(info_field.id, duckdb::LogicalType::BOOLEAN));
            }
            else
            {
                throw std::runtime_error("Unknown info field type: " + std::string(info_field.ty));
            }
        }
        return_types.push_back(duckdb::LogicalType::STRUCT(move(info_children)));

        names.push_back("genotypes");

        duckdb::child_list_t<duckdb::LogicalType> struct_children;
        struct_children.push_back(std::make_pair("sample_id", duckdb::LogicalType::VARCHAR));

        auto formats = &reader.formats;
        for (int i = 0; i < formats->total_fields; i++)
        {
            auto format = next_format(formats);

            // printf("Adding format: %s %s %s\n", format.id, format.ty, format.number);

            result->format_strings.push_back(format.id);
            result->format_type_strings.push_back(format.ty);
            result->format_number_strings.push_back(format.number);

            if (strcmp(format.ty, "Integer") == 0 && strcmp(format.number, "1") == 0)
            {
                struct_children.push_back(std::make_pair(format.id, duckdb::LogicalType::BIGINT));
            }
            else if (strcmp(format.ty, "Float") == 0 && strcmp(format.number, "1") == 0)
            {
                struct_children.push_back(std::make_pair(format.id, duckdb::LogicalType::FLOAT));
            }
            else if (strcmp(format.ty, "String") == 0 && strcmp(format.number, "1") == 0)
            {
                struct_children.push_back(std::make_pair(format.id, duckdb::LogicalType::VARCHAR));
            }
            else if (strcmp(format.ty, "String") == 0 && strcmp(format.number, "1") != 0)
            {
                struct_children.push_back(std::make_pair(format.id, duckdb::LogicalType::LIST(duckdb::LogicalType::VARCHAR)));
            }
            else if (strcmp(format.ty, "Integer") == 0 && strcmp(format.number, "1") != 0)
            {
                struct_children.push_back(std::make_pair(format.id, duckdb::LogicalType::LIST(duckdb::LogicalType::BIGINT)));
            }
            else if (strcmp(format.ty, "Float") == 0 && strcmp(format.number, "1") != 0)
            {
                struct_children.push_back(std::make_pair(format.id, duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT)));
            }
            else
            {
                throw std::runtime_error("Unknown format type: " + std::string(format.ty));
            }
        }

        return_types.push_back(duckdb::LogicalType::LIST(duckdb::LogicalType::STRUCT(struct_children)));

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

        auto header = bind_data->reader.header;

        auto format_strings = bind_data->format_strings;
        auto format_type_strings = bind_data->format_type_strings;
        auto format_number_strings = bind_data->format_number_strings;

        auto info_fields = bind_data->info_fields;

        if (local_state->done)
        {
            return;
        }

        while (output.size() < STANDARD_VECTOR_SIZE)
        {
            VCFRecord record = vcf_next(&bind_data->reader);

            if (record.chromosome == NULL || strcmp(record.chromosome, "") == 0)
            {
                local_state->done = true;
                break;
            }

            output.SetValue(0, output.size(), duckdb::Value(record.chromosome));

            if (record.ids == NULL)
            {
                output.SetValue(1, output.size(), duckdb::Value());
            }
            else
            {
                // split the string on ';' and add to a list
                auto ids = duckdb::StringUtil::Split(record.ids, ';');
                std::vector<duckdb::Value> values;

                for (auto id : ids)
                {
                    // duckdb::StringUtil::Trim(id);
                    // duckdb::StringUtil::Trim(id, '"');
                    values.push_back(duckdb::Value(id));
                }

                output.SetValue(1, output.size(), duckdb::Value::LIST(values));
            }

            output.SetValue(2, output.size(), duckdb::Value::BIGINT(record.position));
            output.SetValue(3, output.size(), duckdb::Value(record.reference_bases));
            output.SetValue(4, output.size(), duckdb::Value(record.alternate_bases));

            if (std::isnan(record.quality_score))
            {
                output.SetValue(5, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(5, output.size(), duckdb::Value::FLOAT(record.quality_score));
            }

            if (record.filters == NULL || strcmp(record.filters, "") == 0)
            {
                output.SetValue(6, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(6, output.size(), duckdb::Value(record.filters));
            }

            // auto info_fields = &reader.info_fields;
            auto infos = record.infos;
            duckdb::child_list_t<duckdb::Value> struct_values;

            for (WTInfoField info_field : info_fields)
            {
                auto info = next_info(info_field.id, &infos);

                if (info.error != NULL)
                {
                    throw std::runtime_error("For field: " + std::string(info_field.id) + ", got error: " + std::string(info.error));
                }

                if (info.value == NULL)
                {
                    struct_values.push_back(std::make_pair(info_field.id, duckdb::Value()));
                    continue;
                }

                if (strcmp(info_field.ty, "Integer") == 0 && strcmp(info_field.number, "1") == 0)
                {
                    auto v = info_field_int(&info);
                    struct_values.push_back(std::make_pair(info_field.id, duckdb::Value::BIGINT(v)));
                }
                else if (strcmp(info_field.ty, "Integer") == 0 && strcmp(info_field.number, "1") != 0)
                {
                    auto vs = info_field_ints(&info);
                    std::vector<duckdb::Value> values;

                    for (int i = 0; i < vs.number_of_fields; i++)
                    {
                        auto vi = info_field_int_value(&vs);
                        values.push_back(duckdb::Value::BIGINT(vi));
                    }

                    struct_values.push_back(std::make_pair(info_field.id, duckdb::Value::LIST(values)));
                }
                else if (strcmp(info_field.ty, "Float") == 0 && strcmp(info_field.number, "1") != 0)
                {
                    auto vs = info_field_floats(&info);

                    std::vector<duckdb::Value> values;

                    for (int i = 0; i < vs.number_of_fields; i++)
                    {
                        auto vi = info_field_float_value(&vs);

                        if (vi.is_null)
                        {
                            values.push_back(duckdb::Value::FLOAT(NAN));
                        }
                        else
                        {
                            values.push_back(duckdb::Value::FLOAT(vi.value));
                        }
                    }
                    struct_values.push_back(std::make_pair(info_field.id, duckdb::Value::LIST(values)));
                }
                else if (strcmp(info_field.ty, "Float") == 0 && strcmp(info_field.number, "1") == 0)
                {
                    auto v = info_field_float(&info);
                    struct_values.push_back(std::make_pair(info.id, duckdb::Value::FLOAT(0.1)));
                }
                else if (strcmp(info_field.ty, "String") == 0 && strcmp(info_field.number, "1") == 0)
                {
                    auto v = info_field_string(&info);
                    struct_values.push_back(std::make_pair(info.id, duckdb::Value(v)));
                }
                else if (strcmp(info_field.ty, "String") == 0 && strcmp(info_field.number, "1") != 0)
                {
                    auto v = info_field_strings(&info);

                    std::vector<duckdb::Value> values;

                    for (int i = 0; i < v.number_of_fields; i++)
                    {
                        auto vi = info_field_string_value(&v);
                        values.push_back(duckdb::Value(vi));
                    }

                    struct_values.push_back(std::make_pair(info_field.id, duckdb::Value::LIST(values)));
                }
                else if (strcmp(info_field.ty, "Flag") == 0)
                {
                    auto v = info_field_bool(&info);
                    struct_values.push_back(std::make_pair(info.id, duckdb::Value::BOOLEAN(v)));
                }
                else
                {
                    throw std::runtime_error("Unsupported type for info field: " + std::string(info_field.ty) + " " + std::string(info_field.number));
                }
            }

            if (struct_values.size() == 0)
            {
                output.SetValue(7, output.size(), duckdb::Value());
            }
            else
            {
                output.SetValue(7, output.size(), duckdb::Value::STRUCT(struct_values));
            }

            std::vector<duckdb::Value> genotype_structs;

            auto genotypes = record.genotypes;
            auto sample_names = duckdb::StringUtil::Split(get_sample_names(header), ',');

            for (int i = 0; i < genotypes.number_of_genotypes; i++)
            {
                duckdb::child_list_t<duckdb::Value> struct_values;
                struct_values.push_back(std::make_pair("sample_name", duckdb::Value(sample_names[i])));

                auto genotype = next_genotype(&genotypes);

                for (int j = 0; j < format_strings.size(); j++)
                {
                    // The necessary info in order to parse the genotype field
                    auto key_type = format_type_strings[j];
                    auto key_name = format_strings[j];
                    auto key_number = format_number_strings[j];

                    if (key_type == "Integer" && key_number == "1")
                    {
                        auto value = next_int_key(&genotype, key_name.c_str());

                        if (value.error != NULL)
                        {
                            throw std::runtime_error(value.error);
                        }

                        if (value.none)
                        {
                            struct_values.push_back(std::make_pair(key_name, duckdb::Value::BIGINT(0)));
                        }
                        else
                        {
                            struct_values.push_back(std::make_pair(key_name, duckdb::Value::BIGINT(value.value)));
                        }
                    }
                    else if (key_type == "String" && key_number == "1")
                    {
                        auto value = next_string_key(&genotype, key_name.c_str());

                        if (value.error != NULL)
                        {
                            throw std::runtime_error(value.error);
                        }

                        if (value.none)
                        {
                            struct_values.push_back(std::make_pair(key_name, duckdb::Value("")));
                        }
                        else
                        {
                            struct_values.push_back(std::make_pair(key_name, duckdb::Value(value.value)));
                        }
                    }
                    else if (key_type == "Float" && key_number == "1")
                    {
                        auto value = next_float_key(&genotype, key_name.c_str());

                        if (value.error != NULL)
                        {
                            throw std::runtime_error(value.error);
                        }

                        if (value.none)
                        {
                            struct_values.push_back(std::make_pair(key_name, duckdb::Value()));
                        }
                        else
                        {
                            struct_values.push_back(std::make_pair(key_name, duckdb::Value::FLOAT(value.value)));
                        }
                    }
                    else if (key_type == "Float" && key_number != "1")
                    {
                        auto key_list = next_float_list_key(&genotype, key_name.c_str());
                        if (key_list.error != NULL)
                        {
                            throw std::runtime_error(key_list.error);
                        }

                        std::vector<duckdb::Value> values;
                        auto list_length = key_list.length;

                        if (list_length == 0)
                        {
                            // TODO: handle this case
                            values.push_back(duckdb::Value::FLOAT(1.0));
                        }

                        for (int ll = 0; ll < list_length; ll++)
                        {
                            auto list_value = next_float_list_value(&key_list);

                            values.push_back(duckdb::Value::FLOAT(list_value));
                        }

                        struct_values.push_back(std::make_pair(key_name, duckdb::Value::LIST(values)));
                    }

                    else if (key_type == "String" && key_number != "1")
                    {
                        auto key_list = next_string_list_key(&genotype, key_name.c_str());
                        if (key_list.error != NULL)
                        {
                            throw std::runtime_error(key_list.error);
                        }

                        std::vector<duckdb::Value> values;
                        auto list_length = key_list.length;

                        for (int ll = 0; ll < list_length; ll++)
                        {
                            auto list_value = next_string_list_value(&key_list);

                            values.push_back(duckdb::Value(list_value));
                        }

                        struct_values.push_back(std::make_pair(key_name, duckdb::Value::LIST(values)));
                    }

                    else if (key_type == "Integer" && key_number != "1")
                    {
                        auto key_list = next_int_list_key(&genotype, key_name.c_str());
                        if (key_list.error != NULL)
                        {
                            throw std::runtime_error(key_list.error);
                        }

                        std::vector<duckdb::Value> values;

                        auto list_length = key_list.length;

                        for (int ll = 0; ll < list_length; ll++)
                        {
                            auto list_value = next_int_list_value(&key_list);

                            if (list_value.error != NULL)
                            {
                                throw std::runtime_error(list_value.error);
                            }

                            if (list_value.is_none)
                            {
                                values.push_back(duckdb::Value::BIGINT(0));
                            }
                            else
                            {
                                values.push_back(duckdb::Value::BIGINT(list_value.value));
                            }
                        }

                        if (values.size() != 0)
                        {
                            struct_values.push_back(std::make_pair(key_name, duckdb::Value::LIST(values)));
                        }
                        else
                        {
                            struct_values.push_back(std::make_pair(key_name, duckdb::Value()));
                        }
                    }
                    else
                    {
                        throw std::runtime_error("Unsupported type for genotype field: " + key_type + " " + key_number);
                    }
                }

                genotype_structs.push_back(duckdb::Value::STRUCT(struct_values));
            }

            output.SetValue(8, output.size(), duckdb::Value::LIST(genotype_structs));

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
