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

#include "exon/sam_functions/module.hpp"

#include "exon_duckdb.hpp"

#include <iostream>

#include <duckdb.hpp>
#include <duckdb.h>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/main/capi/capi_internal.hpp>

namespace exon
{

    void ParseCIGARString(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

        for (duckdb::idx_t i = 0; i < args.size(); i++)
        {
            auto string_value = args.data[0].GetValue(i);
            auto ss = string_value.ToString();

            CResult cigar = parse_cigar(ss.c_str());
            if (cigar.error)
            {
                throw std::runtime_error("Invalid CIGAR string: " + ss);
            }

            auto ops = duckdb::StringUtil::Split(cigar.value, ';');

            duckdb::vector<duckdb::Value> op_values;

            for (auto op : ops)
            {
                duckdb::child_list_t<duckdb::Value> struct_values;
                auto op_parts = duckdb::StringUtil::Split(op, '=');

                if (op_parts.size() != 2)
                {
                    throw std::runtime_error("Invalid CIGAR string");
                }

                auto op_type = op_parts[0];
                auto op_length = op_parts[1];

                auto op_type_value = duckdb::Value(op_type);
                auto op_length_value = duckdb::Value::INTEGER(std::atoi(op_length.c_str()));

                struct_values.push_back(std::make_pair("op", op_type_value));
                struct_values.push_back(std::make_pair("len", op_length_value));

                op_values.push_back(duckdb::Value::STRUCT(struct_values));
            }

            result.SetValue(i, duckdb::Value::LIST(op_values));
        }
    }

    void ExtractSequence(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        for (duckdb::idx_t i = 0; i < args.size(); i++)
        {
            auto sequence = args.data[0].GetValue(i).ToString();
            auto cigar = args.data[1].GetValue(i).ToString();

            auto extract_result = extract_from_cigar(sequence.c_str(), cigar.c_str());
            if (extract_result.error)
            {
                throw std::runtime_error("Invalid CIGAR string");
            }

            duckdb::child_list_t<duckdb::Value> struct_values;
            struct_values.push_back(std::make_pair("sequence_start", duckdb::Value::INTEGER(extract_result.sequence_start)));
            struct_values.push_back(std::make_pair("sequence_end", duckdb::Value::INTEGER(extract_result.sequence_len)));
            struct_values.push_back(std::make_pair("sequence", duckdb::Value(extract_result.extracted_sequence)));

            auto struct_value = duckdb::Value::STRUCT(struct_values);

            result.SetValue(i, struct_value);
        }
    }

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> SamFunctions::GetExtractFromCIGARFunction()
    {
        duckdb::ScalarFunctionSet set("extract_from_cigar");

        duckdb::child_list_t<duckdb::LogicalType> struct_children;
        struct_children.push_back(std::make_pair("sequence_start", duckdb::LogicalType::INTEGER));
        struct_children.push_back(std::make_pair("sequence_end", duckdb::LogicalType::INTEGER));
        struct_children.push_back(std::make_pair("sequence", duckdb::LogicalType::VARCHAR));

        auto record_type = duckdb::LogicalType::STRUCT(std::move(struct_children));

        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, record_type, ExtractSequence));

        return duckdb::make_uniq<duckdb::CreateScalarFunctionInfo>(set);
    }

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> SamFunctions::GetParseCIGARStringFunction()
    {
        duckdb::ScalarFunctionSet set("parse_cigar");

        duckdb::child_list_t<duckdb::LogicalType> struct_children;
        struct_children.push_back(std::make_pair("op", duckdb::LogicalType::VARCHAR));
        struct_children.push_back(std::make_pair("len", duckdb::LogicalType::INTEGER));

        auto record_type = duckdb::LogicalType::STRUCT(std::move(struct_children));
        auto row_type = duckdb::LogicalType::LIST(std::move(record_type));

        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, row_type, ParseCIGARString));

        return duckdb::make_uniq<duckdb::CreateScalarFunctionInfo>(set);
    }

    std::vector<duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo>> SamFunctions::GetSamFunctions()
    {

        struct SamFunction
        {
            std::string name;
            std::function<bool(int32_t)> func;
        };

        std::vector<SamFunction> sam_functions = {
            {"is_segmented", is_segmented},
            {"is_unmapped", is_unmapped},
            {"is_properly_aligned", is_properly_aligned},
            {"is_mate_unmapped", is_mate_unmapped},
            {"is_reverse_complemented", is_reverse_complemented},
            {"is_mate_reverse_complemented", is_mate_reverse_complemented},
            {"is_first_segment", is_first_segment},
            {"is_last_segment", is_last_segment},
            {"is_secondary", is_secondary},
            {"is_quality_control_failed", is_quality_control_failed},
            {"is_duplicate", is_duplicate},
            {"is_supplementary", is_supplementary}};

        std::vector<duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo>> sam_scalar_functions;

        for (auto &sam_function : sam_functions)
        {
            duckdb::ScalarFunctionSet set(sam_function.name);

            auto duckdb_function = [sam_function](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
            {
                result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
                for (duckdb::idx_t i = 0; i < args.size(); i++)
                {
                    auto value = args.data[0].GetValue(i);
                    auto int_value = duckdb::IntegerValue::Get(value);

                    auto bool_value = sam_function.func(int_value);

                    result.SetValue(i, duckdb::Value::BOOLEAN(bool_value));
                }
            };

            set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::INTEGER}, duckdb::LogicalType::BOOLEAN, duckdb_function));

            sam_scalar_functions.emplace_back(duckdb::make_uniq<duckdb::CreateScalarFunctionInfo>(set));
        }

        return sam_scalar_functions;
    }

}
