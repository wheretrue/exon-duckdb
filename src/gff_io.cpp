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

#include "gff_io.hpp"
#include "rust.hpp"

namespace wtt01
{
    duckdb::CreateScalarFunctionInfo GFFunctions::GetGFFParseAttributesFunction()
    {
        duckdb::ScalarFunctionSet set("gff_parse_attributes");

        auto duckdb_function = [](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
        {
            result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

            // TODO: add soft failure
            for (duckdb::idx_t i = 0; i < args.size(); i++)
            {
                auto value = args.data[0].GetValue(i);
                auto string_value = duckdb::StringValue::Get(value);

                auto last_semicolon_pos = string_value.find_last_of(";");
                auto string_len = string_value.size();
                if (last_semicolon_pos == string_len)
                {
                    string_value = string_value.substr(0, string_len - 1);
                }

                auto attributes = duckdb::StringUtil::Split(string_value, ";");

                duckdb::vector<duckdb::Value> items;

                for (auto &attribute : attributes)
                {
                    duckdb::StringUtil::Trim(attribute);
                    auto key_value = duckdb::StringUtil::Split(attribute, "=");

                    if (key_value.size() != 2)
                    {
                        throw duckdb::Exception("Invalid attribute: '" + attribute + "' expected 'key=value;key2=value2'");
                    }

                    duckdb::child_list_t<duckdb::Value> map_struct;

                    auto new_key = duckdb::Value(key_value[0]);
                    auto new_value = duckdb::Value(key_value[1]);

                    map_struct.emplace_back(std::make_pair("key", std::move(new_key)));
                    map_struct.emplace_back(std::make_pair("value", std::move(new_value)));

                    items.push_back(duckdb::Value::STRUCT(std::move(map_struct)));
                }

                duckdb::LogicalType map_type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR);
                result.SetValue(i, duckdb::Value::MAP(duckdb::ListType::GetChildType(map_type), std::move(items)));
            }
        };

        auto return_type = duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR);
        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, return_type, duckdb_function));

        return duckdb::CreateScalarFunctionInfo(set);
    }

}
