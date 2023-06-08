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

#include <duckdb.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>

#include "fastq_io.hpp"
#include "rust.hpp"

namespace exondb
{

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> FastqFunctions::GetQualityScoreStringToList()
    {
        duckdb::ScalarFunctionSet set("quality_score_string_to_list");

        auto duckdb_function = [](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
        {
            result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

            for (duckdb::idx_t i = 0; i < args.size(); i++)
            {
                auto value = args.data[0].GetValue(i);
                auto string_value = duckdb::StringValue::Get(value);

                duckdb::vector<duckdb::Value> quality_scores;

                for (auto c : string_value)
                {
                    quality_scores.push_back(duckdb::Value::INTEGER(c - 33));
                }

                result.SetValue(i, duckdb::Value::LIST(quality_scores));
            }
        };

        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), duckdb_function));
        return duckdb::make_uniq<duckdb::CreateScalarFunctionInfo>(set);
    }
}
