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

#include <duckdb.hpp>
#include <string>
#include <map>

#include "wtt01_functions.hpp"
#include "rust.hpp"

namespace wtt01
{

    const std::string EXON_01_VERSION = "0.3.9";

    duckdb::CreateScalarFunctionInfo Wtt01Functions::GetWtt01VersionFunction()
    {
        duckdb::ScalarFunctionSet set("exondb_version");

        auto duckdb_function = [](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
        {
            result.SetValue(0, duckdb::Value(EXON_01_VERSION));
        };

        set.AddFunction(duckdb::ScalarFunction({}, duckdb::LogicalType::VARCHAR, duckdb_function));

        return duckdb::CreateScalarFunctionInfo(std::move(set));
    }

}
