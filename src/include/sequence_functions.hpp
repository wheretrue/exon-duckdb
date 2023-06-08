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

#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace wtt01
{
    class SequenceFunctions
    {
    public:
        static duckdb::CreateScalarFunctionInfo GetReverseComplementFunction();
        static duckdb::CreateScalarFunctionInfo GetComplementFunction();
        static duckdb::CreateScalarFunctionInfo GetGCContentFunction();
        static duckdb::CreateScalarFunctionInfo GetReverseTranscribeRnaToDnaFunction();
        static duckdb::CreateScalarFunctionInfo GetTranslateDnaToAminoAcidFunction();

        static duckdb::CreateScalarFunctionInfo GetAlignFunction();

        static duckdb::CreateScalarFunctionInfo GetTranscribeDnaToRnaFunction();

        static std::vector<duckdb::CreateScalarFunctionInfo> GetSequenceFunctions();
    };
}
