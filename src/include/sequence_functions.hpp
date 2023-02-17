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

        static duckdb::CreateScalarFunctionInfo GetTranscribeDnaToRnaFunction();

        static std::vector<duckdb::CreateScalarFunctionInfo> GetSequenceFunctions();
    };
}
