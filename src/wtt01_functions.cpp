#include <duckdb.hpp>
#include <string>
#include <map>

#include "wtt01_functions.hpp"
#include "wtt01_rust.hpp"

namespace wtt01
{

    const std::string WTT_01_VERSION = "0.3.6";

    duckdb::CreateScalarFunctionInfo Wtt01Functions::GetWtt01VersionFunction()
    {
        duckdb::ScalarFunctionSet set("exondb_version");

        auto duckdb_function = [](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
        {
            result.SetValue(0, duckdb::Value(WTT_01_VERSION));
        };

        set.AddFunction(duckdb::ScalarFunction({}, duckdb::LogicalType::VARCHAR, duckdb_function));

        return duckdb::CreateScalarFunctionInfo(std::move(set));
    }

}
