#define DUCKDB_EXTENSION_MAIN

#include "wtt01_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "sequence_functions.hpp"

namespace duckdb
{

    inline void Wtt01ScalarFun(DataChunk &args, ExpressionState &state, Vector &result)
    {
        auto &name_vector = args.data[0];
        UnaryExecutor::Execute<string_t, string_t>(
            name_vector, result, args.size(),
            [&](string_t name)
            {
                return StringVector::AddString(result, "Wtt01 " + name.GetString() + " 🐥");
                ;
            });
    }

    static void LoadInternal(DatabaseInstance &instance)
    {
        Connection con(instance);
        con.BeginTransaction();

        auto &catalog = Catalog::GetSystemCatalog(*con.context);

        auto sequence_functions = wtt01::SequenceFunctions::GetSequenceFunctions();
        for (auto &fun : sequence_functions)
        {
            catalog.CreateFunction(*con.context, &fun);
        }
        con.Commit();
    }

    void Wtt01Extension::Load(DuckDB &db)
    {
#if defined(CHECK_LICENSE)
        try
        {
            wtt01::LicenseCheck::ValidateLicense();
        }
        catch (std::exception &e)
        {
            throw InvalidInputException(std::string("License verification failed: " + std::string(e.what())));
        }
#endif
        LoadInternal(*db.instance);
    }
    std::string Wtt01Extension::Name()
    {
        return "wtt01";
    }

} // namespace duckdb

extern "C"
{

    DUCKDB_EXTENSION_API void wtt01_init(duckdb::DatabaseInstance &db)
    {
        LoadInternal(db);
    }

    DUCKDB_EXTENSION_API const char *wtt01_version()
    {
        return duckdb::DuckDB::LibraryVersion();
    }
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
