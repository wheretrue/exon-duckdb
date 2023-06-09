#define DUCKDB_EXTENSION_MAIN

#include "exon_extension.hpp"
#include "exon/sam_functions/module.hpp"
#include "duckdb.hpp"

using namespace duckdb;

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	Connection con(instance);
	con.BeginTransaction();

	auto &context = *con.context;
    auto &catalog = Catalog::GetSystemCatalog(context);

    auto get_sam_functions = exon::SamFunctions::GetSamFunctions();
    for (auto &func : get_sam_functions)
    {
        catalog.CreateFunction(context, *func);
    }

	con.Commit();
}

void ExonExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string ExonExtension::Name() {
	return "exon";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void exon_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *exon_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif