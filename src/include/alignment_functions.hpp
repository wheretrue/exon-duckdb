#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "bindings/cpp/WFAligner.hpp"

namespace wtt01
{
    class AlignmentFunctions
    {
    public:
        static duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> GetAlignFunction();

        struct AlignBindData : public duckdb::FunctionData
        {
            AlignBindData() : aligner(4, 6, 2, wfa::WFAligner::Alignment, wfa::WFAligner::MemoryHigh) {}

            AlignBindData(int32_t mismatch, int32_t gap_opening, int32_t gap_closing, wfa::WFAligner::AlignmentScope alignment_scope, wfa::WFAligner::MemoryModel memory_model) : aligner(mismatch, gap_opening, gap_closing, alignment_scope, memory_model) {}
            AlignBindData(int32_t match, int32_t mismatch, int32_t gap_opening, int32_t gap_closing, wfa::WFAligner::AlignmentScope alignment_scope, wfa::WFAligner::MemoryModel memory_model) : aligner(match, mismatch, gap_opening, gap_closing, alignment_scope, memory_model) {}

            wfa::WFAlignerGapAffine aligner;

            virtual bool Equals(const duckdb::FunctionData &other_p) const override;
            std::unique_ptr<duckdb::FunctionData> Copy() const override;
        };
    };
}
