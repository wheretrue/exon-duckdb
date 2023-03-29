#ifdef WFA2_ENABLED

#pragma once

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "bindings/cpp/WFAligner.hpp"

namespace wtt01
{
    class AlignmentFunctions
    {
    public:
        static duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> GetAlignmentStringFunction();

        struct AlignmentStringBindData : public duckdb::FunctionData
        {
            AlignmentStringBindData() : aligner(4, 6, 2, wfa::WFAligner::Alignment, wfa::WFAligner::MemoryHigh) {}

            AlignmentStringBindData(int32_t mismatch, int32_t gap_opening, int32_t gap_closing, wfa::WFAligner::MemoryModel memory_model) : aligner(mismatch, gap_opening, gap_closing, wfa::WFAligner::Alignment, memory_model) {}
            AlignmentStringBindData(int32_t match, int32_t mismatch, int32_t gap_opening, int32_t gap_closing, wfa::WFAligner::MemoryModel memory_model) : aligner(match, mismatch, gap_opening, gap_closing, wfa::WFAligner::Alignment, memory_model) {}

            wfa::WFAlignerGapAffine aligner;

            virtual bool Equals(const duckdb::FunctionData &other_p) const override;
            std::unique_ptr<duckdb::FunctionData> Copy() const override;
        };

        static duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> GetAlignmentScoreFunction();

        struct AlignmentScoreBindData : public duckdb::FunctionData
        {
            AlignmentScoreBindData() : aligner(4, 6, 2, wfa::WFAligner::Alignment, wfa::WFAligner::MemoryHigh) {}

            AlignmentScoreBindData(int32_t mismatch, int32_t gap_opening, int32_t gap_closing, wfa::WFAligner::MemoryModel memory_model) : aligner(mismatch, gap_opening, gap_closing, wfa::WFAligner::Alignment, memory_model) {}
            AlignmentScoreBindData(int32_t match, int32_t mismatch, int32_t gap_opening, int32_t gap_closing, wfa::WFAligner::MemoryModel memory_model) : aligner(match, mismatch, gap_opening, gap_closing, wfa::WFAligner::Alignment, memory_model) {}

            wfa::WFAlignerGapAffine aligner;

            virtual bool Equals(const duckdb::FunctionData &other_p) const override;
            std::unique_ptr<duckdb::FunctionData> Copy() const override;
        };
    };
}

#endif
