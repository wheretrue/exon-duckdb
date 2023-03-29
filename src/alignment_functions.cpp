#include <duckdb.hpp>

#include <string>
#include <map>

#include "bindings/cpp/WFAligner.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "alignment_functions.hpp"

using duckdb::LogicalType;

namespace wtt01
{

    bool AlignmentFunctions::AlignBindData::Equals(const duckdb::FunctionData &other_p) const
    {
        // TODO: implement this or add the underlying accessors
        return true;
    }

    std::unique_ptr<duckdb::FunctionData> AlignmentFunctions::AlignBindData::Copy() const
    {
        auto result = duckdb::make_unique<AlignBindData>();
        result->aligner = aligner;

        return std::move(result);
    }

    void AlignFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        auto &func_expr = (duckdb::BoundFunctionExpression &)state.expr;
        auto &info = (AlignmentFunctions::AlignBindData &)*func_expr.bind_info;
        auto &aligner = info.aligner;

        for (duckdb::idx_t row_i = 0; row_i < args.size(); row_i++)
        {
            auto string_value = args.data[0].GetValue(row_i);
            auto sequence = string_value.ToString();

            auto second_string_value = args.data[1].GetValue(row_i);
            auto second_sequence = second_string_value.ToString();

            aligner.alignEnd2End(sequence, second_sequence);
            auto alignment = aligner.getAlignmentCigar();

            result.SetValue(row_i, duckdb::Value(alignment));
        }
    }

    std::unique_ptr<duckdb::FunctionData> AlignBind(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, std::vector<std::unique_ptr<duckdb::Expression>> &arguments)
    {
        if (arguments.size() == 2)
        {
            return duckdb::make_unique<AlignmentFunctions::AlignBindData>();
        }

        if (arguments.size() == 7)
        {
            duckdb::Value mismatch_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
            int mismatch = duckdb::IntegerValue::Get(mismatch_value);

            duckdb::Value gap_opening_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[3]);
            int gap_opening = duckdb::IntegerValue::Get(gap_opening);

            duckdb::Value gap_closing_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
            int gap_closing = duckdb::IntegerValue::Get(gap_closing);

            duckdb::Value alignment_scope_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[5]);
            std::string alignment_scope = alignment_scope_value.ToString();

            wfa::WFAligner::AlignmentScope scope;
            if (alignment_scope == "alignment")
            {
                scope = wfa::WFAligner::Alignment;
            }
            else if (alignment_scope == "score")
            {
                scope = wfa::WFAligner::Score;
            }
            else
            {
                throw duckdb::InvalidInputException(std::string("Invalid alignment scope: ") + alignment_scope);
            }

            duckdb::Value memory_model_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[6]);
            std::string memory_model = memory_model_value.ToString();

            wfa::WFAligner::MemoryModel model;
            if (memory_model == "memory_high")
            {
                model = wfa::WFAligner::MemoryHigh;
            }
            else if (memory_model == "memory_med")
            {
                model = wfa::WFAligner::MemoryMed;
            }
            else if (memory_model == "memory_low")
            {
                model = wfa::WFAligner::MemoryLow;
            }
            else
            {
                throw duckdb::InvalidInputException(std::string("Invalid memory model: ") + memory_model);
            }

            return duckdb::make_unique<AlignmentFunctions::AlignBindData>(mismatch, 6, 2, scope, model);
        }

        if (arguments.size() == 8)
        {
            duckdb::Value match_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
            int match = duckdb::IntegerValue::Get(match_value);

            if (match > 0)
            {
                throw duckdb::InvalidInputException("Match score must be negative or zero.");
            }

            duckdb::Value mismatch_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[3]);
            int mismatch = duckdb::IntegerValue::Get(mismatch_value);

            duckdb::Value gap_opening_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
            int gap_opening = duckdb::IntegerValue::Get(gap_opening);

            duckdb::Value gap_closing_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[5]);
            int gap_closing = duckdb::IntegerValue::Get(gap_closing);

            duckdb::Value alignment_scope_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[6]);
            std::string alignment_scope = alignment_scope_value.ToString();

            wfa::WFAligner::AlignmentScope scope;
            if (alignment_scope == "alignment")
            {
                scope = wfa::WFAligner::Alignment;
            }
            else if (alignment_scope == "score")
            {
                scope = wfa::WFAligner::Score;
            }
            else
            {
                throw duckdb::InvalidInputException(std::string("Invalid alignment scope: ") + alignment_scope);
            }

            duckdb::Value memory_model_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[7]);
            std::string memory_model = memory_model_value.ToString();

            wfa::WFAligner::MemoryModel model;
            if (memory_model == "memory_high")
            {
                model = wfa::WFAligner::MemoryHigh;
            }
            else if (memory_model == "memory_low")
            {
                model = wfa::WFAligner::MemoryLow;
            }
            else
            {
                throw duckdb::InvalidInputException(std::string("Invalid memory model: ") + memory_model);
            }

            return duckdb::make_unique<AlignmentFunctions::AlignBindData>(match, mismatch, 6, 2, scope, model);
        }

        throw duckdb::InvalidInputException("Invalid number of arguments for align function");
    }

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> AlignmentFunctions::GetAlignFunction()
    {
        duckdb::ScalarFunctionSet set("align_wfa_gap_affine");

        auto align_function = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::VARCHAR, AlignFunction, AlignBind);
        set.AddFunction(align_function);

        auto align_function_match_arg = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                                                                duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
                                                                duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
                                                               duckdb::LogicalType::VARCHAR, AlignFunction, AlignBind);
        set.AddFunction(align_function_match_arg);

        auto align_function_mismatch_arg = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                                                                   duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
                                                                   duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
                                                                  duckdb::LogicalType::VARCHAR, AlignFunction, AlignBind);
        set.AddFunction(align_function_mismatch_arg);

        return duckdb::make_unique<duckdb::CreateScalarFunctionInfo>(set);
    }
}
