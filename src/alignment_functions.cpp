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

    struct WFAOptions
    {
        int match;
        int mismatch;
        int gap_opening;
        int gap_extension;
        wfa::WFAligner::MemoryModel memory_model;

        std::string ToString()
        {
            std::string result = "WFAOptions(";
            result += "match: " + std::to_string(match) + ", ";
            result += "mismatch: " + std::to_string(mismatch) + ", ";
            result += "gap_opening: " + std::to_string(gap_opening) + ", ";
            result += "gap_extension: " + std::to_string(gap_extension) + ", ";
            result += "memory_model: " + std::to_string(memory_model) + ")";
            return result;
        }

        // Create a new WFAOptions object from a DuckDB context and a list of arguments
        WFAOptions(duckdb::ClientContext &context, std::vector<std::unique_ptr<duckdb::Expression>> &arguments)
        {
            if (arguments.size() == 2)
            {
                match = 6;
                mismatch = 2;
                gap_opening = 6;
                gap_extension = 2;
                memory_model = wfa::WFAligner::MemoryHigh;
            }
            else if (arguments.size() == 6)
            {
                duckdb::Value mismatch_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
                mismatch = duckdb::IntegerValue::Get(mismatch_value);

                duckdb::Value gap_opening_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
                gap_opening = duckdb::IntegerValue::Get(gap_opening_value);

                duckdb::Value gap_extension_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
                gap_extension = duckdb::IntegerValue::Get(gap_extension_value);

                duckdb::Value memory_model_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[5]);
                std::string memory_model = memory_model_value.ToString();

                if (memory_model == "memory_high")
                {
                    memory_model = wfa::WFAligner::MemoryHigh;
                }
                else if (memory_model == "memory_med")
                {
                    memory_model = wfa::WFAligner::MemoryMed;
                }
                else if (memory_model == "memory_low")
                {
                    memory_model = wfa::WFAligner::MemoryLow;
                }
                else
                {
                    throw duckdb::InvalidInputException(std::string("Invalid memory model: ") + memory_model);
                }
            }
            else if (arguments.size() == 7)
            {
                duckdb::Value match_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
                match = duckdb::IntegerValue::Get(match_value);

                if (match > 0)
                {
                    throw duckdb::InvalidInputException("Match score must be negative or zero.");
                }

                duckdb::Value mismatch_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[3]);
                mismatch = duckdb::IntegerValue::Get(mismatch_value);

                duckdb::Value gap_opening_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[4]);
                gap_opening = duckdb::IntegerValue::Get(gap_opening_value);

                duckdb::Value gap_extension_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[5]);
                gap_extension = duckdb::IntegerValue::Get(gap_extension_value);

                duckdb::Value memory_model_value = duckdb::ExpressionExecutor::EvaluateScalar(context, *arguments[6]);
                std::string memory_model = memory_model_value.ToString();

                if (memory_model == "memory_high")
                {
                    memory_model = wfa::WFAligner::MemoryHigh;
                }
                else if (memory_model == "memory_med")
                {
                    memory_model = wfa::WFAligner::MemoryMed;
                }
                else if (memory_model == "memory_low")
                {
                    memory_model = wfa::WFAligner::MemoryLow;
                }
                else
                {
                    throw duckdb::InvalidInputException(std::string("Invalid memory model: ") + memory_model);
                }
            }
        };
    };

    bool AlignmentFunctions::AlignmentStringBindData::Equals(const duckdb::FunctionData &other_p) const
    {
        // TODO: implement this or add the underlying accessors
        return true;
    }

    std::unique_ptr<duckdb::FunctionData> AlignmentFunctions::AlignmentStringBindData::Copy() const
    {
        auto result = duckdb::make_unique<AlignmentStringBindData>();
        result->aligner = aligner;

        return std::move(result);
    }

    void AlignmentStringFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        auto &func_expr = (duckdb::BoundFunctionExpression &)state.expr;
        auto &info = (AlignmentFunctions::AlignmentStringBindData &)*func_expr.bind_info;
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

    std::unique_ptr<duckdb::FunctionData> AlignmentStringBind2Arguments(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, std::vector<std::unique_ptr<duckdb::Expression>> &arguments)
    {
        auto options = WFAOptions(context, arguments);
        return duckdb::make_unique<AlignmentFunctions::AlignmentStringBindData>();
    }

    std::unique_ptr<duckdb::FunctionData> AlignmentStringBindMatchArgument(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, std::vector<std::unique_ptr<duckdb::Expression>> &arguments)
    {
        auto options = WFAOptions(context, arguments);
        return duckdb::make_unique<AlignmentFunctions::AlignmentStringBindData>(options.mismatch, options.gap_opening, options.gap_extension, options.memory_model);
    }

    std::unique_ptr<duckdb::FunctionData> AlignmentStringBindMismatchArgument(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, std::vector<std::unique_ptr<duckdb::Expression>> &arguments)
    {
        auto options = WFAOptions(context, arguments);
        return duckdb::make_unique<AlignmentFunctions::AlignmentStringBindData>(options.match, options.mismatch, options.gap_opening, options.gap_extension, options.memory_model);
    }

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> AlignmentFunctions::GetAlignmentStringFunction()
    {
        duckdb::ScalarFunctionSet set("alignment_string_wfa_gap_affine");

        auto align_function = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::VARCHAR, AlignmentStringFunction, AlignmentStringBind2Arguments);
        set.AddFunction(align_function);

        auto align_function_match_arg = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                                                                duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
                                                                duckdb::LogicalType::VARCHAR},
                                                               duckdb::LogicalType::VARCHAR, AlignmentStringFunction, AlignmentStringBindMismatchArgument);
        set.AddFunction(align_function_match_arg);

        auto align_function_mismatch_arg = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                                                                   duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
                                                                   duckdb::LogicalType::VARCHAR},
                                                                  duckdb::LogicalType::VARCHAR, AlignmentStringFunction, AlignmentStringBindMatchArgument);
        set.AddFunction(align_function_mismatch_arg);

        return duckdb::make_unique<duckdb::CreateScalarFunctionInfo>(set);
    }

    // Score not alignment functions.

    bool AlignmentFunctions::AlignmentScoreBindData::Equals(const duckdb::FunctionData &other_p) const
    {
        return true;
    }

    std::unique_ptr<duckdb::FunctionData> AlignmentFunctions::AlignmentScoreBindData::Copy() const
    {
        auto result = duckdb::make_unique<AlignmentScoreBindData>();
        result->aligner = aligner;

        return std::move(result);
    }

    void AlignmentScoreFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        auto &func_expr = (duckdb::BoundFunctionExpression &)state.expr;
        auto &info = (AlignmentFunctions::AlignmentScoreBindData &)*func_expr.bind_info;
        auto &aligner = info.aligner;

        for (duckdb::idx_t row_i = 0; row_i < args.size(); row_i++)
        {
            auto string_value = args.data[0].GetValue(row_i);
            auto sequence = string_value.ToString();

            auto second_string_value = args.data[1].GetValue(row_i);
            auto second_sequence = second_string_value.ToString();

            aligner.alignEnd2End(sequence, second_sequence);
            auto score = aligner.getAlignmentScore();

            result.SetValue(row_i, duckdb::Value::FLOAT(score));
        }
    }

    std::unique_ptr<duckdb::FunctionData> AlignmentScoreBind(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, std::vector<std::unique_ptr<duckdb::Expression>> &arguments)
    {
        if (arguments.size() == 2)
        {
            return duckdb::make_unique<AlignmentFunctions::AlignmentScoreBindData>();
        }

        if (arguments.size() == 6)
        {
            auto options = WFAOptions(context, arguments);
            return duckdb::make_unique<AlignmentFunctions::AlignmentScoreBindData>(options.mismatch, options.gap_opening, options.gap_extension, options.memory_model);
        }

        if (arguments.size() == 7)
        {
            auto options = WFAOptions(context, arguments);
            return duckdb::make_unique<AlignmentFunctions::AlignmentScoreBindData>(options.match, options.mismatch, options.gap_opening, options.gap_extension, options.memory_model);
        }

        throw duckdb::InvalidInputException("Invalid number of arguments for align function");
    }

    duckdb::unique_ptr<duckdb::CreateScalarFunctionInfo> AlignmentFunctions::GetAlignmentScoreFunction()
    {
        duckdb::ScalarFunctionSet set("alignment_score_wfa_gap_affine");

        auto align_function = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::FLOAT, AlignmentScoreFunction, AlignmentScoreBind);
        set.AddFunction(align_function);

        auto align_function_match_arg = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                                                                duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
                                                                duckdb::LogicalType::VARCHAR},
                                                               duckdb::LogicalType::FLOAT, AlignmentScoreFunction, AlignmentScoreBind);
        set.AddFunction(align_function_match_arg);

        auto align_function_mismatch_arg = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                                                                   duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
                                                                   duckdb::LogicalType::VARCHAR},
                                                                  duckdb::LogicalType::FLOAT, AlignmentScoreFunction, AlignmentScoreBind);

        set.AddFunction(align_function_mismatch_arg);

        return duckdb::make_unique<duckdb::CreateScalarFunctionInfo>(set);
    }
}
