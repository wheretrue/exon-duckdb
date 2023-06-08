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
        WFAOptions(duckdb::ClientContext &context, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments)
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

    duckdb::unique_ptr<duckdb::FunctionData> AlignmentFunctions::AlignmentStringBindData::Copy() const
    {
        auto result = duckdb::make_uniq<AlignmentStringBindData>();
        result->aligner = aligner;

        return std::move(result);
    }

    // AACCTTGGAAACCC -> 2A2C2T2G3A3C
    std::string compressCigarString(std::string str)
    {
        if (str.empty())
        {
            return "";
        }

        std::string compressedStr = "";
        int count = 1;
        char prevChar = str[0];

        for (int i = 1; i < str.length(); i++)
        {
            if (str[i] == prevChar)
            {
                count++;
            }
            else
            {
                compressedStr += std::to_string(count) + prevChar;
                count = 1;
                prevChar = str[i];
            }
        }

        compressedStr += std::to_string(count) + prevChar;

        return compressedStr;
    }

    void AlignmentStringFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        auto &func_expr = (duckdb::BoundFunctionExpression &)state.expr;
        auto &info = (AlignmentFunctions::AlignmentStringBindData &)*func_expr.bind_info;
        auto &aligner = info.aligner;

        for (duckdb::idx_t row_i = 0; row_i < args.size(); row_i++)
        {
            auto string_value = args.data[0].GetValue(row_i);
            auto text = string_value.ToString();

            auto second_string_value = args.data[1].GetValue(row_i);
            auto pattern = second_string_value.ToString();

            int pattern_begin_free = 0;
            int pattern_end_free = 0;
            int text_begin_free = 0;
            int text_end_free = 0;

            aligner.alignEndsFree(pattern, pattern_begin_free, pattern_end_free, text, text_begin_free, text_end_free);

            auto alignment = aligner.getAlignmentCigar();
            alignment = compressCigarString(alignment);

            result.SetValue(row_i, duckdb::Value(alignment));
        }
    }

    duckdb::unique_ptr<duckdb::FunctionData> AlignmentStringBind2Arguments(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments)
    {
        auto options = WFAOptions(context, arguments);
        return duckdb::make_uniq<AlignmentFunctions::AlignmentStringBindData>();
    }

    duckdb::unique_ptr<duckdb::FunctionData> AlignmentStringBindMatchArgument(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments)
    {
        auto options = WFAOptions(context, arguments);
        return duckdb::make_uniq<AlignmentFunctions::AlignmentStringBindData>(options.mismatch, options.gap_opening, options.gap_extension, options.memory_model);
    }

    duckdb::unique_ptr<duckdb::FunctionData> AlignmentStringBindMismatchArgument(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments)
    {
        auto options = WFAOptions(context, arguments);
        return duckdb::make_uniq<AlignmentFunctions::AlignmentStringBindData>(options.match, options.mismatch, options.gap_opening, options.gap_extension, options.memory_model);
    }

    duckdb::CreateScalarFunctionInfo AlignmentFunctions::GetAlignmentStringFunction(std::string name)
    {
        duckdb::ScalarFunctionSet set(name);

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

        return duckdb::CreateScalarFunctionInfo(set);
    }

    // Score not alignment functions.

    bool AlignmentFunctions::AlignmentScoreBindData::Equals(const duckdb::FunctionData &other_p) const
    {
        return true;
    }

    duckdb::unique_ptr<duckdb::FunctionData> AlignmentFunctions::AlignmentScoreBindData::Copy() const
    {
        auto result = duckdb::make_uniq<AlignmentScoreBindData>();
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
            auto text = string_value.ToString();

            auto second_string_value = args.data[1].GetValue(row_i);
            auto pattern = second_string_value.ToString();

            // Align the pattern against the text.
            aligner.alignEnd2End(pattern, text);
            auto score = aligner.getAlignmentScore();

            result.SetValue(row_i, duckdb::Value::FLOAT(score));
        }
    }

    duckdb::unique_ptr<duckdb::FunctionData> AlignmentScoreBind(duckdb::ClientContext &context, duckdb::ScalarFunction &bound_function, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments)
    {
        if (arguments.size() == 2)
        {
            return duckdb::make_uniq<AlignmentFunctions::AlignmentScoreBindData>();
        }

        if (arguments.size() == 6)
        {
            auto options = WFAOptions(context, arguments);
            return duckdb::make_uniq<AlignmentFunctions::AlignmentScoreBindData>(options.mismatch, options.gap_opening, options.gap_extension, options.memory_model);
        }

        if (arguments.size() == 7)
        {
            auto options = WFAOptions(context, arguments);
            return duckdb::make_uniq<AlignmentFunctions::AlignmentScoreBindData>(options.match, options.mismatch, options.gap_opening, options.gap_extension, options.memory_model);
        }

        throw duckdb::InvalidInputException("Invalid number of arguments for align function");
    }

    duckdb::CreateScalarFunctionInfo AlignmentFunctions::GetAlignmentScoreFunction(std::string name)
    {
        duckdb::ScalarFunctionSet set(name);

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

        return duckdb::CreateScalarFunctionInfo(set);
    }
}
