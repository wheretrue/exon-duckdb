#include <duckdb.hpp>

#include <string>
#include <map>

#include "bindings/cpp/WFAligner.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "sequence_functions.hpp"

using duckdb::LogicalType;

// Get the WFA2_ENABLED compile flag
#ifdef WFA2_ENABLED
#define WFA2_ENABLED_BOOL true
#else
#define WFA2_ENABLED_BOOL false
#endif

namespace wtt01
{

// Don't enable if WF2 is not enabled
#if WFA2_ENABLED_BOOL
    bool SequenceFunctions::AlignBindData::Equals(const duckdb::FunctionData &other_p) const
    {
        // TODO: implement this or add the underlying accessors
        return true;
    }

    std::unique_ptr<duckdb::FunctionData> SequenceFunctions::AlignBindData::Copy() const
    {
        auto result = duckdb::make_unique<AlignBindData>();
        result->aligner = aligner;

        return std::move(result);
    }

    void AlignFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        auto &func_expr = (duckdb::BoundFunctionExpression &)state.expr;
        auto &info = (SequenceFunctions::AlignBindData &)*func_expr.bind_info;
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
            return duckdb::make_unique<SequenceFunctions::AlignBindData>();
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

            return duckdb::make_unique<SequenceFunctions::AlignBindData>(mismatch, 6, 2, scope, model);
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

            return duckdb::make_unique<SequenceFunctions::AlignBindData>(match, mismatch, 6, 2, scope, model);
        }

        throw duckdb::InvalidInputException("Invalid number of arguments for align function");
    }

    duckdb::CreateScalarFunctionInfo SequenceFunctions::GetAlignFunction()
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

        return duckdb::CreateScalarFunctionInfo(set);
    }
#endif

    void ReverseComplementFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

        for (duckdb::idx_t row_i = 0; row_i < args.size(); row_i++)
        {

            auto string_value = args.data[0].GetValue(row_i);
            auto sequence = string_value.ToString();

            std::string reverse_complement;

            int size = sequence.size();

            for (int i = 0; i < size; i++)
            {
                if (sequence[i] == 'A')
                {
                    reverse_complement += 'C';
                }
                else if (sequence[i] == 'T')
                {
                    reverse_complement += 'G';
                }
                else if (sequence[i] == 'C')
                {
                    reverse_complement += 'A';
                }
                else if (sequence[i] == 'G')
                {
                    reverse_complement += 'T';
                }
                else
                {
                    throw duckdb::InvalidInputException(std::string("Invalid character in sequence: ") + sequence[i]);
                }
            }
            result.SetValue(row_i, duckdb::Value(reverse_complement));
        }
    }

    duckdb::CreateScalarFunctionInfo SequenceFunctions::GetReverseComplementFunction()
    {
        duckdb::ScalarFunctionSet set("reverse_complement");

        auto reverse_complement_scalar_function = duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::VARCHAR, ReverseComplementFunction);
        set.AddFunction(reverse_complement_scalar_function);

        return duckdb::CreateScalarFunctionInfo(set);
    }

    void ComplementFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

        for (duckdb::idx_t row_i = 0; row_i < args.size(); row_i++)
        {

            auto string_value = args.data[0].GetValue(row_i);
            auto sequence = string_value.ToString();

            std::string complement;

            int size = sequence.size();

            for (int i = 0; i < size; i++)
            {
                if (sequence[i] == 'A')
                {
                    complement += 'T';
                }
                else if (sequence[i] == 'T')
                {
                    complement += 'A';
                }
                else if (sequence[i] == 'C')
                {
                    complement += 'G';
                }
                else if (sequence[i] == 'G')
                {
                    complement += 'C';
                }
                else
                {
                    throw duckdb::InvalidInputException("Invalid character in sequence: %c", sequence[i]);
                }
            }

            result.SetValue(row_i, duckdb::Value(complement));
        }
    }

    duckdb::CreateScalarFunctionInfo SequenceFunctions::GetComplementFunction()
    {
        duckdb::ScalarFunctionSet set("complement");
        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::VARCHAR, ComplementFunction));

        return duckdb::CreateScalarFunctionInfo(set);
    }

    void GcContent(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        // result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
        result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);

        for (duckdb::idx_t i = 0; i < args.size(); i++)
        {
            auto string_value = args.data[0].GetValue(i);
            auto ss = string_value.ToString();

            int size = ss.size();
            if (size == 0)
            {
                result.SetValue(i, duckdb::Value::FLOAT(0));
                return;
            }

            int gc_count = 0;
            for (int i = 0; i < size; i++)
            {
                if (ss[i] == 'G' || ss[i] == 'C')
                {
                    gc_count++;
                }
            }
            result.SetValue(i, duckdb::Value::FLOAT((float)gc_count / (float)size));
        }
    }

    duckdb::CreateScalarFunctionInfo SequenceFunctions::GetGCContentFunction()
    {
        duckdb::ScalarFunctionSet set("gc_content");
        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::FLOAT, GcContent));

        return duckdb::CreateScalarFunctionInfo(set);
    }

    void ReverseTranscribeRnaToDna(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

        for (duckdb::idx_t row_i = 0; row_i < args.size(); row_i++)
        {

            auto string_value = args.data[0].GetValue(row_i);
            auto sequence = string_value.ToString();
            int size = sequence.size();

            std::string reversed_transcribed_sequence;
            for (int i = 0; i < size; i++)
            {
                if (sequence[i] == 'U')
                {
                    reversed_transcribed_sequence += 'T';
                }
                else if (sequence[i] == 'A')
                {
                    reversed_transcribed_sequence += 'A';
                }
                else if (sequence[i] == 'C')
                {
                    reversed_transcribed_sequence += 'C';
                }
                else if (sequence[i] == 'G')
                {
                    reversed_transcribed_sequence += 'G';
                }
                else
                {
                    throw duckdb::InvalidInputException(std::string("Invalid character in sequence: ") + sequence[i]);
                }
            }
            result.SetValue(row_i, duckdb::Value(reversed_transcribed_sequence));
        }
    }

    duckdb::CreateScalarFunctionInfo SequenceFunctions::GetReverseTranscribeRnaToDnaFunction()
    {
        duckdb::ScalarFunctionSet set("reverse_transcribe");
        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::VARCHAR, ReverseTranscribeRnaToDna));

        return duckdb::CreateScalarFunctionInfo(set);
    }

    void TranscribeDnaToRnaFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

        for (duckdb::idx_t row_i = 0; row_i < args.size(); row_i++)
        {
            auto string_value = args.data[0].GetValue(row_i);
            auto sequence = string_value.ToString();

            std::string transcribed_sequence;

            int size = sequence.size();

            for (int i = 0; i < size; i++)
            {
                if (sequence[i] == 'T')
                {
                    transcribed_sequence += 'U';
                }
                else if (sequence[i] == 'A')
                {
                    transcribed_sequence += 'A';
                }
                else if (sequence[i] == 'C')
                {
                    transcribed_sequence += 'C';
                }
                else if (sequence[i] == 'G')
                {
                    transcribed_sequence += 'G';
                }
                else
                {
                    throw duckdb::InvalidInputException(std::string("Invalid character in sequence: ") + sequence[i]);
                }
            }
            result.SetValue(row_i, duckdb::Value(transcribed_sequence));
        }
    }

    duckdb::CreateScalarFunctionInfo SequenceFunctions::GetTranscribeDnaToRnaFunction()
    {
        duckdb::ScalarFunctionSet set("transcribe");
        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::VARCHAR, TranscribeDnaToRnaFunction));

        return duckdb::CreateScalarFunctionInfo(set);
    }

    void TranslateDnaToAminoAcid(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result)
    {
        result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
        std::map<std::string, std::string> standard_dna_codon_table = {
            {"AAA", "K"},
            {"AAT", "N"},
            {"AAC", "N"},
            {"AAG", "K"},
            {"ATA", "I"},
            {"ATT", "I"},
            {"ATC", "I"},
            {"ATG", "M"},
            {"ACA", "T"},
            {"ACT", "T"},
            {"ACC", "T"},
            {"ACG", "T"},
            {"AGA", "R"},
            {"AGT", "S"},
            {"AGC", "S"},
            {"AGG", "R"},
            {"TAA", "*"},
            {"TAT", "Y"},
            {"TAC", "Y"},
            {"TAG", "*"},
            {"TTA", "L"},
            {"TTT", "F"},
            {"TTC", "F"},
            {"TTG", "L"},
            {"TCA", "S"},
            {"TCT", "S"},
            {"TCC", "S"},
            {"TCG", "S"},
            {"TGA", "*"},
            {"TGT", "C"},
            {"TGC", "C"},
            {"TGG", "W"},
            {"CAA", "Q"},
            {"CAT", "H"},
            {"CAC", "H"},
            {"CAG", "Q"},
            {"CTA", "L"},
            {"CTT", "L"},
            {"CTC", "L"},
            {"CTG", "L"},
            {"CCA", "P"},
            {"CCT", "P"},
            {"CCC", "P"},
            {"CCG", "P"},
            {"CGA", "R"},
            {"CGT", "R"},
            {"CGC", "R"},
            {"CGG", "R"},
            {"GAA", "E"},
            {"GAT", "D"},
            {"GAC", "D"},
            {"GAG", "E"},
            {"GTA", "V"},
            {"GTT", "V"},
            {"GTC", "V"},
            {"GTG", "V"},
            {"GCA", "A"},
            {"GCT", "A"},
            {"GCC", "A"},
            {"GCG", "A"},
            {"GGA", "G"},
            {"GGT", "G"},
            {"GGC", "G"},
            {"GGG", "G"}};

        for (duckdb::idx_t row_i = 0; row_i < args.size(); row_i++)
        {

            auto string_value = args.data[0].GetValue(row_i);
            auto sequence = string_value.ToString();

            std::string amino_acid_sequence;

            int size = sequence.size();

            if (size % 3 != 0)
            {
                throw duckdb::InvalidInputException(std::string("Invalid sequence length: ") + std::to_string(size));
            }

            int aa_size = size / 3;

            for (int i = 0; i < aa_size; i++)
            {
                std::string codon = sequence.substr(i * 3, 3);

                if (standard_dna_codon_table.find(codon) == standard_dna_codon_table.end())
                {
                    throw duckdb::InvalidInputException(std::string("Invalid codon: ") + codon);
                }
                amino_acid_sequence += standard_dna_codon_table[codon];
            }

            result.SetValue(row_i, duckdb::Value(amino_acid_sequence));
        }
    }

    duckdb::CreateScalarFunctionInfo SequenceFunctions::GetTranslateDnaToAminoAcidFunction()
    {
        duckdb::ScalarFunctionSet set("translate_dna_to_aa");
        set.AddFunction(duckdb::ScalarFunction({duckdb::LogicalType::VARCHAR}, duckdb::LogicalType::VARCHAR, TranslateDnaToAminoAcid));

        return duckdb::CreateScalarFunctionInfo(set);
    }

    std::vector<duckdb::CreateScalarFunctionInfo> SequenceFunctions::GetSequenceFunctions()
    {
        std::vector<duckdb::CreateScalarFunctionInfo> functions;
        functions.push_back(GetTranscribeDnaToRnaFunction());
        functions.push_back(GetTranslateDnaToAminoAcidFunction());
        functions.push_back(GetGCContentFunction());
        functions.push_back(GetReverseComplementFunction());
        functions.push_back(GetComplementFunction());
        functions.push_back(GetReverseTranscribeRnaToDnaFunction());

#ifdef WFA2_ENABLED
        functions.push_back(GetAlignFunction());
#endif

        return functions;
    }
}
