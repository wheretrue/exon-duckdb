#include <duckdb.hpp>

#include <string>
#include <map>

#include "sequence_functions.hpp"

namespace wtt01
{
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
        result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
        // result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);

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
        return functions;
    }
}
