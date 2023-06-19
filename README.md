<h1 align="center">
    <img src=".github/Duckdb+Exon.svg" width="200px" alt="exon-duckdb" />
</h1>

Exon-DuckDB is a DuckDB Extension for Exon that allows for users to use exon functionality through DuckDB.

For example, you can use the following query count the sequences in a FASTA file:

```sql
LOAD exon;

SELECT COUNT(*)
FROM read_fasta('file.fasta');
```

You can read more about how to use Exon-DuckDB in the [documentation](https://www.wheretrue.dev/docs/exon/exondb/).
