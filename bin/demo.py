import duckdb
import pathlib
import os

data_dir = pathlib.Path(os.environ["DATA_DIR"])

con = duckdb.connect(config={'allow_unsigned_extensions': True})

con.load_extension('/wtt/wtt01.duckdb_extension')

vals = con.execute("SELECT gc_content('ATCG')").fetchall()
print(vals)

query = f"SELECT * FROM read_fasta('{data_dir / 'test.fasta'}')"
print(query)
vals = con.execute(query).fetchall()
print(vals)
