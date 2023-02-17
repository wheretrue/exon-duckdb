import duckdb

def test_wtt01():
    conn = duckdb.connect('');
    conn.execute("SELECT gc_content('ATCG') as value;");
    res = conn.fetchall()
    assert(res[0][0] == .5)
