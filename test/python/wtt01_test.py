import duckdb

def test_wtt01():
    conn = duckdb.connect('');
    conn.execute("SELECT wtt01('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Wtt01 Sam 🐥");