var duckdb = require('../../duckdb/tools/nodejs');
var assert = require('assert');

describe(`wtt01 extension`, () => {
    let db;
    let conn;
    before((done) => {
        db = new duckdb.Database(':memory:');
        conn = new duckdb.Connection(db);
        done();
    });

    it('function should return expected constant', function (done) {
        db.all("SELECT gc_content('ATCG') as value;", function (err, res) {
            if (err) throw err;
            assert.deepEqual(res, [{ value: 0.5 }]);
            done();
        });
    });
});
