var duckdb = require('duckdb');

module.exports.getConnection = function () {
    const db = new duckdb.Database('/tmp/db', {
        allow_unsigned_extensions: true
    })

    return db
}
