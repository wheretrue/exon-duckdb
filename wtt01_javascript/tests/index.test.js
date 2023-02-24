var wtt01 = require('wtt01');

describe('index', () => {
    it('should be true', () => {
        const db = wtt01.getConnection();

        db.all('SELECT 42 AS fortytwo', function (err, res) {
            if (err) {
                throw err;
            }
            console.log(res[0].fortytwo)
        });
    });
});
