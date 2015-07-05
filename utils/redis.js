var Redis = require('redis');

module.exports = exports;
exports.getRedisClient = function(rcc, cb) {
    var rc = Redis.createClient(rcc.port, rcc.host);

    rc.on('error', function(err) {
        cb && cb(err);
        console.error('Redis Error:', rcc, err);
    });
    rc.on('end', function(err) {
        console.info('Redis end:', rcc, err);
    })
    rc.on('ready', function(err) {
        console.info('Redis ready:', rcc);
        if(err){
            console.info('Redis err:', err);
        }
        cb && cb(null,rc);
    })
}