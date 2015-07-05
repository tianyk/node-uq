var redis = require('../utils/redis');

function RedisStore(opts) {
    if (!(this instanceof RedisStore)) return new RedisStore(opts);

    this.ip = opts;
    this.port = opts;

    this.db = redis.getRedisClient();
}


RedisStore.prototype.set = function(key, data, cb) {
    this.db.set(key, data, function (err, data) {
        return cb(err, data);
    })
}

RedisStore.prototype.get = function(key, cb) {
    this.db.get(key, function (err, data) {
        return cb(err, data);
    })
}

RedisStore.prototype.del = function (key, cb) {
    this.db.del(key, function (err, data) {
        return cb(err, data);
    })
}

RedisStore.prototype.close = function (key, cb) {

}