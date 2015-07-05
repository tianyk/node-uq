/**
 * config
 */
var config = {
    logDir: __dirname + '/logs/',
    redis: {
        host: '127.0.0.1',
        port: ''
    }
}

if (process.env.NODE_ENV === 'production') {
    config.redis = {}
}

module.exports = config;
