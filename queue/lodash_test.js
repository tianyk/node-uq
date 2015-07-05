var _ = require('lodash');

var key = '/hello/world/';
key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');
console.log(key.split('/'));