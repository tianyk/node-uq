var async = require('async');

function task(taskName, cb) {
    var timeout = 0;
    if ('task1' === taskName) timeout = 300;
    else if ('task2' === taskName) timeout = 200;
    else if ('task3' === taskName) timeout = 100;

    setTimeout(function() {
        return cb(null, taskName);
    }, timeout);
}


function task1(cb) {
    setTimeout(function() {
        console.log('run task3')
            // return cb(null, 'task1');
        return cb('err1')
    }, 300);
}

function task2(cb) {
    setTimeout(function() {
        console.log('run task2')
            // return cb(null, 'task2');
        return cb('err2');
    }, 200);
}

function task3(cb) {
    setTimeout(function() {
        console.log('run task1')
        return cb(null, 'task3');
        // return cb('err')
    }, 100);
}

var start = Date.now();
// async.parallel([function(cb) {
//     task1(cb);
// }, function(cb) {
//     task2(cb);
// }, function(cb) {
//     task3(cb);
// }], function(err, results) {
//     console.log(Date.now() - start);
//     console.log(results);
// });

// async.parallel([
//     task1,
//     task2,
//     task3
// ], function(err, results) {
//     console.log(err);
//     console.log(Date.now() - start);
//     console.log(results);
// });

// async.map(['task1', 'task2', 'task3'], task, function(err, results) {
//     console.log(err);
//     console.log(results);
// })



var numCPUs = require('os').cpus().length;
function worker(key, cb) {
    setTimeout(function() {
        // console.log('run ' + key)
        if ('55' === key) {
            return cb('err' + key);
        }
        return cb(null, key);
    }, 100);
}

var concurrency = numCPUs;
var queue = async.queue(worker, concurrency);
queue.drain = function () {
    console.log('invoke over.');
}

for (var i = 0; i < 100; i++) {
    var key = '' + i;
    queue.push(key, function (err, key) {
        console.log(err, key);
    });
}
