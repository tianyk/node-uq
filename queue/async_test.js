var async = require('async');

function task(taskName, cb) {
    var timeout = 0;
    if ('task1' === taskName) timeout = 300;
    else if ('task2' === taskName) timeout = 200;
    else if('task3' === taskName) timeout = 100;

    setTimeout(function() {
        return cb(null, taskName);
    }, timeout);
}


function task1(cb) {
    setTimeout(function() {
        return cb(null, 'task1');
    }, 300);
}

function task2(cb) {
    setTimeout(function() {
        return cb(null, 'task2');
    }, 200);
}

function task3(cb) {
    setTimeout(function() {
        return cb(null, 'task3');
    }, 100);
}

var start = Date.now();
async.parallel([function(cb) {
    task1(cb);
}, function(cb) {
    task2(cb);
}, function(cb) {
    task3(cb);
}], function(err, results) {
    console.log(Date.now() - start);
    console.log(results);
});

async.map(['task1', 'task2', 'task3'], task, function(err, results) {
    console.log(err);
    console.log(results);
})
