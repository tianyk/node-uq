var express = require('express');
var path = require('path');
var morgan = require('morgan');
var bodyParser = require('body-parser');

var config = require('./config');
var routes = require('./routes/index');

var app = express();

app.set('trust proxy', true);
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: false
}));

if (app.get('env') === 'production') {
    var FileStreamRotator = require('file-stream-rotator');
    var mkdirp = require('mkdirp');

    var logDirectory = config.logDir;
    // ensure log directory exists
    mkdirp.sync(logDirectory);
    var accessLogStream = FileStreamRotator.getStream({
        filename: logDirectory + '/access-%DATE%.log',
        frequency: 'daily',
        verbose: false, // 开发模式下打开
        date_format: 'YYYY-MM-DD'
    });
    app.use(morgan('combined', {
        stream: accessLogStream
    }));
} else {
    app.use(morgan('dev'));
}

app.use('/', routes);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

// error handlers
// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.json({
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.json({
        message: err.message,
        error: {}
    });
});


module.exports = app;
