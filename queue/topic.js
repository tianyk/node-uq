/**
 * topic
 */

var async = require('async');
var _ = require('lodash');
var LinkedList = require('linkedlist');
var numCPUs = require('os').cpus().length;

var Line = require('./line');

var KeyLineRecycle = ":recycle";

function Topic() {
    if (!(this instanceof Topic)) return new Topic();
}

module.export.Topic = Topic;

/**
 * Topic 序列化
 * @param {[type]} lines [description]
 */
function TopicStore(lines) {
    if (!(this instanceof TopicStore)) return new TopicStore(lines);
    this.lines = lines || [];
}

module.export.TopicStore = TopicStore;



/**
 * getData
 * @param  {[type]}   id [description]
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.getData = function(id, cb) {
    var key = this.name + ":" + id;
    this.q.getData(key, cb)
}


/**
 * setData
 * @param {[type]}   id   [description]
 * @param {[type]}   data [description]
 * @param {Function} cb   [description]
 */
Topic.prototype.setData = function(id, data, cb) {
    var key = this.name + ':' + id;
    this.q.setData(key, cb);
}


/**
 * getHead
 * @return {[type]} [description]
 */
Topic.prototype.getHead = function() {
    return this.head;
}


/**
 * getTail
 * @param  {[type]} argument [description]
 * @return {[type]}          [description]
 */
Topic.prototype.getTail = function(argument) {
    return this.tail;
}


/**
 * Line 反序列化
 * @param  {[type]}   lineName       [description]
 * @param  {[type]}   lineStoreValue [description]
 * @param  {Function} cb             [description]
 * @return {[type]}                  [description]
 */
Topic.prototype.loadLine = function(lineName, lineStoreValue, cb) {
    var self = this;

    var l = Line();
    l.t = this;
    l.name = lineName;
    l.recycleKey = this.name + '/' + lineName + KeyLineRecycle;

    this.q.getData(l.recycleKey, function(err, lineRecycleData) {
        if (err) return cb(err);

        var lineRecycle = lineRecycleData;
        l.recycle = lineRecycle;
        l.head = lineStoreValue.Head;
        l.ihead = lineStoreValue.Ihead;

        var imap = {};
        for (var i = l.ihead; i < l.head; i++) {
            imap[i] = false;
        }
        l.imap = imap;

        var inflight = new LinkedList();
        for (var i = 0; i < lineStoreValue.Inflights.length; i++) {
            var msg = lineStoreValue.Inflights.length[i];
            inflight.push(msg);
            imap[msg.tId] = true;
        }
        l.inflight = inflight;

        return cb(err, l);
    });
}


/**
 * 创建一条Line并持久化它
 * @param  {[type]}   name    [description]
 * @param  {[type]}   recycle [description]
 * @param  {Function} cb      [description]
 * @return {[type]}           [description]
 */
Topic.prototype.newLine = function(name, recycle, cb) {
    var self = this;
    var inflight = new LinkedList();
    var imap = {};
    l = Line();
    l.name = name;
    l.head = self.head;
    l.recycle = recycle;
    l.recycleKey = self.name + '/' + name + KeyLineRecycle;
    l.inflight = inflight;
    l.ihead = self.head;
    l.imap = imap;
    l.t = self;

    async.parallel([
        function(cb) {
            l.exportLine(cb);
        },
        function(cb) {
            l.exportRecycle(cb)
        }
    ], function(err) {
        return cb(err, l);
    });
}


/**
 * 持久化Topic Head
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.exportHead = function(cb) {
    var topicHeadData = this.head;
    this.q.setData(this.headKey, topicHeadData, cb);
}


/**
 * 删除topic head data
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.removeHeadData = function(cb) {
    this.q.delData(this.headKey, cb);
}


/**
 * 持久化Topic tail
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.exportTail = function(cb) {
    var topicTailData = t.tail;

    this.q.setData(this.tailKey, topicTailData, cb)
}


/**
 * 删除Topic tail data
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.removeTailData = function(cb) {
    this.q.delData(this.tailKey, cb);
}


/**
 * Topic 序列化
 * @return {[type]} [description]
 */
Topic.prototype.genTopicStore = function() {
    var lines = [];
    var line;
    for (var i = 0; i < this.lines.length; i++) {
        line = this.lines[i];
        lines[i] = line;
        i++;
    }

    var ts = TopicStore(lines);
    return ts;
}


/**
 * 创建一条Line，并持久化它及Topic
 * @param  {[type]}   name    [description]
 * @param  {[type]}   recycle [description]
 * @param  {Function} cb      [description]
 * @return {[type]}           [description]
 */
Topic.prototype.createLine = function(name, recycle, cb) {
    if (_.has(this.lines, name)) {
        return // error ErrLineExisted
    }

    this.newLine(name, recycle, function(err, line) {
        if (err) return cb(err);
        this.lines[name] = line;

        this.exportTopic(function(err) {
            if (err) {
                delete this.lines[name];
                reutrn cb(err);
            }
            return cb();
        })
    })
}


/**
 * 删除Topic Lines
 * @param  {[type]}   lineName [description]
 * @param  {Function} cb       [description]
 * @return {[type]}            [description]
 */
Topic.prototype.removeLines = function(lineName, cb) {
    var self = this;
    async.map(this.lines, function(l, cb) {
        l = self.ines[lineName];
        delete self.lines[lineName];
        l.remove(cb);
    }, cb);
}


/**
 * Topic 持久化
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.exportTopic = function(cb) {
    var topicStoreValue = this.genTopicStore();

    var buffer = JSON.stringify(topicStoreValue);
    this.q.setData(this.name, buffer, cb);
}

/**
 * 删除Topic数据
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.removeTopicData = function(cb) {
    this.q.delData(this.name, cb);
}


/**
 * 删除持久化的消息
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.removeMsgData = function(cb) {
    var self = this;

    function worker(key, cb) {
        self.q.delData(key, function(err) {
            cb(err, key);
        });
    }

    var concurrency = numCPUs; // 并发度
    var queue = async.queue(worker, concurrency);
    queue.drain = cb; // 完工事件的处理

    for (var i = this.head; i < this.tail; i++) {
        var key = this.name + ':' + i;
        queue.push(key, function(err, key) {
            if (err) {
                // log.Printf("topic[%s] del data[%s] error; %s", t.name, key, err);
                console.error(err, key);
            }
        });
    }
}


/**
 * 删除Topic
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.remove = function(cb) {
    var self = this;
    async.parallel([
        function(cb) {
            self.removeLines(cb);
        },
        function(cb) {
            self.removeHeadData(cb);
        },
        function(cb) {
            self.removeTailData(cb);
        },
        function(cb) {
            self.removeTopicData(cb);
        },
        function(cb) {
            self.removeMsgData(cb)
        }
    ], function(err) {
        return cb && cb(err);
    });
}


/**
 * push Data
 * @param  {[type]}   data [description]
 * @param  {Function} cb   [description]
 * @return {[type]}        [description]
 */
Topic.prototype.push = function(data, cb) {
    var self = this;
    var key = this.name + ':' + this.tail;

    this.q.setData(key, data, function(err) {
        if (err) return cb(err);
        self.tail++;

        self.exportTail(function(err) {
            if (err) {
                self.tail--;
                return cb(err);
            }
            return cb();
        })
    })
}


/**
 * pop Data
 * @param  {[type]}   LineName [description]
 * @param  {Function} cb       [description]
 * @return {[type]}            [description]
 */
Topic.prototype.pop = function(LineName, cb) {
    var l = this.lines[LineName];
    if (!l) return cb(new Error('ErrLineNotExisted'));
    l.pop(cb);
}


/**
 * [confirm description]
 * @param  {[type]}   lineName [description]
 * @param  {[type]}   id       [description]
 * @param  {Function} cb       [description]
 * @return {[type]}            [description]
 */
Topic.prototype.confirm = function(lineName, id, cb) {
    var l = this.lines[lineName];
    if (!l) return cb(new Error('ErrLineNotExisted: ' + lineName));
    l.confirm(id, cb);
}


/**
 * 清空一个line
 * @param  {[type]}   lineName [description]
 * @param  {Function} cb       [description]
 * @return {[type]}            [description]
 */
Topic.prototype.emptyLine = function(lineName, cb) {
    var l = this.lines[lineName];
    if (!l) return cb(new Error('ErrLineNotExisted'));
    l.empty(cb);
}


/**
 * [empty description]
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Topic.prototype.empty = function(cb) {
    async.map(this.lines, function(l, cb) {
        l.empty(cb);
    }, function(err) {
        if (err) return cb(err);

        // 重置head
        this.head = this.tail;
        this.exportHead(cb);
    })
}


/**
 * Line Stat
 * @param  {[type]}   lineName [description]
 * @param  {Function} cb       [description]
 * @return {[type]}            [description]
 */
Topic.prototype.statLine = function(lineName, cb) {
    var l = this.lines[lineName];
    if (!l) throw new Error('ErrLineNotExisted: ' + lineName);
    return l.stat();
}
