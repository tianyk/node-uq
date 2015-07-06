var async = require('async');
var _ = require('lodash');
var Topic = require('./topic').Topic;
var TopicStore = require('./topic').TopicStore;

var StorageKeyWord = "UnitedQueueKey";
var BgBackupInterval = 10 * 1000; // 备份间隔
var BgCleanInterval = 20 * 1000; // 清理间隔
var BgCleanTimeout = 5 * 1000; // 清理超时
var KeyTopicStore = ":store";
var KeyTopicHead = ":head";
var KeyTopicTail = ":tail";
var KeyLineStore = ":store";
var KeyLineHead = ":head";

var KeyLineInflight = ":inflight";

function UnitedQueue(storage, ip, port) {
    if (!(this instanceof UnitedQueue)) return new UnitedQueue(storage, ip, port);

    this.storage = storage;
    this.ip = ip;
    this.port = port;
}


function UnitedQueueStore(topics) {
    if (!(this instanceof UnitedQueueStore)) return new UnitedQueueStore(topics);
    this.topics = topics || [];
}


/**
 * push Data
 * @param  {[type]} key  [description]
 * @param  {[type]} data [description]
 * @return {[type]}      [description]
 */
UnitedQueue.prototype.push = function(key, data, cb) {
    key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');

    if (data.length <= 0)
        return cb(new Error('ErrBadRequest'));

    var t = this.topics[key];
    if (!t)
        return cb(new Error('ErrTopicNotExisted'));

    t.push(data, cb);
}


/**
 * pop Data
 * @param  {[type]}   key [description]
 * @param  {Function} cb  [description]
 * @return {[type]}       [description]
 */
UnitedQueue.prototype.pop = function(key, cb) {
    key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');

    var parts = key.split('/');
    if (parts.length !== 2) return cb(new Error('ErrBadKey'))

    var topicName = parts[0];
    var lineName = parts[1];

    var t = this.topics[topicName];
    if (!t) return cb(new Error('ErrTopicNotExisted'));
    t.pop(lineName, function(err, id, data) {
        if (!err) return cb(err);

        return cb(null, key + '/' + id, data);
    })
}


/**
 * [confirm description]
 * @param  {[type]}   key [description]
 * @param  {Function} cb  [description]
 * @return {[type]}       [description]
 */
UnitedQueue.prototype.confirm = function(key, cb) {
    key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');

    var topicName, lineName;
    var id;

    var parts = key.split('/');
    if (parts.length !== 3) return cb(new Error('ErrBadKey: ' + key));

    topicName = parts[0];
    lineName = parts[1];
    id = parts[2];

    var t = this.topics[topicName];
    if (!t) return cb(new Error('ErrTopicNotExisted: ' + topicName));
    t.confirm(lineName, id, cb);
}


/**
 * 创建一个Topic并持久化它
 * @param  {[type]}   name [description]
 * @param  {Function} cb   [description]
 * @return {[type]}        [description]
 */
UnitedQueue.prototype.newTopic = function(name, cb) {
    var lines = {};
    var t = Topic();
    t.name = name;
    t.lines = lines;
    t.head = 0;
    t.headKey = name + KeyTopicHead;

    t.tail = 0
    t.tailKey = name + KeyTopicTail
    t.q = this;
    // t.quit = make(chan bool)

    async.parallel([
        t.exportHead,
        t.exportTail
    ], function(err) {
        return cb(err, t);
    });
}


/**
 * 创建一个Topic，并持久化它
 * @param  {[type]} name [description]
 * @return {[type]}      [description]
 */
UnitedQueue.prototype.createTopic = function(name, cb) {
    var self = this;
    if (_.has(this.topics, name)) {
        return cb(new Error('ErrTopicExisted'));
    }

    this.newTopic(name, function(err, t) {
        if (err) return cb(err);

        self.topics[name] = t;
        self.exportQueue(function(err) {
            if (err) {
                t.remove();
                delete self.topics[name];
                return cb(err);
            }

            return cb();
        })
    });
}


/**
 * Create Topic
 * @param  {[type]}   key     [description]
 * @param  {[type]}   recycle [description]
 * @param  {Function} cb      [description]
 * @return {[type]}           [description]
 */
UnitedQueue.prototype.create = function(key, recycle, cb) {
    key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');

    var topicName, lineName;
    var parts = key.split(key, '/');
    if (parts.length < 1 || parts.length > 2) {
        return cb(new Error('ErrBadKey')); // err ErrBadKey;
    }

    topicName = parts[0];
    if (parts.length === 2) {
        lineName = parts[1];
        var t = this.topics[topicName];
        t.createLine(lineName, recycle, cb);
    } else {
        this.createTopic(topicName, cb);
    }
}


/**
 * 清空一个topic或者line
 * @param  {[type]} key [description]
 * @return {[type]}     [description]
 */
UnitedQueue.prototype.empty = function(key) {
    key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');

    var topicName, lineName;
    var parts = key.split(key, '/');
    if (parts.length < 1 || parts.length > 2) {
        return cb(new Error('ErrBadKey')); // err ErrBadKey;
    }

    topicName = parts[0];
    if (topicName.trim().length === 0) return cb(new Error('ErrBadKey'));

    var t = this.topics[topicName];
    if (!t) return cb(new Error('ErrTopicNotExisted: ' + topicName));

    if (parts.length === 2) {
        lineName = parts[1];
        return t.emptyLine(lineName, cb);
    }

    return t.empty(cb);
}


func(u * UnitedQueue) removeTopic(name string, fromEtcd bool) error {
    u.topicsLock.Lock()
    defer u.topicsLock.Unlock()

    t, ok: = u.topics[name]
    if !ok {
        return NewError(
            ErrTopicNotExisted,
            `queue remove`,
        )
    }

    delete(u.topics, name)
    err: = u.exportQueue()
    if err != nil {
        u.topics[name] = t
        return err
    }

    if !fromEtcd {
        u.unRegisterTopic(name)
    }

    return t.remove()
}

UnitedQueue.prototype.removeTopic = function(topicName) {
    var self = this;
    var t = this.topics[topicName];
    if (!t) return cb(new Error('ErrTopicNotExisted'));
    delete this.topics[topicName];

    this.exportQueue(function(err) {
        if (err) {
            self.lines[topicName] = t;
            return cb(err);
        }
        return t.remove(cb);
    })
}


/**
 * 删除Topic或者Line
 * @param  {[type]}   key [description]
 * @param  {Function} cb  [description]
 * @return {[type]}       [description]
 */
UnitedQueue.prototype.remove = function(key, cb) {
    key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');

    var topicName, lineName;
    var parts = key.split(key, '/');
    if (parts.length < 1 || parts.length > 2) {
        return cb(new Error('ErrBadKey')); // err ErrBadKey;
    }

    topicName = parts[0];
    if (topicName.trim().length === 0) return cb(new Error('ErrBadKey'));

    if (parts.length === 1) {
        return this.removeTopic(topicName, cb);
    }

    var t = this.topics[topicName];
    if (!t) return cb(new Error('ErrTopicNotExisted'));
    lineName = parts[1];
    return t.removeLine(lineName, cb);
}


/**
 * 同步方法，需要catch异常
 * @param  {[type]} key [description]
 * @return {[type]}     [description]
 */
UnitedQueue.prototype.stat = function(key) {
    key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');

    var topicName, lineName;
    var parts = key.split(key, '/');
    if (parts.length < 1 || parts.length > 2)
        throw new Error('ErrBadKey'); // err ErrBadKey;


    topicName = parts[0];
    if (topicName.trim().length === 0) throw new Error('ErrBadKey');

    var t = this.topics[topicName];
    if (!t) throw new Error('ErrTopicNotExisted');
    if parts.length === 2 {
        lineName = parts[1];
        return t.statLine(lineName);
    }

    return t.stat();
}

UnitedQueue.prototype.close = function() {}


/**
 * 基础方法 setData
 * @param {[type]}   key  [description]
 * @param {[type]}   data [description]
 * @param {Function} cb   [description]
 */
UnitedQueue.prototype.setData = function(key, data, cb) {
    this.store.setData(key, data, cb);
}


/**
 * 基础方法 getData
 * @param  {[type]}   key [description]
 * @param  {Function} cb  [description]
 * @return {[type]}       [description]
 */
UnitedQueue.prototype.getData = function(key, cb) {
    this.store.get(key, cb);
}


/**
 * 基础方法 delData
 * @param  {[type]}   key [description]
 * @param  {Function} cb  [description]
 * @return {[type]}       [description]
 */
UnitedQueue.prototype.delData = function(key, cb) {
    this.store.del(key, cb);
}


/**
 * Topic 反序列化
 * @param  {[type]}   topicName       [description]
 * @param  {[type]}   topicStoreValue [description]
 * @param  {Function} cb              [description]
 * @return {[type]}                   [description]
 */
UnitedQueue.prototype.loadTopic = function(topicName, topicStoreValue, cb) {
    var self = this;
    var t = Topic();
    t.name = topicName;
    t.u = this;

    t.headKey = topicName + KeyTopicHead;
    t.tailKey = topicName + KeyTopicTail;

    async.map([t.headKey, t.tailKey], this.getData, function(err, results) {
        if (err) return cb(err);
        if (!results || results.length !== 2) return cb(new Error('topic head and tail data missing.'))

        t.head = results[0]; // 头计数器
        t.tail = results[1]; // 尾计数器

        async.map(topicStoreValue.lines, function(lineName, cb) {
            var lineStoreKey = topicName + "/" + lineName;
            self.getData(lineStoreKey, function(err, lineStoreData) {
                if (err) return cb(err);
                if (!lineStoreData) return cb(new Error('line backup data missing: ' + lineStoreKey));

                var lineStoreValue = lineStoreData;
                t.loadLine(lineName, lineStoreValue, function(err, line) {
                    if (err) return cb(err);

                    return cb(null, line);
                });
            });
        }, function(err, lines) {
            if (err) return cb(err);

            t.lines = _.zipObject(topicStoreValue.lines, lines);
            return cb(null, t);
        });
    });
}


/**
 * Queue 反序列化
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
UnitedQueue.prototype.loadQueue = function(cb) {
    var self = this;
    this.getData(StorageKeyWord, function(err, unitedQueueStoreData) {
        if (err) return cb(err);

        if (unitedQueueStoreData) {
            var unitedQueueStoreValue;
            try {
                unitedQueueStoreValue = unitedQueueStoreData;
            } catch (e) {
                return cb(new Error('queue backup data broken: ' + unitedQueueStoreData));
            }

            async.map(unitedQueueStoreValue.topics, function(lineName, cb) {
                self.getData(topicName, function(err, topicStoreData) {
                    if (err) return cb(err);
                    if (!topicStoreData) return cb(new Error('topic backup data missing: ' + topicName));

                    var topicStoreValue;
                    try {
                        topicStoreValue = topicStoreData;
                    } catch (e) {
                        return cb(new Error('topic backup data broken: ' + topicStoreData));
                    }
                    self.loadTopic(topicName, topicStoreValue, function(err, topic) {
                        return cb(null, topic);
                    });
                });
            }, function(err, topics) {
                t.lines = _.zipObject(unitedQueueStoreValue.topics, topics);
                return cb();
            });
        }
    })
}


/**
 * Queue 序列化
 * @return {[type]} [description]
 */
UnitedQueue.prototype.genQueueStore = function() {
    var topics = [];

    for (var topicName in this.topics) {
        topics[i] = topicName;
    }

    var qs = UnitedQueueStore(topics);
    return qs;
}


/**
 * Queue 持久化
 * @param  {[type]} argument [description]
 * @return {[type]}          [description]
 */
UnitedQueue.prototype.exportQueue = function(cb) {
    var queueStoreValue = this.genQueueStore();

    var buffer = JSON.stringify(queueStoreValue);

    this.setData(StorageKeyWord, buffer, cb);
}


function NewUnitedQueue(storage, ip, port) { /*storage store.Storage, ip string, port int*/
    var uq = UnitedQueue(storage, ip, port);
    uq.topics = {};
    uq.storage = storage

    uq.loadQueue(function(err) {
        return cb(err, uq);
    })
}
