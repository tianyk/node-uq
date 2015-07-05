// Push(key string, data []byte) error
// MultiPush(key string, datas [][]byte) error
// Pop(key string) (string, []byte, error)
// MultiPop(key string, n int) ([]string, [][]byte, error)
// Confirm(key string) error
// MultiConfirm(keys []string) []error

// // admin functions
// Create(key, recycle string) error
// Empty(key string) error
// Remove(key string) error
// Stat(key string) (*QueueStat, error)
// Close()

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

UnitedQueue.prototype.push = function(key, data) {}

UnitedQueue.prototype.pop = function(key) {}

UnitedQueue.prototype.confirm = function(key) {}

UnitedQueue.prototype.create = function(key, recycle) {
    key = _.trimLeft(key, '/');
    key = _.trimRight(key, '/');


    var topicName, lineName;
    var parts = key.split(key, "/")
    if (parts.length < 1 || parts.length > 2) {
        return;
        // return NewError(
        //     ErrBadKey,
        //     `create key parts error: `+ItoaQuick(len(parts)),
        // )
    }

    topicName = parts[0];


}

UnitedQueue.prototype.empty = function(key) {}

UnitedQueue.prototype.remove = function(key) {}

UnitedQueue.prototype.stat = function(key) {}

UnitedQueue.prototype.close = function() {}


UnitedQueue.prototype.setData = function(key, data, cb) {
    this.store.setData(key, data, function(err, data) {
        return cb(err, data);
    })
}


UnitedQueue.prototype.getData = function(key, cb) {
    this.store.get(key, function(err, data) {
        return cb(err, data);
    })
}

UnitedQueue.prototype.delData = function(key, cb) {
    this.store.del(key, function(err, data) {
        return cb(err, data);
    })
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

            // _.zipObject(['fred', 'barney'], [30, 40]);
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


UnitedQueue.prototype.genQueueStore = function() {
    var topics = [];

    for (var topicName in this.topics) {
        topics[i] = topicName;
    }

    var qs = UnitedQueueStore(topics);
    return qs;
}

/**
 * 持久化（序列化）
 * @param  {[type]} argument [description]
 * @return {[type]}          [description]
 */
UnitedQueue.prototype.exportQueue = function(cb) {
    var queueStoreValue = this.genQueueStore();

    var buffer = JSON.stringify(queueStoreValue);

    this.setData(StorageKeyWord, buffer, function(err) {
        return cb(err);
    });
}

function NewUnitedQueue(storage, ip, port) { /*storage store.Storage, ip string, port int*/
    var uq = UnitedQueue(storage, ip, port);
    uq.topics = {};
    uq.storage = storage

    uq.loadQueue(function(err) {
        return cb(err, uq);
    })
}
