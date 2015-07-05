/**
 * topic
 */
var LinkedList = require('linkedlist');

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
    l.recycleKey = this.name + "/" + lineName + KeyLineRecycle;

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
