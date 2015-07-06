/**
 * Line
 */
var LinkedList = require('linkedlist');
var _ = require('lodash');

var InflightMessage = require('./message').InflightMessage;

function Line() {
    if (!(this instanceof Line)) return new Line();
}


function LineStore(head, inflights, ihead) {
    if (!(this instanceof Line)) return new Line(head, inflights, ihead);

    this.head = head;
    this.inflights = inflights;
    this.ihead = ihead;
}


/**
 * Line 序列化
 * @return {[type]} [description]
 */
Line.prototype.genLineStore = function() {
    var inflights = [];
    var i = 0;
    while (this.inflights.next()) {
        inflights[i] = this.inflights.next();
        i++;
    }

    var ls = LineStore(this.head, inflights, this.ihead);
    return ls;
}


/**
 * 删除Line data
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Line.prototype.removeLineData = function(cb) {
    var lineStoreKey = this.t.name + '/' + this.name;
    this.t.q.delData(lineStoreKey, cb);
}


/**
 * Line Recycle 持久化
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Line.prototype.exportRecycle = function(cb) {
    var lineRecycleData = this.recycle;

    this.t.q.setData(this.recycleKey, lineRecycleData, cb);
}

/**
 * 删除line recycle
 * @return {[type]} [description]
 */
Line.prototype.removeRecycleData = function(cb) {
    this.t.q.delData(this.recycleKey, cb)
}

/**
 * Line 持久化
 * @return {[type]} [description]
 */
Line.prototype.exportLine = function(cb) {
    var lineStoreValue = this.genLineStore();

    var buffer = JSON.stringify(lineStoreValue);
    var lineStoreKey = this.t.name + "/" + this.name;

    this.t.q.setData(lineStoreKey, buffer, cb);
}


/**
 * 删除Line的数据
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Line.prototype.remove = function(cb) {
    async.parallel([
        l.removeLineData,
        l.removeRecycleData
    ], cb);
}


/**
 * pop Data
 * @param  {Function} cb [description]
 * @return {[type]}      [description]
 */
Line.prototype.pop = function(cb) {
    var self = this;
    var now = _.now();

    // 先从inflight中检查
    if (this.recycle > 0) {
        var m = this.inflight.pop();
        if (m) {
            var msg = m;
            if (now > msg.exptime) {
                msg.exptime = now + self.recycle;
                self.t.getData(msg.tId, function(err, data) {
                    if (err) return cb(err);

                    // self.inflight.remove(m);
                    while (self.inflight.next()) {
                        if (self.inflight.current.tId === m.tId) {
                            self.inflight.removeCurrent();
                            break;
                        }
                    }
                    self.inflight.resetCursor();

                    self.inflight.push(msg);
                    return cb(null, msg.tId, data);
                })
            }
        }
    }

    var tId = this.head;

    var topicTail = this.t.getTail();
    if (this.tail >= topicTail) return cb(new Error('ErrNone'));

    this.t.getData(tId, function(err, data) {
        if (err) return cb(err);

        this.head++;
        if (this.recycle > 0) {
            var msg = InflightMessage(tId, now + self.recycle);
            self.inflight.push(msg);

            self.imap[tId] = true;
        }
        return cb(null, tId, data);
    })
}

Line.prototype.updateiHead = function() {
    var id;
    var fl;
    for (; l.ihead < l.head;) {
        id = this.ihead;
        fl = this.imap[id];

        if (fl) {
            return;
        } else {
            delete this.imap[id];
            this.ihead++;
        }
    }
}


Line.prototype.confirm = function(id, cb) {
    if (l.recycle === 0) return cb(new Error('ErrNotDelivered'));

    var head = l.head;
    if (id >= head) return cb(new Error('ErrNotDelivered'));

    var m, msg;
    while (this.inflight.next()) {
        m = this.inflight.current;
        msg = m;
        if (msg.tId === id) {
            this.inflight.removeCurrent();
            this.inflight.resetCursor();

            this.imap[id] = false;
            this.updateiHead();

            return cb();
        }
    }

    return cb(new Error('ErrNotDelivered'));
}
