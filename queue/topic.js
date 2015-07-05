/**
 * topic
 */
var LinkedList = require('linkedlist');
var _ = require('lodash');

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


Topic.prototype.newLine = function (name, recycle) {
    var self = this;
    var inflight = new LinkedList();
    var imap = {};
    l = Line();
    l.name = name;
    l.head = self.head;
    l.recycle = recycle;
    l.recycleKey = self.name + '' + name + KeyLineRecycle;
    l.inflight = inflight;
    l.ihead = self.head;
    l.imap = imap;
    l.t = self;

    l.exportLine();
}

func (t *topic) newLine(name string, recycle time.Duration) (*line, error) {
    inflight := list.New()
    imap := make(map[uint64]bool)
    l := new(line)
    l.name = name
    l.head = t.head
    l.recycle = recycle
    l.recycleKey = t.name + "/" + name + KeyLineRecycle
    l.inflight = inflight
    l.ihead = t.head
    l.imap = imap
    l.t = t

    // 持久化line
    err := l.exportLine()
    if err != nil {
        return nil, err
    }
    // 持久化确认机制
    err = l.exportRecycle()
    if err != nil {
        return nil, err
    }

    return l, nil
}

Topic.prototype.createLine = function (name, recycle) {
    if (_.has(this.lines, name)) {
        return // error
    }

    var l = this.newLine()
}
/**
 * new Line
 * 持久化line
 */
func (t *topic) createLine(name string, recycle time.Duration, fromEtcd bool) error {
    t.linesLock.Lock()
    defer t.linesLock.Unlock()
    _, ok := t.lines[name]
    if ok {
        return NewError(
            ErrLineExisted,
            `topic createLine`,
        )
    }

    l, err := t.newLine(name, recycle)
    if err != nil {
        return err
    }

    t.lines[name] = l

    err = t.exportTopic()
    if err != nil {
        t.linesLock.Lock()
        delete(t.lines, name)
        t.linesLock.Unlock()
        return err
    }

    if !fromEtcd {
        t.q.registerLine(t.name, l.name, l.recycle.String())
    }

    log.Printf("topic[%s] line[%s:%v] created.", t.name, name, recycle)
    return nil
}
