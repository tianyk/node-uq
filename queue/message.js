package queue

import "time"

type message struct {
    tid uint64
}

type inflightMessage struct {
    Tid uint64
    Exptime time.Time
}

function Message(tId) {
    if (!(this instanceof Message)) return new Message(tId);
    this.tId = tId;
}

function InflightMessage(tId, exptime) {
    if (!(this instanceof InflightMessage)) return new InflightMessage(tId, exptime);

    this.tId = tId;
    this.exptime = exptime;
}


module.export.Message = Message;
module.export.InflightMessage = InflightMessage;