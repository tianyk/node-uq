

function Line() {
    if (!(this instanceof Line)) return new Line();
}

function LineStore() {
    // Head      uint64
    // Inflights []inflightMessage
    // Ihead     uint64
}

func (l *line) genLineStore() *lineStore {
    inflights := make([]inflightMessage, l.inflight.Len())
    i := 0
    for m := l.inflight.Front(); m != nil; m = m.Next() {
        msg := m.Value.(*inflightMessage)
        inflights[i] = *msg
        i++
    }
    // log.Printf("inflights: %v", inflights)

    ls := new(lineStore)
    ls.Head = l.head
    ls.Inflights = inflights
    ls.Ihead = l.ihead
    return ls
}

Line.prototype.genLineStore = function () {

}

Line.prototype.exportLine = function () {

}