

function Line() {
    if (!(this instanceof Line)) return new Line();
}

function LineStore() {
    // Head      uint64
    // Inflights []inflightMessage
    // Ihead     uint64
}
