// type QueueStat struct {
//     Name string `json:"name"`
//     Type string `json:"type"`
//     Lines[] * QueueStat `json:"lines,omitempty"`
//     Recycle string `json:"recycle,omitempty"`
//     Head uint64 `json:"head"`
//     IHead uint64 `json:"ihead"`
//     Tail uint64 `json:"tail"`
//     Count uint64 `json:"count"`
// }
/**
 * QueueStat
 */

function QueueStat() {
    if (!(this instanceof QueueStat)) return new QueueStat();
}


module.export.QueueStat = QueueStat;