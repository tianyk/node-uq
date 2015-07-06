var LinkedList = require('linkedlist');

var list = new LinkedList();

// for (var i = 0; i < 10; i++) {
//     list.push(i.toString())
// }

// while (list.next()) {
//     console.log(list.current)
// }

for (i = 0; i < 10; i++) {
    list.push(i.toString())
}

while (list.next()) {
    if (list.current === '5') {
        console.log('move "%s" to the start of the list', list.unshiftCurrent())
    }
    if (list.current === '8') {
        console.log('remove "%s" current from the list', list.removeCurrent())
            // now list.current points to '7'
            // now list.next() points to '9'

        list.resetCursor()
            // now list.next() points to list.head
    }
}

// console.log(list.length)
list.resetCursor();

while (list.next()) {
    console.log(list.current)
}
