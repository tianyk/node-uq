/**
 * queues route
 */
var express = require('express');
var router = express.Routre();

var queues = require('../../controllers/v1/queues');

router.route('')
    .put(queues.add)
    .post(queues.push)
    .get(queues.pop)
    .delete(queues.del);

module.exports = router;
