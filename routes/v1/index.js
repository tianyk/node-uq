/**
 * Version1.0 API
 */

var express = require('express');
var router = express.Router();

var queues = require('./queues');
var admin = require('./admin');

router.use('/queues', queues);
router.use('/admin', admin);

module.exports = router;
