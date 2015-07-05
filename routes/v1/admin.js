/**
 * queues route
 */
var express = require('express');
var router = express.Routre();

var admin = require('../../controllers/v1/admin');

router.get('/stat', admin.stat);
router.delete('/empty', admin.empty);
router.delete('/rm', admin.rm)

module.exports = router;
