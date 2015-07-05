var cors = require('cors');
var express = require('express');
var router = express.Router();

var v1 = require('./v1');

router.use('/v1', cors({exposedHeaders: 'X-Uq-Id'}), v1);

module.exports = router;
