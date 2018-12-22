/*
 * ClickFlux DB
 * InfluxDB to Clickhouse Gateway
 * (C) 2018-2019 QXIP BV
 * Some Rights Reserved.
 */

var debug = true;

const clickline = require('clickline');
const ClickHouse = require('@apla/clickhouse');

const clickhouse_options = {
    host: process.env.CLICKHOUSE_SERVER || 'localhost',
    port: process.env.CLICKHOUSE_PORT || 8123,
    queryOptions: { database: process.env.CLICKHOUSE_DB || 'hepic_statistics' }
};

var clickhouse = new ClickHouse(clickhouse_options);

var express = require('express')
  , http = require('http')
  , path = require('path')
  , util = require('util');

var app = express();

function rawBody(req, res, next) {
  req.setEncoding('utf8');
  req.rawBody = '';
  req.on('data', function(chunk) {
    req.rawBody += chunk;
  });
  req.on('end', function(){
    next();
  });
}

app.set('port', process.env.PORT || 8686);
app.use(rawBody);

app.post('/write', function(req, res) {
  if (debug) console.log('RAW: ' , req.rawBody);
  if (debug) console.log('PARAMS: ', req.params);

  // Use DB from Query, if any
  if (req.param('db')) {
	clickhouse_options.queryOptions.database = req.param('db');
  	// Re-Initialize Clickhouse Client
  	clickhouse = new ClickHouse(clickhouse_options);
  }
  // Use TABLE from Query, if any
  var table = 'ts1';
  if (req.param('table')) { table = req.param('table') }

  var query = clickline(req.rawBody, table);
  if (debug) console.log('Trying.. ', query);
  clickhouse.query(query, function (err, data) {
        if (err) {
                if (debug) console.log('ERR',err);
                res.sendStatus(500);
        } else {
                if (debug) console.log(data);
                res.sendStatus(200);
        }
  });
});

http.createServer(app).listen(app.get('port'), function(){
  console.log("ClickFlux server listening on port " + app.get('port'));
});
