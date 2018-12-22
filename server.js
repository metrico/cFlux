/*
 * ClickFlux DB
 * InfluxDB to Clickhouse Gateway
 * (C) 2018-2019 QXIP BV
 * Some Rights Reserved.
 */

var debug = process.env.DEBUG || false;

const ifqlparser = require('ifql-parser')();

const clickline = require('clickline');
const ClickHouse = require('@apla/clickhouse');

const clickhouse_options = {
    host: process.env.CLICKHOUSE_SERVER || 'localhost',
    port: process.env.CLICKHOUSE_PORT || 8123,
    queryOptions: { database: process.env.CLICKHOUSE_DB || 'hepic_statistics' }
};

var clickhouse = new ClickHouse(clickhouse_options);

var createTable = function(tableName){
	if (!tableName || tables[tableName]) return;
	var query = "CREATE TABLE IF NOT EXISTS "+tableName+" (entity String, ts UInt64, m Array(String), mv Array(Float32), t Array(String), tv Array(String), d Date MATERIALIZED toDate(round(ts/1000)), dt DateTime MATERIALIZED toDateTime(round(ts/1000)) ) ENGINE = MergeTree(d, entity, 8192)";
	return query;
};

var tables = [];
var getTables = function(){
	var showTables = "show tables";
	var stream = clickhouse.query (showTables);
	stream.on ('data', function (row) {
	  if (tables.indexOf(row[0]) === -1) tables.push (row[0]);
	});
	stream.on ('error', function (err) {
		  // TODO: handler error
		  console.log('GET TABLES ERR',err);
		return false;
	});
	stream.on ('end', function () {
		console.log('TABLES:',tables);
		return tables;
	});
}
getTables();

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
  if (debug) console.log('QUERY: ', req.query);

  // Use DB from Query, if any
  if (req.query.db) {
	if (debug) console.log('DB',req.query.db )
	clickhouse_options.queryOptions.database = req.query.db;
  	// Re-Initialize Clickhouse Client
  	clickhouse = new ClickHouse(clickhouse_options);
  }

  var query = clickline(req.rawBody);
  if (query.parsed.measurement) table = query.parsed.measurement;
  if (debug) console.log('Trying.. ', query, table);
  if (!tables[table]) { 
	  clickhouse.querying(createTable(table)).then((result) => sendQuery(query.query,res,true) )
  } else {
	  sendQeury(query.query, res);
  }
});

http.createServer(app).listen(app.get('port'), function(){
  console.log("ClickFlux server listening on port " + app.get('port'));
});


var sendQuery = async function(query,res,update){
  if (debug) console.log('SHIPPING QUERY...',query);
  clickhouse.query(query, {syncParser: true}, function (err, data) {
        if (err) {
                console.log('QUERY ERR',err);
                res.sendStatus(500);
        } else {
                if (debug) console.log(data);
                res.sendStatus(200);
        }
	if (update) getTables();
  });
};

app.post('/query', function(req, res) {
  if (debug) console.log('RAW: ' , req.rawBody);
  if (debug) console.log('QUERY: ', req.query);
  var rawQuery =  req.rawBody.replace(/^q=/,'');
  rawQuery = unescape(rawQuery);
  res.send( ifqlparser.parse(rawQuery) );
});

