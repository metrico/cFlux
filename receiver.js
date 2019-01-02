/*
 * ClickFlux DB
 * InfluxDB to Clickhouse Line-Protocol Gateway
 * (C) 2018-2019 QXIP BV
 * Some Rights Reserved.
 */

var debug = process.env.DEBUG || false;
var exception = process.env.EXCEPTION || false;
var tsDivide = process.env.TSDIVIDE || 1000000000;

/* DB Helper */
const ifqlparser = require('ifql-parser')();
const clickline = require('clickline');
const ClickHouse = require('@apla/clickhouse');

const clickhouse_options = {
    host: process.env.CLICKHOUSE_SERVER || 'localhost',
    port: process.env.CLICKHOUSE_PORT || 8123,
    queryOptions: { database: process.env.CLICKHOUSE_DB || 'default' }
};

var clickhouse = new ClickHouse(clickhouse_options);

/* Response Helpers */

var resp_empty = {"results":[{"statement_id":0}]};

/* Cache Helper */
var recordCache = require('record-cache');

var onStale = function(data){
 	for (let [key, value] of data.records.entries()) {
	     var statement = "INSERT INTO "+key+"(entity,ts,m,mv,t,tv)";
	     var clickStream = clickhouse.query (statement, {inputFormat: 'TSV'}, function (err) {
	       if (err) console.log('ERROR BULK',err);
	       if (debug) console.log ('Insert complete for',key);
	     });
 	     value.list.forEach(function(row){
		if (!row.record) return;
		// console.log(row.record);
		clickStream.write ( row.record );
             });
	     clickStream.end ();
        }
}

var cache = recordCache({
  maxSize: 5000,
  maxAge: 2000,
  onStale: onStale
})

/* Function Helpers */
var createTable = function(tableName){
	if (!tableName) return;
	if (tableName === 'syslog' || tableName === 'alarm' ) {
		console.log('Logs Table!');
		var query = "CREATE TABLE IF NOT EXISTS "+tableName+" (entity String, ts UInt64, m Array(String), mv Array(String), t Array(String), tv Array(String), d Date MATERIALIZED toDate(round(ts/"+tsDivide+")), dt DateTime MATERIALIZED toDateTime(round(ts/"+tsDivide+")) ) ENGINE = MergeTree(d, entity, 8192)";
	} else {
		console.log('Metrics Table!');
		var query = "CREATE TABLE IF NOT EXISTS "+tableName+" (entity String, ts UInt64, m Array(String), mv Array(Float32), t Array(String), tv Array(String), d Date MATERIALIZED toDate(round(ts/"+tsDivide+")), dt DateTime MATERIALIZED toDateTime(round(ts/"+tsDivide+")) ) ENGINE = MergeTree(d, entity, 8192)";
	}
	return query;
};

var tables = [];
var getTables = function(){
	var showTables = "show tables";
	var stream = clickhouse.query(showTables);
	stream.on ('data', function (row) {
	  if (tables.indexOf(row[0]) === -1) tables.push (row[0]);
	});
	stream.on ('error', function (err) {
		// TODO: handler error
		console.log('GET TABLES ERR',err);
		var parsed = err.toString().match(/Table\s(.*) doesn/);
                if (parsed && parsed[1]){
                   console.log('Create Table!',parsed);
                   try {
                       clickhouse.querying(createTable(parsed[1])).then((result) => console.log(result) )
                       if(res) res.sendStatus(resp_empty);
                   } catch(e) { if (res) res.sendStatus(500) }

                } else {
                        return;
                }
		return false;
	});
	stream.on ('end', function () {
		if (debug) console.log('RELOAD TABLES:',tables);
		return tables;
	});
}
getTables();

/* HTTP Helper */

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

/* Write Handler */

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

  var queries = req.rawBody.split("\n");
  queries.forEach(function(rawBody){
	  if (!rawBody || rawBody == '') return;
	  var query = clickline(rawBody.trim());
	  if (query.parsed.measurement) table = query.parsed.measurement;
	  if (tables.indexOf(table) === -1) {
		  console.log('Creating new table...',table)
		  try {
			clickhouse.querying(createTable(table))
				.then((result) => sendQuery(query,true) )
				getTables();
		  } catch(e) { console.log(e) }
	  } else {
		  sendQuery(query,false);
	  }
  });
  res.sendStatus(204);
});

http.createServer(app).listen(app.get('port'), function(){
  console.log("ClickFlux server listening on port " + app.get('port'));
});

var sendQuery = async function(query,update){
  console.log('QUERY>BULK',query);
  cache.add(query.parsed.measurement, query.values);
  if (update) getTables();
  return;
};

/* INFLUXDB PING EMULATION */
app.get('/ping', (req, res) => {
	if (debug) console.log('PING req!');
	clickhouse.pinging().then((result) => { res.sendStatus(204) } )
})

process.on('unhandledRejection', function(err, promise) {
    console.error('Error:',err);
    if (exception) console.error('Unhandled rejection (promise: ', promise, ', reason: ', err, ').');
});
