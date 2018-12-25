/*
 * ClickFlux DB
 * InfluxDB to Clickhouse Gateway
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
	var query = "CREATE TABLE IF NOT EXISTS "+tableName+" (entity String, ts UInt64, m Array(String), mv Array(Float32), t Array(String), tv Array(String), d Date MATERIALIZED toDate(round(ts/"+tsDivide+")), dt DateTime MATERIALIZED toDateTime(round(ts/"+tsDivide+")) ) ENGINE = MergeTree(d, entity, 8192)";
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
	  var query = clickline(rawBody);
	  if (query.parsed.measurement) table = query.parsed.measurement;
	  if (tables.indexOf(table) === -1) { 
		  console.log('Creating new table...',table)
		  try {
			clickhouse.querying(createTable(table))
				.then((result) => sendQuery(query.query,false,true) )
			getTables();
		  } catch(e) { console.log(e) }
	  } else {
		  sendQuery(query.query);
	  }
	  cache.add(query.parsed.measurement, query.values);
  });
  res.sendStatus(204);
});

http.createServer(app).listen(app.get('port'), function(){
  console.log("ClickFlux server listening on port " + app.get('port'));
});

var sendQuery = async function(query,res,update){
  if (update) getTables();
  return;

  // to be decommissioned?

  if (query.includes('undefined')) return;
  if (debug) console.log('SHIPPING QUERY...',query,res,update);
  clickhouse.query(query, {syncParser: true}, function (err, data) {
        if (err) {
                console.log('QUERY ERR',err.toString(),query);
		var parsed = err.toString().match(/Table\s(.*) doesn/);
               	if (parsed && parsed[1]){
               	 console.log('Create Table and retry!',parsed);
                 try {
                       clickhouse.querying(createTable(parsed[1])).then((result) => sendQuery(query.query,res,true) )
                       if(res) res.sendStatus(204);
                 } catch(e) { if (res) res.sendStatus(500) }
               	} else {
			return;
		}
        } else {
                if (debug) console.log(data);
                if (res) res.sendStatus(204);
        }
	if (update) getTables();
  });
};

/* Query Handlers */

var databases = [];
app.all('/query', function(req, res) {
  if (debug) console.log('QUERY:', req.query.q, req.rawBody);
	if (req.query.q && req.query.q.includes('GROUP BY ')) req.query.q = req.query.q.replace(/GROUP BY time.*\)/, " FILL(null)");
	if (req.rawBody && req.rawBody.includes('GROUP BY ')) req.rawBody = req.rawBody.replace(/GROUP BY time.*\)/, " FILL(null)");

  var rawQuery;
  try {
	  if(req.query.q) { rawQuery = req.query.q; }
          else if(req.rawBody) { rawQuery =  unescape( req.rawBody.replace(/^q=/,'').replace(/\+/g,' ') ); }

          if (rawQuery.startsWith('CREATE DATABASE')) {

		console.log('TRYING... ',req.query);
		if (req.query.db && req.query.db != "") {
			var db = req.query.db.replace(".","");
		} else if (req.query.q) {
			var db = req.query.q.match(/CREATE DATABASE \"?([^\s]*)\"?\s?/)[1] || false;	
		}
		if (db) {
	                 console.log('Create Database!',db);
	                 try {
	                       clickhouse.querying('CREATE DATABASE "'+db+'"').then((result) => console.log(result) )
	                       if(res) res.send(resp_empty);
	                 } catch(e) { 
				console.error(e);
				if (res) res.sendStatus(500) 
			 }

		} else {
			console.log('No Database Name!');
			res.sendStatus(204);
		}

          } else if (rawQuery.startsWith('SHOW RETENTION')) {
		var data = { "results": [] };
		databases.forEach(function(db,i){
	  	    data.results.push({
		      "statement_id": i,
		      "series": [
		        {
		          "columns": [
		            "name",
		            "duration",
		            "shardGroupDuration",
		            "replicaN",
		            "default"
		          ],
		          "values": [
		            [
		              "autogen",
		              "0s",
		              "168h0m0s",
		              1,
		              true
		            ]
		          ]
		        }
		      ]
		    });

		});
		res.send(data);

          } else if (rawQuery.startsWith('SHOW FIELD KEYS')) {

		var parsed = rawQuery.match(/SHOW FIELD KEYS FROM "(.*)"."(.*)"/);
		if (parsed && parsed[1] && parsed[2]){
			if (debug) console.log('get fields for',parsed[2],req.query.db);
			var response = [];
			clickhouse_options.queryOptions.database = req.query.db;
		  	// Re-Initialize Clickhouse Client
		  	var tmp = new ClickHouse(clickhouse_options);
			var stream = tmp.query("SELECT DISTINCT m FROM "+parsed[2]+" ARRAY JOIN m");
			stream.on ('data', function (row) {
			  	response.push ([row[0],"float"]);
			});
			stream.on ('error', function (err) {
				// TODO: handler error
				console.error('GET DATA ERR',err);
			});
			stream.on ('end', function () {
				var results = {"results":[{"statement_id":0,"series":[{"name":parsed[2],"columns":["fieldKey","fieldType"],"values":response }]}]};
				res.send(results);
			});

		}

          } else if (rawQuery.startsWith('SHOW TAG KEYS')) {

		var parsed = rawQuery.match(/SHOW TAG KEYS FROM \"(.*)\"\.\"(.*)\"\s?/);
		if (parsed && parsed[1] && parsed[2]){
			if (debug) console.log('get fields for',parsed[2],req.query.db);
			var response = [];
			clickhouse_options.queryOptions.database = req.query.db;
		  	// Re-Initialize Clickhouse Client
		  	var tmp = new ClickHouse(clickhouse_options);
			var stream = tmp.query("SELECT uniq_pair.1 AS k, uniq_pair.2 AS v FROM (SELECT groupUniqArray((t, tv)) AS uniq_pair FROM "+parsed[2]+" ARRAY JOIN t, tv) ARRAY JOIN uniq_pair");
			stream.on ('data', function (row) {
			  response.push ([row[0],row[1]]);
			});
			stream.on ('error', function (err) {
				// TODO: handler error
				console.error('GET DATA ERR',err);
			});
			stream.on ('end', function () {
				var results = {"results":[{"statement_id":0,"series":[{"name":parsed[2],"columns":["key","value"],"values":results }]}]}
				res.send(results);
			});

		}

          } else if (rawQuery.startsWith('SHOW TAG VALUES FROM')) {

		var parsed = rawQuery.match(/SHOW TAG VALUES FROM \"(.*)\"\.\"(.*)\"\s?/);
		if (parsed && parsed[1] && parsed[2]){
			if (debug) console.log('get tag values for',parsed[2],req.query.db);
			var response = [];
			clickhouse_options.queryOptions.database = req.query.db;
		  	// Re-Initialize Clickhouse Client
		  	var tmp = new ClickHouse(clickhouse_options);
			var stream = tmp.query("SELECT uniq_pair.1 AS k, uniq_pair.2 AS v FROM (SELECT groupUniqArray((t, tv)) AS uniq_pair FROM "+parsed[2]+" ARRAY JOIN t, tv) ARRAY JOIN uniq_pair");
			stream.on ('data', function (row) {
			  	response.push( { name: row[0], columns: ['key','value'], values: [ [row[0], row[1] ] ] } );
			});
			stream.on ('error', function (err) {
				// TODO: handler error
				console.error('GET DATA ERR',err);
			});
			stream.on ('end', function () {
				var results = {"results":[{"statement_id":0,"series":response }]};
				res.send(results);
			});

		}

          } else if (rawQuery.startsWith('SHOW MEASUREMENTS')) {
		if (req.query.db) {
			if (debug) console.log('get measurements for',req.query.db);
			var response = [];
			clickhouse_options.queryOptions.database = req.query.db;
		  	// Re-Initialize Clickhouse Client
		  	var tmp = new ClickHouse(clickhouse_options);
			var stream = tmp.query('SHOW TABLES');
			stream.on ('data', function (row) {
			  response.push (row);
			});
			stream.on ('error', function (err) {
				// TODO: handler error
				console.error('GET DATA ERR',err);
			});
			stream.on ('end', function () {
				var results = {"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":response }]}]}
				res.send(results);
			});

		}

          } else if (rawQuery.startsWith('SHOW DATABASES')) {
		var response = [];
		var stream = clickhouse.query(rawQuery);
		stream.on ('data', function (row) {
		  response.push (row);
		});
		stream.on ('error', function (err) {
			// TODO: handler error
			console.error('GET DATA ERR',err);
		});
		stream.on ('end', function () {
			databases = response;
			if (debug) console.log(databases)
			var results = {"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"], "values": response } ]} ]};
			res.send(results);
		});

          } else if (rawQuery.startsWith('SELECT')) {
		//var cleanQuery = rawQuery.replace(/GROUP BY time.*\)/, "");
		if (debug||exception) console.log('OH OH SELECT!',rawQuery);
                var parsed = ifqlparser.parse(rawQuery);
		if (debug||exception) console.log('OH OH PARSED!',JSON.stringify(parsed));
		var settings = parsed.parsed.table_exp.from.table_refs[0];
		var where = parsed.parsed.table_exp.where;
		var from_ts = where.condition.left.value == 'time' ? "toDateTime("+parseInt(where.condition.right.left.name.from_timestamp/1000)+")" : 'NOW()-300';
		var to_ts = where.condition.left.value == 'time' ? "toDateTime("+parseInt(where.condition.right.left.name.to_timestamp/1000)+")" : 'NOW()';
		var response = [];
		var sample = "SELECT entity, dt, ts,"
				+ " arrayJoin(arrayMap((mm, vv) -> (mm, vv), m, mv)) AS metric,  metric.1 AS metric_name, metric.2 AS metric_value "
				+ " FROM " + settings.table
				+ " WHERE dt BETWEEN " + from_ts + " AND " + to_ts;
		if(parsed.returnColumns[0].sourceColumns[0].value) {
			var subq = []
			parsed.returnColumns.forEach(function(source){
			  source.sourceColumns.forEach(function(metric_id){
				if (metric_id.value){
				   subq.push("metric_name = '" + metric_id.value+"'");
				}
			  })
			})
			sample += " AND ("+subq.join(' OR ')+")";
		}
		if (debug) console.log('QUERY',sample);
		clickhouse_options.queryOptions.database = settings.db || settings.database.replace('.autogen','');

		var metrics = {};
		var template = {"statement_id":0,"series":[{"name": settings.table ,"columns":[] }]};

	  	// Re-Initialize Clickhouse Client
	  	var tmp = new ClickHouse(clickhouse_options);
		var stream = tmp.query(sample);
		stream.on ('data', function (row) {
		  if(!metrics[row[4]]) metrics[row[4]] = [];
		  var tmp = [row[2]/1000000];
		  for (i=5;i<row.length;i++){ tmp.push(row[i]) };
		  metrics[row[4]].push(tmp);
		});
		stream.on ('error', function (err) {
			// TODO: handler error
			console.error('GET DATA ERR',err);
		});
		stream.on ('end', function () {
			var results = {"results": []};
			var columns = parsed.returnColumns.map(x => x.name);
			columns.unshift("time");
			Object.keys(metrics).forEach(function(key,i) {
			  results.results.push( {"statement_id":i,"series":[{"name": key ,"columns": columns, "values": metrics[key] }]} );
			});
			res.send(results);
		});

          } else if (rawQuery.startsWith('SHOW SUBSCRIPTIONS')) {

		var mock = {
		    "results": [
		        {
		            "statement_id": 0
		        }
		    ]
		};
		res.send(mock);

          } else {
		try {
	                //var parsed = ifqlparser.parse(rawQuery);
			console.log('UNSUPPORTED',rawQuery);
			res.send(resp_empty);
			
		} catch(e) { console.error('UNSUPPORTED',e) }
          }
  } catch(e) {
          console.log(e);
	  getTables();
          res.send(resp_empty);
  }
	
});

/* INFLUXDB PING EMULATION */
app.get('/ping', (req, res) => {
	if (debug) console.log('PING req!');
	clickhouse.pinging().then((result) => { res.sendStatus(204) } )
})

process.on('unhandledRejection', function(err, promise) {
    console.error('Error:',err);
    if (exception) console.error('Unhandled rejection (promise: ', promise, ', reason: ', err, ').');
});
