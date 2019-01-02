var lineToJSON = require('@qxip/influx-line-protocol-parser');

module.exports = function(line,tableForce){

	var table = 'ts1';

	var m = [], mv = [];
	var t = [], tv = [];

	try {
		var parsed = lineToJSON(line);
	} catch(e){
		throw e;
	}

	if (tableForce) table = tableForce;
	else if (parsed.measurement) table = parsed.measurement;

	if (parsed.fields){
	    parsed.fields.map(function (obj) { return Object.keys(obj) }).forEach(function(item){ m.push(item[0]) });
	    parsed.fields.map(function (obj) { return Object.values(obj) }).forEach(function(item){ mv.push(item[0]) });
	}

	if (parsed.tags){
	    parsed.tags.map(function (obj) { return Object.keys(obj) }).forEach(function(item){ t.push(item[0]) });
	    parsed.tags.map(function (obj) { return Object.values(obj) }).forEach(function(item){ tv.push(item[0]) });
	}

	var values = [];
	    values.push(parsed.measurement);
	    values.push(parsed.timestamp);
	    values.push(JSON.stringify(m).replace(/"/g, "'"));
	    values.push(JSON.stringify(mv).replace(/"/g, "'"));
	    values.push(JSON.stringify(t).replace(/"/g, "'"));
	    values.push(JSON.stringify(tv).replace(/"/g, "'"));

	parsed.ts = new Date().getTime();

	return {
		parsed: parsed,
		m: JSON.stringify(m).replace(/"/g, "'"),
		mv: JSON.stringify(mv).replace(/"/g, "'"),
		t: JSON.stringify(t).replace(/"/g, "'"),
		tv: JSON.stringify(tv).replace(/"/g, "'")
 	};

}
