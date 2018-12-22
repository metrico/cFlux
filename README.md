# clickFlux
Experimental InfluxDB to Clickhouse Gateway for Timeseries

clickFlux supports HTTP POST data using the InfluxDB line protocol:
```
<measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
```

### Status
- [x] implement `/write`
- [ ] implement `/query`

### Usage
##### Start Server
```
CLICKHOUSE_SERVER=my.clickhouse.server npm start
```
##### POST Metrics to `/write` endpoint
```
 curl -d "statistics_method,cseq=OPTIONS 100=1,OPTIONS=1 1545424651000000000" \
      -X POST 'http://localhost:8686/write?db=mystats'
```
