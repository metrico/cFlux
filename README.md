# clickFlux
Experimental InfluxDB to Clickhouse Gateway for Timeseries


### Status
- [x] implement `/write`
  - [x] line protocol parser
  - [x] clickhouse insert statement
- [ ] implement `/query`
   - [x] ifql protocol parser

### Usage
##### Start Server
```
CLICKHOUSE_SERVER=my.clickhouse.server npm start
```
##### POST Metrics `/write`
The `/write` endpoint expects HTTP POST data using the InfluxDB line protocol:
```
<measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
```
###### Example
```
 curl -d "statistics_method,cseq=OPTIONS 100=1,OPTIONS=1 1545424651000000000" \
      -X POST 'http://localhost:8686/write?db=mystats'
```
