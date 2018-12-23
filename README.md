<img src="https://user-images.githubusercontent.com/1423657/50374656-0a078400-05f2-11e9-8754-98b98e0244c4.png" width=100>

# nonFlux
Experimental, unoptimized InfluxDB to Clickhouse Gateway prototype for Timeseries. 

**Do not use this!**


### Status
- [x] Endpoint `/write`
  - [x] line protocol parser
  - [x] clickhouse insert statement
  - [x] clickhouse bulk inserts w/ LRU
- [ ] Endpoint `/query`
  - [x] IFQL Parser
  - [x] SHOW DATABASES
  - [x] SHOW MEASUREMENTS
  - [x] SHOW RETENTION POLICIES (fake)
  - [ ] SHOW TAG KEYS
  - [ ] SHOW TAG VALUES
  - [x] SHOW FIELDS KEYS
  - [ ] SELECT
    - [x] Fields
    - [ ] Tags
    - [x] Timerange _(now)_

### Usage
##### Start Server
```
CLICKHOUSE_SERVER=my.clickhouse.server npm start
```

The server attempts emulating an InfluxDB instance and can accept line protocol and query requests from Telegraf, Chronograf, Kapacitor and potentially clients with (extremely) basic features.


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

![ezgif com-optimize 13](https://user-images.githubusercontent.com/1423657/50386450-e114eb00-06e6-11e9-986b-aa6fd0ad9026.gif)
