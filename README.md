<img src="https://user-images.githubusercontent.com/1423657/50374656-0a078400-05f2-11e9-8754-98b98e0244c4.png" width=100>

# nonFlux
Experimental, unoptimized InfluxDB to Clickhouse Gateway prototype for Timeseries. Do not use this!


### Status
- [x] implement `/write`
  - [x] line protocol parser
  - [x] clickhouse insert statement
  - [x] clickhouse bulk inserts w/ LRU
- [ ] implement `/query`
  - [x] ifql protocol parser
  - [x] SHOW DATABASES
  - [x] SHOW MEASUREMENTS
  - [x] SHOW RETENTION POLICIES (fake)
  - [ ] SHOW TAG KEYS
  - [ ] SELECT

### Usage
##### Start Server
```
CLICKHOUSE_SERVER=my.clickhouse.server npm start
```

The server attempts emulating an InfluxDB instance with basic features and can accept data from Telegraf and other line clients.


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
