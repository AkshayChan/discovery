CREATE EXTERNAL TABLE IF NOT EXISTS stream_data (
    ts TIMESTAMP,
    race_id INT,
    captures ARRAY<STRUCT<
    ts_mes: STRING, 
    bib: STRING, 
    rank: INT, 
    state: STRING, 
    distance: DOUBLE, 
    distance_proj: DOUBLE, 
    speed: DOUBLE, 
    speed_max: DOUBLE, 
    speed_avg: DOUBLE, 
    distance_first: DOUBLE, 
    distance_next: DOUBLE, 
    acc: DOUBLE, 
    pos: STRUCT<lat: DOUBLE, lng: DOUBLE>,
    heartrate: INT, 
    cadence: INT, 
    power: INT>>)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (
  'paths'='captures,race_id,ts',
  'ignore.malformed.json' = 'true') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://eurosport-cycling-raw-dev/stream-data/'
TBLPROPERTIES ('classification'='json');
