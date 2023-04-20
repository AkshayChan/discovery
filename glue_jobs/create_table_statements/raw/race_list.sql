CREATE EXTERNAL TABLE IF NOT EXISTS `race_list`(
  `ts` string, 
  `date` string, 
  `event_name` string, 
  `races` array<struct<id:int,type:string,laps:int,distance:int,race_name:string,start_time:string>>)
PARTITIONED BY ( 
  `date_part` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='date,event_name,races,ts',
  'ignore.malformed.json' = 'true') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://eurosport-cycling-raw-dev/batch-data/race_list/'
TBLPROPERTIES ('classification'='json')