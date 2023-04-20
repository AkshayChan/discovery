CREATE EXTERNAL TABLE IF NOT EXISTS `race_results`(
  `race_id` int, 
  `type` string, 
  `info` string, 
  `state` string, 
  `race_name` string, 
  `laps` int, 
  `distance` int, 
  `race_time` string, 
  `race_speed` string, 
  `ts` string, 
  `results` array<struct<rank:string,bib:string,name:string,team:string,nat:string,state:string,laps:int>>)
PARTITIONED BY ( 
  `date_part` string, 
  `race_part` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='distance,info,laps,race_id,race_name,race_speed,race_time,results,state,ts,type',
  'ignore.malformed.json' = 'true') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://eurosport-cycling-raw-dev/batch-data/race_results/'
TBLPROPERTIES ('classification'='json')