CREATE EXTERNAL TABLE IF NOT EXISTS `race_start_list`(
  `race_id` int, 
  `type` string, 
  `info` string, 
  `race_name` string, 
  `laps` int, 
  `distance` int, 
  `ts` string, 
  `startlist` array<struct<bib:int,name:string,team:string,nat:string,state:string,start_position:int,start_lane:int>> )
PARTITIONED BY ( 
  `date_part` string, 
  `race_part` string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='distance,info,laps,race_id,race_name,startlist,ts,type',
  'ignore.malformed.json' = 'true') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://eurosport-cycling-raw-dev/batch-data/race_start_list/'
TBLPROPERTIES ('classification'='json')