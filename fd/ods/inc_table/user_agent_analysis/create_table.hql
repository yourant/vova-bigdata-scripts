CREATE TABLE IF NOT EXISTS `tmp.tmp_fd_user_agent_analysis`(
  `user_agent_id` int,
  `is_app` int,
  `device_type` string,
  `os_type` string,
  `version` string,
  `device_id` string,
  `uuid` string,
  `project_name` string,
  `device_name` string,
  `browser` string,
  `idfa` string,
  `idfv` string,
  `imei` string,
  `android_id` string,
  `ga_id` string)
COMMENT 'Imported by sqoop'
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\001',
  'line.delim'='\n',
  'serialization.format'='\001')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${hiveconf:s3_hive_path}/tmp_fd_user_agent_analysis'
TBLPROPERTIES (
  'transient_lastDdlTime'='1604315307');
