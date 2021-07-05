CREATE EXTERNAL TABLE if not exists `pdb.pdb_fd_snowplow_offline`(
  `common_struct` struct<app_id:string,platform:string,project:string,platform_type:string,collector_ts:string,dvce_created_ts:string,dvce_sent_ts:string,etl_ts:string,derived_ts:string,os_tz:string,event_fingerprint:string,name_tracker:string,user_id:string,domain_userid:string,user_ipaddress:string,session_idx:bigint,session_id:string,useragent:string,dvce_type:string,dvce_ismobile:boolean,os_name:string,geo_country:string,geo_region:string,geo_city:string,geo_region_name:string,geo_timezone:string,raw_event_name:string,event_name:string,language:string,country:string,currency:string,page_code:string,user_unique_id:string,abtest:string,page_url:string,referrer_url:string,mkt_medium:string,mkt_source:string,mkt_term:string,mkt_content:string,mkt_campaign:string,mkt_clickid:string,mkt_network:string,user_fingerprint:string,br_name:string,br_lang:string,app_version:string,device_model:string,android_id:string,imei:string,idfa:string,idfv:string,apple_id_fa:string,apple_id_fv:string,os_type:string,os_version:string,network_type:string,referrer_page_code:string,url_virtual_goods_id:bigint,url_route_sn:bigint> COMMENT 'from deserializer',
  `goods_event_struct` array<struct<list_uri:string,list_type:string,virtual_goods_id:string,picture:string,page_position:bigint,absolute_position:bigint,page_size:bigint,page_no:bigint,element_name:string,picture_group: String,picture_batch: String,extra:string>> COMMENT 'from deserializer',
  `element_event_struct` array<struct<list_uri:string,list_type:string,types: String,element_name:string,element_url:string,element_content:string,element_id:string,element_type:string,picture:string,absolute_position:bigint,element_batch: String,element_tag: String,extra:string>> COMMENT 'from deserializer',
  `data_event_struct` struct<element_name:string,extra:string> COMMENT 'from deserializer',
  `ecommerce_action` struct<id:string,affiliation:string,option:string,list:string,revenue:double,step:bigint> COMMENT 'from deserializer',
  `ecommerce_product` array<struct<id:string,name:string,brand:string,category:string,coupon:string,position:bigint,price:double,quantity:bigint,variant:string>> COMMENT 'from deserializer')
PARTITIONED BY (
  `pt` string,
  `hour` string)
ROW FORMAT SERDE
  'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bigdata-offline/warehouse/pdb/fd/snowplow/snowplow_batch'
TBLPROPERTIES (
  'transient_lastDdlTime'='1606900800')

MSCK REPAIR TABLE pdb.pdb_fd_snowplow_offline;