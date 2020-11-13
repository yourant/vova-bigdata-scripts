CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_region_arc
(
  `region_id` int COMMENT '',
  `parent_id` string COMMENT '',
  `region_name` string COMMENT '',
  `region_type` int COMMENT '',
  `region_display` int COMMENT '',
  `region_code` string COMMENT '',
  `area_id` int COMMENT '区域ID',
  `display_order` string COMMENT '国家排序',
  `last_update_time` string COMMENT '最后更新时间',
  `time_zone` int COMMENT '',
  `chinese_region_name` int COMMENT '',
  `prefix` string COMMENT '国家的手机号前缀'
 )comment '区域'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_region_arc PARTITION (dt='${hiveconf:dt}')
select  
    region_id,
    parent_id,
    region_name,
    region_type,
    region_display,
    region_code,
    area_id,
    display_order,
    last_update_time,
    time_zone,
    chinese_region_name,
    prefix
from tmp.tmp_fd_region_full;
