CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_region_area(
  `id` int COMMENT '',
  `region_area_name` string COMMENT '区域名/洲名en',
  `region_area_cn_name` string COMMENT '区域名/洲名cn',
  `region_area_code` string COMMENT '区域code',
  `parent_id` int COMMENT '父ID',
  `created` string COMMENT '创建时间',
  `modified` string COMMENT '修改时间'
 )comment '区域'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_region_area
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_region_area_arc
where dt = '${hiveconf:dt}';
