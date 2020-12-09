CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_route_arc(
  `route_id` bigint,
  `route_sn` string,
  `route_code` string,
  `cat_id` bigint,
  `parent_sn` string,
  `filter` string,
  `display_anchor` string,
  `project_name` string,
  `sitemap` bigint
 )comment '币种'
PARTITIONED BY (pt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;



INSERT overwrite table ods_fd_vb.ods_fd_route_arc PARTITION (pt='${hiveconf:pt}')
select  
	route_id,
	route_sn,
	route_code,
	cat_id,
	parent_sn,
	filter,
	display_anchor,
	project_name,
	sitemap
from tmp.tmp_fd_route_full;
