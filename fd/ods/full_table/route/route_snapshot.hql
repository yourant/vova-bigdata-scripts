CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_route(
   `route_id` bigint,
  `route_sn` string,
  `route_code` string,
  `cat_id` bigint,
  `parent_sn` string,
  `filter` string,
  `display_anchor` string,
  `project_name` string,
  `sitemap` bigint
 )comment ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_route
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_route_arc
where dt = '${hiveconf:dt}';
