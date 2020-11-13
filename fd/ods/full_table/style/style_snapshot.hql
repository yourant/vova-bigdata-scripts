CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_style(
        `style_id` bigint,
        `cat_id` int, 
        `type` string, 
        `name` string, 
        `parent_id` bigint, 
	`value` string,
        `is_show` int,
        `display_order` int COMMENT '显示排序', 
        `last_update_time` string COMMENT '最后更新时间',
        `style_price` double COMMENT '定制样式费用',
        `is_important` int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_style
select `(dt)?+.+`
from ods_fd_vb.ods_fd_style_arc
where dt = '${hiveconf:dt}';
