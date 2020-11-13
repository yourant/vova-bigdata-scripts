CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_style(
        `style_id` bigint,
        `cat_id` bigint,
        `type` string, 
        `name` string, 
        `parent_id` bigint, 
	    `value` string,
        `is_show` bigint,
        `display_order` bigint COMMENT '显示排序',
        `last_update_time` string COMMENT '最后更新时间',
        `style_price` decimal(15, 4) COMMENT '定制样式费用',
        `is_important` bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_style
select `(dt)?+.+`
from ods_fd_vb.ods_fd_style_arc
where dt = '${hiveconf:dt}';
