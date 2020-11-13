CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_style_arc(
        `style_id` bigint,
        `cat_id` int, 
        `type` string, 
        `name` string,
	`value` string, 
        `parent_id` bigint, 
        `is_show` int,
        `display_order` int COMMENT '显示排序', 
        `last_update_time` string COMMENT '最后更新时间',
        `style_price` double COMMENT '定制样式费用',
        `is_important` int)
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_style_arc PARTITION (dt='${hiveconf:dt}')
select
    style_id,
    cat_id,
    type,
    name,
    value,
    parent_id,
    is_show,
    display_order,
    last_update_time,
    style_price,
    is_important
from tmp.tmp_fd_style_full;
