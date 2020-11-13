CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_style_arc(
        `style_id` bigint,
        `cat_id` bigint,
        `type` string, 
        `name` string,
	    `value` string,
        `parent_id` bigint, 
        `is_show` bigint,
        `display_order` bigint COMMENT '显示排序',
        `last_update_time` string COMMENT '最后更新时间',
        `style_price` decimal(15, 4) COMMENT '定制样式费用',
        `is_important` bigint)
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


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
