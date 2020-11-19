CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_extension_arc
(
    id bigint,
    order_id bigint,
    ext_name string,
    ext_value string,
    is_delete bigint,
    last_update_time timestamp COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单扩展表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_vb.ods_fd_order_extension_arc PARTITION (pt='${hiveconf:pt}')
select id, order_id, ext_name, ext_value, is_delete, last_update_time
from (
        select pt, id, order_id, ext_name, ext_value, is_delete, last_update_time,
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (
        select
            pt,
            id,
            order_id,
            ext_name,
            ext_value,
            is_delete,
            last_update_time
        from ods_fd_vb.ods_fd_order_extension_arc where pt='${hiveconf:pt_last}'
        UNION
        select
            pt,
            id,
            order_id,
            ext_name,
            ext_value,
            is_delete,
            last_update_time
        from ods_fd_vb.ods_fd_order_extension_inc where pt >= '${hiveconf:pt}'
    )arc

) tab where tab.rank = 1;
