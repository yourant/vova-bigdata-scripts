CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_status_change_history_arc (
    id INT,
    order_sn STRING,
    field_name STRING,
    old_value BIGINT,
    new_value BIGINT,
    create_time timestamp COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单状态变化表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_vb.ods_fd_order_status_change_history_arc PARTITION (pt='${hiveconf:pt}')
select id, order_sn, field_name, old_value, new_value, create_time
from (
        select dt, id, order_sn, field_name, old_value, new_value, create_time,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from (
        select
            dt,
            id,
            order_id,
            ext_name,
            ext_value,
            is_delete,
            create_time
        from ods_fd_vb.ods_fd_order_status_change_history_arc where pt='${hiveconf:pt_last}'

        UNION
        select
            dt,
            id,
            order_sn,
            field_name,
            old_value,
            new_value,
            create_time
        from ods_fd_vb.ods_fd_order_status_change_history_inc where pt >= '${hiveconf:pt}'
    )arc

) tab where tab.rank = 1;