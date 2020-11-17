CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_status_change_history_arc (
    id INT,
    order_sn STRING,
    field_name STRING,
    old_value BIGINT,
    new_value BIGINT,
    create_time BIGINT COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单状态变化表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_vb.ods_fd_order_status_change_history_arc PARTITION (pt='${hiveconf:pt}')
select id, order_sn, field_name, old_value, new_value, create_time,dt
from (
        select dt, id, order_sn, field_name, old_value, new_value, create_time,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from (
            select
            '2020-01-01' as dt,
            id,
            order_id,
            ext_name,
            ext_value,
            is_delete,
            cast(unix_timestamp(last_update_time, 'yyyy-MM-dd HH:mm:ss') as BIGINT) AS last_update_time
        from ods_fd_vb.ods_fd_order_status_change_history_arc where pt='${hiveconf:pt_last}'

        UNION
        select dt, id, order_sn, field_name, old_value, new_value, create_time
        from(
            select
                dt,
                id,
                order_sn,
                field_name,
                old_value,
                new_value,
                create_time,
                row_number () OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_vb.ods_fd_order_status_change_history_inc where pt >= '${hiveconf:pt}'
        )inc where inc.rank = 1
    )arc

) tab where tab.rank = 1;