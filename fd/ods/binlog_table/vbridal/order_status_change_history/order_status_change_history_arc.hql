CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_status_change_history_arc (
`id` bigint COMMENT '自增id',
`order_sn` string COMMENT '订单号',
`field_name` string COMMENT '字段名',
`old_value` bigint COMMENT '旧值',
`new_value` bigint COMMENT '新值',
`create_time` string COMMENT '发生时间'
) COMMENT '订单状态变更记录'
PARTITIONED BY (pt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

INSERT overwrite table ods_fd_vb.ods_fd_order_status_change_history_arc PARTITION (pt='${hiveconf:pt}')
select id, order_sn, field_name, old_value, new_value, create_time
from (
    select id, order_sn, field_name, old_value, new_value, create_time, 
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from(
        select 
            pt,
            id,
            order_sn,
            field_name,
            old_value,
            new_value,
            create_time
        from ods_fd_vb.ods_fd_order_status_change_history_arc 
        where pt = '${hiveconf:pt_last}'

        UNION

        select 
            pt,
            id,
            order_sn,
            field_name,
            old_value,
            new_value,
            create_time
        from ods_fd_vb.ods_fd_order_status_change_history_inc
        where pt >= '${hiveconf:pt}'
    )inc
) arc where arc.rank =1;
