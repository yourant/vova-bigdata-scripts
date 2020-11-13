CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_extension_arc
(
    id INT,
    order_id INT,
    ext_name STRING,
    ext_value STRING,
    is_delete TINYINT,
    last_update_time BIGINT COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单扩展表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_vb.ods_fd_order_extension_arc PARTITION (dt='${hiveconf:dt}')
select id, order_id, ext_name, ext_value, is_delete, last_update_time
from (
        select dt, id, order_id, ext_name, ext_value, is_delete, last_update_time,
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
        from tmp.tmp_fd_order_extension_full
        UNION
        select dt, id, order_id, ext_name, ext_value, is_delete, last_update_time
        from(
            select 
                '${hiveconf:dt}' as dt,
                id,
                order_id,
                ext_name,
                ext_value,
                is_delete,
                last_update_time,
                row_number () OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_vb.ods_fd_order_extension_inc where dt = '${hiveconf:dt}'
        )inc where inc.rank = 1
    )arc

) tab where tab.rank = 1;
