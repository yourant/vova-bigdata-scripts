CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_marketing_data_inc
(
    id BIGINT,
    order_id BIGINT,
    sp_session_id STRING COMMENT 'artemis session_id',
    created_time STRING COMMENT '创建时间',
    last_update_time timestamp COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单session关联表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
INSERT overwrite TABLE ods_fd_vb.ods_fd_order_marketing_data_inc PARTITION (pt= '${hiveconf:pt}')
select id, order_id, sp_session_id, created_time, last_update_time
from (
    SELECT  o_raw.xid AS event_id,
            o_raw.`table` AS event_table,
            o_raw.type AS event_type,
            cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
            cast(o_raw.ts AS BIGINT) AS event_date,
            cast(o_raw.id AS INT) AS id,
            cast(o_raw.order_id AS INT) AS order_id,
            o_raw.sp_session_id AS sp_session_id,
            o_raw.created_time AS created_time,
            o_raw.last_update_time AS last_update_time,
            row_number () OVER (PARTITION BY o_raw.id ORDER BY o_raw.xid DESC) AS rank
    FROM    pdb.fd_vb_order_marketing_data
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type', 'kafka_old', 'id', 'order_id', 'sp_session_id', 'created_time', 'last_update_time') o_raw
    AS `table`, ts, `commit`, xid, type, old, id, order_id, sp_session_id, created_time, last_update_time
    WHERE pt = '${hiveconf:pt}'
) inc where inc.rank = 1;
