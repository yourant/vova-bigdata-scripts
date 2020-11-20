INSERT overwrite TABLE ods_fd_vb.ods_fd_order_status_change_history_inc PARTITION (pt= '${hiveconf:pt})
select id, order_sn, field_name, old_value, new_value, create_time
from (
    SELECT  o_raw.xid AS event_id,
            o_raw.`table` AS event_table,
            o_raw.type AS event_type,
            cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
            cast(o_raw.ts AS BIGINT) AS event_date,
            cast(o_raw.id AS INT) AS id,
            o_raw.order_sn  AS order_sn,
            o_raw.field_name AS field_name,
            cast(o_raw.old_value AS bigint) AS old_value,
            cast(o_raw.new_value AS bigint) AS new_value,
            cast(unix_timestamp(o_raw.create_time, 'yyyy-MM-dd HH:mm:ss') as BIGINT) AS create_time,
            row_number () OVER (PARTITION BY o_raw.id ORDER BY o_raw.xid DESC) AS rank
    FROM    pdb.fd_vb_order_status_change_history
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type', 'kafka_old','id', 'order_sn', 'field_name', 'old_value', 'new_value', 'create_time') o_raw
    AS `table`, ts, `commit`, xid, type, old, id, order_sn, field_name, old_value, new_value, create_time
    WHERE pt = '${hiveconf:pt}'
)inc where inc.rank = 1;
