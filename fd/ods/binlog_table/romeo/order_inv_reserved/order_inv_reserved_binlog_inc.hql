INSERT overwrite table ods_fd_romeo.ods_fd_order_inv_reserved_binlog_inc PARTITION (pt='${pt}')
select /*+ REPARTITION(1) */order_inv_reserved_id, version, status, order_id, facility_id, container_id, party_id, reserved_time, delivery_time, order_time, version2, created_stamp, last_updated_stamp,event_type
from(
    select
        o_raw.xid AS event_id,
        o_raw.`table` AS event_table,
        o_raw.type AS event_type,
        cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
        cast(o_raw.ts AS BIGINT) AS event_date,
        o_raw.order_inv_reserved_id,
        o_raw.version,
        o_raw.status,
        o_raw.order_id,
        o_raw.facility_id,
        o_raw.container_id,
        o_raw.party_id,
        o_raw.reserved_time,
        o_raw.delivery_time,
        o_raw.order_time,
        o_raw.version2,
        o_raw.created_stamp,
        o_raw.last_updated_stamp,
        row_number () OVER (PARTITION BY o_raw.order_inv_reserved_id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.pdb_fd_order_inv_reserved
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'ORDER_INV_RESERVED_ID', 'VERSION', 'STATUS', 'ORDER_ID', 'FACILITY_ID', 'CONTAINER_ID', 'PARTY_ID', 'RESERVED_TIME', 'DELIVERY_TIME', 'ORDER_TIME', 'VERSION2', 'CREATED_STAMP', 'LAST_UPDATED_STAMP') o_raw
    AS `table`, ts, `commit`, xid, type, old, order_inv_reserved_id, version, status, order_id, facility_id, container_id, party_id, reserved_time, delivery_time, order_time, version2, created_stamp, last_updated_stamp
    where pt in ('${pt}',date_add('${pt}',1))
)inc where inc.rank = 1;

