INSERT overwrite table ods_fd_romeo.ods_fd_order_inv_reserved_detail_binlog_inc  PARTITION (pt='${pt}')
select /*+ REPARTITION(1) */order_inv_reserved_detail_id, status, order_id, order_item_id, goods_number, product_id, order_inv_reserved_id, reserved_quantity, reserved_time, status_id, facility_id, version, created_stamp, last_updated_stamp,event_type
from(
    select
        o_raw.xid AS event_id,
        o_raw.`table` AS event_table,
        o_raw.type AS event_type,
        cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
        cast(o_raw.ts AS BIGINT) AS event_date,
        o_raw.order_inv_reserved_detail_id,
        o_raw.status,
        o_raw.order_id,
        o_raw.order_item_id,
        o_raw.goods_number,
        o_raw.product_id,
        o_raw.order_inv_reserved_id,
        o_raw.reserved_quantity,
        o_raw.reserved_time,
        o_raw.status_id,
        o_raw.facility_id,
        o_raw.version,
        o_raw.created_stamp,
        o_raw.last_updated_stamp,
        row_number () OVER (PARTITION BY o_raw.order_inv_reserved_detail_id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.pdb_fd_order_inv_reserved_detail
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'ORDER_INV_RESERVED_DETAIL_ID', 'STATUS', 'ORDER_ID', 'ORDER_ITEM_ID', 'GOODS_NUMBER', 'PRODUCT_ID', 'ORDER_INV_RESERVED_ID', 'RESERVED_QUANTITY', 'RESERVED_TIME', 'STATUS_ID', 'FACILITY_ID', 'VERSION', 'CREATED_STAMP', 'LAST_UPDATED_STAMP') o_raw
    AS `table`, ts, `commit`, xid, type, old, order_inv_reserved_detail_id, status, order_id, order_item_id, goods_number, product_id, order_inv_reserved_id, reserved_quantity, reserved_time, status_id, facility_id, version, created_stamp, last_updated_stamp
    where pt in ('${pt}',date_add('${pt}',1))
) inc where inc.rank = 1;
