INSERT overwrite table ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping_binlog_inc PARTITION (pt= '${pt}')
select /*+ REPARTITION(1) */id,order_inv_reserved_detail_id,inventory_item_id,quantity,created_stamp,last_updated_stamp,event_type
from(

    select
        o_raw.xid AS event_id,
        o_raw.`table` AS event_table,
        o_raw.type AS event_type,
        cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
        cast(o_raw.ts AS BIGINT) AS event_date,
        o_raw.id,
        o_raw.order_inv_reserved_detail_id,
        o_raw.inventory_item_id,
        o_raw.quantity,
        o_raw.created_stamp AS created_stamp,
        o_raw.last_updated_stamp AS last_updated_stamp,
        row_number () OVER (PARTITION BY o_raw.id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.pdb_fd_order_inv_reserverd_inventory_mapping
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'ORDER_INV_RESERVED_DETAIL_ID', 'INVENTORY_ITEM_ID', 'QUANTITY', 'CREATED_STAMP', 'LAST_UPDATED_STAMP') o_raw
    AS `table`, ts, `commit`, xid, type, old, id, order_inv_reserved_detail_id, inventory_item_id, quantity, created_stamp, last_updated_stamp
    where pt in ('${pt}',date_add('${pt}',1))

) inc where inc.rank = 1;

