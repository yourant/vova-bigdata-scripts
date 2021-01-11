INSERT overwrite table ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail_inc  PARTITION (pt= '${hiveconf:pt}')
select id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
from(
    select
        o_raw.xid AS event_id,
        o_raw.`table` AS event_table,
        o_raw.type AS event_type,
        cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
        cast(o_raw.ts AS BIGINT) AS event_date,
        o_raw.id,
        o_raw.bak_id,
        o_raw.bak_order_date as bak_order_date,
        o_raw.external_goods_id,
        o_raw.on_sale_time as on_sale_time,
        o_raw.7d_sale,
        o_raw.14d_sale,
        o_raw.28d_sale,
        o_raw.uniq_sku,
        row_number() OVER (PARTITION BY o_raw.id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.fd_ecshop_fd_stock_ecs_order_sale_bak_detail
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'bak_id', 'bak_order_date', 'external_goods_id', 'on_sale_time', '7d_sale', '14d_sale', '28d_sale', 'uniq_sku') o_raw
    AS `table`, ts, `commit`, xid, type, old, id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
    where pt= '${hiveconf:pt}'
)inc where inc.rank = 1;
