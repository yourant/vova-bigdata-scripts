INSERT overwrite table ods_fd_ecshop.ods_fd_fd_sku_backups_inc  PARTITION (pt= '${hiveconf:pt}')
select id,uniq_sku,sale_region,color,size
from(
    select
        o_raw.xid AS event_id,
        o_raw.`table` AS event_table,
        o_raw.type AS event_type,
        cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
        cast(o_raw.ts AS BIGINT) AS event_date,
        o_raw.id,
        o_raw.uniq_sku,
        o_raw.sale_region,
        o_raw.color,
        o_raw.size,
        row_number() OVER (PARTITION BY o_raw.id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.fd_ecshop_fd_sku_backups
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'uniq_sku', 'sale_region', 'color', 'size') o_raw
    AS `table`, ts, `commit`, xid, type, old, id,uniq_sku,sale_region,color,size
    where pt= '${hiveconf:pt}'
)inc where inc.rank = 1;
