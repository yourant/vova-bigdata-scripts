INSERT overwrite table ods_fd_ecshop.ods_fd_fd_stock_goods_config_inc  PARTITION (pt= '${hiveconf:pt}')
select id, goods_id, min_quantity, produce_days, change_provider, change_provider_days, change_provider_reason, pms_purchase, pms_purchase_days, is_delete, update_date, fabric, provider_type, audit_submit_time, audit_action_id, status
from(
    select
        o_raw.xid AS event_id,
        o_raw.`table` AS event_table,
        o_raw.type AS event_type,
        cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
        cast(o_raw.ts AS BIGINT) AS event_date,
        o_raw.id,
        o_raw.goods_id,
        o_raw.min_quantity,
        o_raw.produce_days,
        o_raw.change_provider,
        o_raw.change_provider_days,
        o_raw.change_provider_reason,
        o_raw.pms_purchase,
        o_raw.pms_purchase_days,
        o_raw.is_delete,
        o_raw.update_date,
        o_raw.fabric,
        o_raw.provider_type,
        o_raw.audit_submit_time,
        o_raw.audit_action_id,
        o_raw.status,
        row_number () OVER (PARTITION BY o_raw.id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.fd_ecshop_fd_stock_goods_config
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'goods_id', 'min_quantity', 'produce_days', 'change_provider', 'change_provider_days', 'change_provider_reason', 'pms_purchase', 'pms_purchase_days', 'is_delete', 'update_date', 'fabric', 'provider_type', 'audit_submit_time', 'audit_action_id', 'status') o_raw
    AS `table`, ts, `commit`, xid, type, old, id, goods_id, min_quantity, produce_days, change_provider, change_provider_days, change_provider_reason, pms_purchase, pms_purchase_days, is_delete, update_date, fabric, provider_type, audit_submit_time, audit_action_id, status
    where pt= '${hiveconf:pt}'
)inc where inc.rank = 1
;
