INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_order_goods_inc  PARTITION (pt= '${hiveconf:pt}')
select rec_id, order_id, goods_id, goods_name, goods_sn, goods_number, market_price, goods_price, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, external_order_goods_id
from(
    select
        o_raw.xid AS event_id
        ,o_raw.`table` AS event_table
        ,o_raw.type AS event_type
        ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
        ,cast(o_raw.ts AS BIGINT) AS event_date
        ,o_raw.rec_id
        ,o_raw.order_id
        ,o_raw.goods_id
        ,o_raw.goods_name
        ,o_raw.goods_sn
        ,o_raw.goods_number
        ,o_raw.market_price
        ,o_raw.goods_price
        ,o_raw.goods_attr
        ,o_raw.send_number
        ,o_raw.is_real
        ,o_raw.extension_code
        ,o_raw.parent_id
        ,o_raw.is_gift
        ,o_raw.goods_status
        ,o_raw.action_amt
        ,o_raw.action_reason_cat
        ,o_raw.action_note
        ,o_raw.carrier_bill_id
        ,o_raw.provider_id
        ,o_raw.invoice_num
        ,o_raw.return_points
        ,o_raw.return_bonus
        ,o_raw.biaoju_store_goods_id
        ,o_raw.subtitle
        ,o_raw.addtional_shipping_fee
        ,o_raw.style_id
        ,o_raw.customized
        ,o_raw.status_id
        ,o_raw.added_fee
        ,o_raw.external_order_goods_id
        ,row_number () OVER (PARTITION BY o_raw.rec_id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.fd_ecshop_ecs_order_goods
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'rec_id', 'order_id', 'goods_id', 'goods_name', 'goods_sn', 'goods_number', 'market_price', 'goods_price', 'goods_attr', 'send_number', 'is_real', 'extension_code', 'parent_id', 'is_gift', 'goods_status', 'action_amt', 'action_reason_cat', 'action_note', 'carrier_bill_id', 'provider_id', 'invoice_num', 'return_points', 'return_bonus', 'biaoju_store_goods_id', 'subtitle', 'addtional_shipping_fee', 'style_id', 'customized', 'status_id', 'added_fee', 'external_order_goods_id') o_raw
    AS `table`, ts, `commit`, xid, type, old, rec_id, order_id, goods_id, goods_name, goods_sn, goods_number, market_price, goods_price, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, external_order_goods_id
    where pt = '${hiveconf:pt}'
)inc where inc.rank = 1;
