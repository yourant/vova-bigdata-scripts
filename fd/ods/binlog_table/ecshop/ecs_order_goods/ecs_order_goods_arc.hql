set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_order_goods_arc PARTITION (pt = '${pt}')
select 
     rec_id, order_id, goods_id, goods_name, goods_sn, goods_number, market_price, goods_price, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, external_order_goods_id
from (

    select 
        pt,rec_id, order_id, goods_id, goods_name, goods_sn, goods_number, market_price, goods_price, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, external_order_goods_id,
        row_number () OVER (PARTITION BY rec_id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                rec_id,
                order_id,
                goods_id,
                goods_name,
                goods_sn,
                goods_number,
                market_price,
                goods_price,
                goods_attr,
                send_number,
                is_real,
                extension_code,
                parent_id,
                is_gift,
                goods_status,
                action_amt,
                action_reason_cat,
                action_note,
                carrier_bill_id,
                provider_id,
                invoice_num,
                return_points,
                return_bonus,
                biaoju_store_goods_id,
                subtitle,
                addtional_shipping_fee,
                style_id,
                customized,
                status_id,
                added_fee,
                external_order_goods_id
        from ods_fd_ecshop.ods_fd_ecs_order_goods_arc where pt = '${pt_last}'

        UNION

        select  pt,
                rec_id,
                order_id,
                goods_id,
                goods_name,
                goods_sn,
                goods_number,
                market_price,
                goods_price,
                goods_attr,
                send_number,
                is_real,
                extension_code,
                parent_id,
                is_gift,
                goods_status,
                action_amt,
                action_reason_cat,
                action_note,
                carrier_bill_id,
                provider_id,
                invoice_num,
                return_points,
                return_bonus,
                biaoju_store_goods_id,
                subtitle,
                addtional_shipping_fee,
                style_id,
                customized,
                status_id,
                added_fee,
                external_order_goods_id
        from ods_fd_ecshop.ods_fd_ecs_order_goods_inc where pt='${pt}'

    ) arc 
) tab where tab.rank = 1;
