CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_goods_arc (
    rec_id                  bigint comment '订单商品记录id(unique)',
    order_id                bigint,
    goods_id                bigint,
    goods_name              string,
    goods_sn                string,
    goods_number            bigint,
    market_price            decimal(10, 2),
    goods_price             decimal(16, 6),
    goods_attr              string,
    send_number             bigint,
    is_real                 tinyint,
    extension_code          string,
    parent_id               bigint,
    is_gift                 bigint,
    goods_status            tinyint,
    action_amt              decimal(10, 2),
    action_reason_cat       tinyint,
    action_note             string,
    carrier_bill_id         bigint,
    provider_id             bigint,
    invoice_num             string,
    return_points           bigint,
    return_bonus            string,
    biaoju_store_goods_id   bigint,
    subtitle                string,
    addtional_shipping_fee  bigint,
    style_id                bigint,
    customized              string,
    status_id               string comment '商品新旧状态',
    added_fee               decimal(10, 4) comment '税率',
    external_order_goods_id bigint comment '网站order_goods_id'
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_order_goods_arc PARTITION (dt = '${hiveconf:dt}')
select 
     rec_id, order_id, goods_id, goods_name, goods_sn, goods_number, market_price, goods_price, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, external_order_goods_i
from (

    select 
        dt,rec_id, order_id, goods_id, goods_name, goods_sn, goods_number, market_price, goods_price, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, external_order_goods_i,
        row_number () OVER (PARTITION BY region_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
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
                external_order_goods_i
        from tmp.tmp_fd_ecs_order_goods_full

        UNION

        select dt,rec_id, order_id, goods_id, goods_name, goods_sn, goods_number, market_price, goods_price, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, external_order_goods_i
        from (

            select  dt
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
                    external_order_goods_i,
                    row_number () OVER (PARTITION BY rec_id ORDER BY event_id DESC) AS rank
            from ods_fd_ecshop.ods_fd_ecs_order_goods_arc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
