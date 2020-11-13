CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_goods_arc (
    rec_id INT,
    order_id INT,
    goods_style_id INT COMMENT '@see goods_style.goods_style_id',
    sku STRING COMMENT '@see gods_style.sku',
    sku_id INT COMMENT '@see goods_sku.sku_id',
    goods_id INT,
    goods_name STRING,
    goods_sn STRING,
    goods_sku STRING COMMENT '商品 sku',
    goods_number INT,
    market_price DECIMAL(10, 2),
    shop_price DECIMAL(10, 2),
    shop_price_exchange DECIMAL(10, 2) COMMENT '价格转换后的数额',
    shop_price_amount_exchange DECIMAL(10, 2) COMMENT '总额的转换后数值',
    bonus DECIMAL(10, 2) COMMENT '该商品的折扣，负值；可能来自分类折扣或该商品折扣',
    coupon_code STRING COMMENT '优惠券代码',
    goods_attr STRING,
    send_number INT,
    is_real INT,
    extension_code STRING,
    parent_id INT,
    is_gift INT,
    goods_status INT,
    action_amt DECIMAL(10, 2),
    action_reason_cat INT,
    action_note STRING,
    carrier_bill_id INT,
    provider_id INT,
    invoice_num STRING,
    return_points INT,
    return_bonus STRING,
    biaoju_store_goods_id INT,
    subtitle STRING,
    addtional_shipping_fee INT,
    style_id INT,
    customized STRING COMMENT '表示移动定制机信息',
    status_id STRING COMMENT '商品新旧状态',
    added_fee DECIMAL(10, 2) COMMENT '税率',
    custom_fee DECIMAL(10, 2) COMMENT '自定义尺寸的费用',
    custom_fee_exchange DECIMAL(10, 2),
    plussize_fee DECIMAL(10, 2) COMMENT '大尺码加钱',
    plussize_fee_exchange DECIMAL(10, 2),
    rush_order_fee DECIMAL(10, 2) COMMENT 'rush order fee',
    rush_order_fee_exchange DECIMAL(10, 2) COMMENT 'rush order fee',
    coupon_goods_id INT,
    coupon_cat_id INT,
    coupon_config_value DECIMAL(10, 2) COMMENT '@see ok_coupon_config',
    coupon_config_coupon_type STRING COMMENT '@see ok_coupon_config',
    styles STRING COMMENT '用户选择或输入的各种样式，json；记录备查',
    img_type STRING COMMENT '默认图类型',
    goods_gallery STRING COMMENT '下单时产品显示图',
    goods_price_original DECIMAL(10, 2) COMMENT '商品未添加任何附加费的价格',
    wrap_price DECIMAL(10, 2) COMMENT '披肩美元价格',
    wrap_price_exchange DECIMAL(10, 2) COMMENT '当前下单使用币种转换后的披肩价格',
    display_shop_price_exchange DECIMAL(10, 2) COMMENT '显示用商品单价',
    display_shop_price_amount_exchange DECIMAL(10, 2) COMMENT '显示用商品总金额费',
    display_custom_fee_exchange DECIMAL(10, 2) COMMENT '显示用自定义尺寸费',
    display_plussize_fee_exchange DECIMAL(10, 2) COMMENT '显示用加大码费',
    display_rush_order_fee_exchange DECIMAL(10, 2) COMMENT '显示用加急费',
    display_wrap_price_exchange DECIMAL(10, 2) COMMENT '显示用披肩费',
    heel_type_price DECIMAL(10, 2) COMMENT '鞋跟定制费用',
    heel_type_price_exchange DECIMAL(10, 2) COMMENT '鞋跟定制支付币种费用',
    display_heel_type_price_exchange DECIMAL(10, 2) COMMENT '鞋跟定制支付币种显示费用'
) COMMENT '来自kafka订单商品每日增量数据'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_vb.ods_fd_order_goods_arc PARTITION (dt='${hiveconf:dt}')
select 
    rec_id, order_id, goods_style_id, sku, sku_id, goods_id, goods_name, goods_sn, goods_sku, goods_number, market_price, shop_price, shop_price_exchange, shop_price_amount_exchange, bonus, coupon_code, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, custom_fee, custom_fee_exchange, plussize_fee, plussize_fee_exchange, rush_order_fee, rush_order_fee_exchange, coupon_goods_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, styles, img_type, goods_gallery, goods_price_original, wrap_price, wrap_price_exchange, display_shop_price_exchange, display_shop_price_amount_exchange, display_custom_fee_exchange, display_plussize_fee_exchange, display_rush_order_fee_exchange, display_wrap_price_exchange, heel_type_price, heel_type_price_exchange, display_heel_type_price_exchange
from (
    select dt,rec_id, order_id, goods_style_id, sku, sku_id, goods_id, goods_name, goods_sn, goods_sku, goods_number, market_price, shop_price, shop_price_exchange, shop_price_amount_exchange, bonus, coupon_code, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, custom_fee, custom_fee_exchange, plussize_fee, plussize_fee_exchange, rush_order_fee, rush_order_fee_exchange, coupon_goods_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, styles, img_type, goods_gallery, goods_price_original, wrap_price, wrap_price_exchange, display_shop_price_exchange, display_shop_price_amount_exchange, display_custom_fee_exchange, display_plussize_fee_exchange, display_rush_order_fee_exchange, display_wrap_price_exchange, heel_type_price, heel_type_price_exchange, display_heel_type_price_exchange,
        row_number () OVER (PARTITION BY rec_id ORDER BY dt DESC) AS rank
    from (
        select 
            '2020-01-01' as dt,
            rec_id,
            order_id,
            goods_style_id,
            sku,
            sku_id,
            goods_id,
            goods_name,
            goods_sn,
            goods_sku,
            goods_number,
            market_price,
            shop_price,
            shop_price_exchange,
            shop_price_amount_exchange,
            bonus,
            coupon_code,
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
            custom_fee,
            custom_fee_exchange,
            plussize_fee,
            plussize_fee_exchange,
            rush_order_fee,
            rush_order_fee_exchange,
            coupon_goods_id,
            coupon_cat_id,
            coupon_config_value,
            coupon_config_coupon_type,
            styles,
            img_type,
            goods_gallery,
            goods_price_original,
            wrap_price,
            wrap_price_exchange,
            display_shop_price_exchange,
            display_shop_price_amount_exchange,
            display_custom_fee_exchange,
            display_plussize_fee_exchange,
            display_rush_order_fee_exchange,
            display_wrap_price_exchange,
            heel_type_price,
            heel_type_price_exchange,
            display_heel_type_price_exchange
        from tmp.tmp_fd_order_goods_full
        union 
        select dt,rec_id, order_id, goods_style_id, sku, sku_id, goods_id, goods_name, goods_sn, goods_sku, goods_number, market_price, shop_price, shop_price_exchange, shop_price_amount_exchange, bonus, coupon_code, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, custom_fee, custom_fee_exchange, plussize_fee, plussize_fee_exchange, rush_order_fee, rush_order_fee_exchange, coupon_goods_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, styles, img_type, goods_gallery, goods_price_original, wrap_price, wrap_price_exchange, display_shop_price_exchange, display_shop_price_amount_exchange, display_custom_fee_exchange, display_plussize_fee_exchange, display_rush_order_fee_exchange, display_wrap_price_exchange, heel_type_price, heel_type_price_exchange, display_heel_type_price_exchange
        from (
            select 
                '${hiveconf:dt}' as dt,
                rec_id,
                order_id,
                goods_style_id,
                sku,
                sku_id,
                goods_id,
                goods_name,
                goods_sn,
                goods_sku,
                goods_number,
                market_price,
                shop_price,
                shop_price_exchange,
                shop_price_amount_exchange,
                bonus,
                coupon_code,
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
                custom_fee,
                custom_fee_exchange,
                plussize_fee,
                plussize_fee_exchange,
                rush_order_fee,
                rush_order_fee_exchange,
                coupon_goods_id,
                coupon_cat_id,
                coupon_config_value,
                coupon_config_coupon_type,
                styles,
                img_type,
                goods_gallery,
                goods_price_original,
                wrap_price,
                wrap_price_exchange,
                display_shop_price_exchange,
                display_shop_price_amount_exchange,
                display_custom_fee_exchange,
                display_plussize_fee_exchange,
                display_rush_order_fee_exchange,
                display_wrap_price_exchange,
                heel_type_price,
                heel_type_price_exchange,
                display_heel_type_price_exchange,
                row_number () OVER (PARTITION BY rec_id ORDER BY event_id DESC) AS rank
            from ods_fd_vb.ods_fd_order_goods_inc where dt = '${hiveconf:dt}'

        )inc where inc.rank = 1

    ) arc
)tab where tab.rank = 1;
