CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_goods_inc (
    rec_id bigint,
    order_id bigint,
    goods_style_id bigint COMMENT '@see goods_style.goods_style_id',
    sku STRING COMMENT '@see gods_style.sku',
    sku_id bigint COMMENT '@see goods_sku.sku_id',
    goods_id bigint,
    goods_name STRING,
    goods_sn STRING,
    goods_sku STRING COMMENT '商品 sku',
    goods_number bigint,
    market_price decimal(15, 4),
    shop_price decimal(15, 4),
    shop_price_exchange decimal(15, 4) COMMENT '价格转换后的数额',
    shop_price_amount_exchange decimal(15, 4) COMMENT '总额的转换后数值',
    bonus decimal(15, 4) COMMENT '该商品的折扣，负值；可能来自分类折扣或该商品折扣',
    coupon_code STRING COMMENT '优惠券代码',
    goods_attr STRING,
    send_number bigint,
    is_real bigint,
    extension_code STRING,
    parent_id bigint,
    is_gift bigint,
    goods_status bigint,
    action_amt decimal(15, 4),
    action_reason_cat bigint,
    action_note STRING,
    carrier_bill_id bigint,
    provider_id bigint,
    invoice_num STRING,
    return_points bigint,
    return_bonus STRING,
    biaoju_store_goods_id bigint,
    subtitle STRING,
    addtional_shipping_fee bigint,
    style_id bigint,
    customized STRING COMMENT '表示移动定制机信息',
    status_id STRING COMMENT '商品新旧状态',
    added_fee decimal(15, 4) COMMENT '税率',
    custom_fee decimal(15, 4) COMMENT '自定义尺寸的费用',
    custom_fee_exchange decimal(15, 4),
    plussize_fee decimal(15, 4) COMMENT '大尺码加钱',
    plussize_fee_exchange decimal(15, 4),
    rush_order_fee decimal(15, 4) COMMENT 'rush order fee',
    rush_order_fee_exchange decimal(15, 4) COMMENT 'rush order fee',
    coupon_goods_id bigint,
    coupon_cat_id bigint,
    coupon_config_value decimal(15, 4) COMMENT '@see ok_coupon_config',
    coupon_config_coupon_type STRING COMMENT '@see ok_coupon_config',
    styles STRING COMMENT '用户选择或输入的各种样式，json；记录备查',
    img_type STRING COMMENT '默认图类型',
    goods_gallery STRING COMMENT '下单时产品显示图',
    goods_price_original decimal(15, 4) COMMENT '商品未添加任何附加费的价格',
    wrap_price decimal(15, 4) COMMENT '披肩美元价格',
    wrap_price_exchange decimal(15, 4) COMMENT '当前下单使用币种转换后的披肩价格',
    display_shop_price_exchange decimal(15, 4) COMMENT '显示用商品单价',
    display_shop_price_amount_exchange decimal(15, 4) COMMENT '显示用商品总金额费',
    display_custom_fee_exchange decimal(15, 4) COMMENT '显示用自定义尺寸费',
    display_plussize_fee_exchange decimal(15, 4) COMMENT '显示用加大码费',
    display_rush_order_fee_exchange decimal(15, 4) COMMENT '显示用加急费',
    display_wrap_price_exchange decimal(15, 4) COMMENT '显示用披肩费',
    heel_type_price decimal(15, 4) COMMENT '鞋跟定制费用',
    heel_type_price_exchange decimal(15, 4) COMMENT '鞋跟定制支付币种费用',
    display_heel_type_price_exchange decimal(15, 4) COMMENT '鞋跟定制支付币种显示费用'
) COMMENT '来自kafka订单商品每日增量数据'
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
hive.exec.dynamici.partition=true;
INSERT overwrite TABLE ods_fd_vb.ods_fd_order_goods_inc PARTITION (pt,hour)
select rec_id, order_id, goods_style_id, sku, sku_id, goods_id, goods_name, goods_sn, goods_sku, goods_number, market_price, shop_price, shop_price_exchange, shop_price_amount_exchange, bonus, coupon_code, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, custom_fee, custom_fee_exchange, plussize_fee, plussize_fee_exchange, rush_order_fee, rush_order_fee_exchange, coupon_goods_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, styles, img_type, goods_gallery, goods_price_original, wrap_price, wrap_price_exchange, display_shop_price_exchange, display_shop_price_amount_exchange, display_custom_fee_exchange, display_plussize_fee_exchange, display_rush_order_fee_exchange, display_wrap_price_exchange, heel_type_price, heel_type_price_exchange, display_heel_type_price_exchange,pt,hour
from(
    SELECT  o_raw.xid AS event_id,
            o_raw.`table` AS event_table,
            o_raw.type AS event_type,
            cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
            cast(o_raw.ts AS BIGINT) AS event_date,
            cast(o_raw.rec_id AS INT) AS rec_id,
            cast(o_raw.order_id AS INT) AS order_id,
            cast(o_raw.goods_style_id AS INT) AS goods_style_id,
            o_raw.sku AS sku,
            cast(o_raw.sku_id AS INT) AS sku_id,
            cast(o_raw.goods_id AS INT) AS goods_id,
            o_raw.goods_name AS goods_name,
            o_raw.goods_sn AS goods_sn,
            o_raw.goods_sku AS goods_sku,
            cast(o_raw.goods_number AS INT) AS goods_number,
            cast(o_raw.market_price AS DECIMAL(10, 2)) AS market_price,
            cast(o_raw.shop_price AS DECIMAL(10, 2)) AS shop_price,
            cast(o_raw.shop_price_exchange AS DECIMAL(10, 2)) AS shop_price_exchange,
            cast(o_raw.shop_price_amount_exchange AS DECIMAL(10, 2)) AS shop_price_amount_exchange,
            cast(o_raw.bonus AS DECIMAL(10, 2)) AS bonus,
            o_raw.coupon_code AS coupon_code,
            o_raw.goods_attr AS goods_attr,
            cast(o_raw.send_number AS INT) AS send_number,
            cast(o_raw.is_real AS INT) AS is_real,
            o_raw.extension_code AS extension_code,
            cast(o_raw.parent_id AS INT) AS parent_id,
            cast(o_raw.is_gift AS INT) AS is_gift,
            cast(o_raw.goods_status AS INT) AS goods_status,
            cast(o_raw.action_amt AS DECIMAL(10, 2)) AS action_amt,
            cast(o_raw.action_reason_cat AS INT) AS action_reason_cat,
            o_raw.action_note AS action_note,
            cast(o_raw.carrier_bill_id AS INT) AS carrier_bill_id,
            cast(o_raw.provider_id AS INT) AS provider_id,
            o_raw.invoice_num AS invoice_num,
            cast(o_raw.return_points AS INT) AS return_points,
            o_raw.return_bonus AS return_bonus,
            cast(o_raw.biaoju_store_goods_id AS INT) AS biaoju_store_goods_id,
            o_raw.subtitle AS subtitle,
            cast(o_raw.addtional_shipping_fee AS INT) AS addtional_shipping_fee,
            cast(o_raw.style_id AS INT) AS style_id,
            o_raw.customized AS customized,
            o_raw.status_id AS status_id,
            cast(o_raw.added_fee AS DECIMAL(10, 2)) AS added_fee,
            cast(o_raw.custom_fee AS DECIMAL(10, 2)) AS custom_fee,
            cast(o_raw.custom_fee_exchange AS DECIMAL(10, 2)) AS custom_fee_exchange,
            cast(o_raw.plussize_fee AS DECIMAL(10, 2)) AS plussize_fee,
            cast(o_raw.plussize_fee_exchange AS DECIMAL(10, 2)) AS plussize_fee_exchange,
            cast(o_raw.rush_order_fee AS DECIMAL(10, 2)) AS rush_order_fee,
            cast(o_raw.rush_order_fee_exchange AS DECIMAL(10, 2)) AS rush_order_fee_exchange,
            cast(o_raw.coupon_goods_id AS INT) AS coupon_goods_id,
            cast(o_raw.coupon_cat_id AS INT) AS coupon_cat_id,
            cast(o_raw.coupon_config_value AS DECIMAL(10, 2)) AS coupon_config_value,
            o_raw.coupon_config_coupon_type AS coupon_config_coupon_type,
            o_raw.styles AS styles,
            o_raw.img_type AS img_type,
            o_raw.goods_gallery AS goods_gallery,
            cast(o_raw.goods_price_original AS DECIMAL(10, 2)) AS goods_price_original,
            cast(o_raw.wrap_price AS DECIMAL(10, 2)) AS wrap_price,
            cast(o_raw.wrap_price_exchange AS DECIMAL(10, 2)) AS wrap_price_exchange,
            cast(o_raw.display_shop_price_exchange AS DECIMAL(10, 2)) AS display_shop_price_exchange,
            cast(o_raw.display_shop_price_amount_exchange AS DECIMAL(10, 2)) AS display_shop_price_amount_exchange,
            cast(o_raw.display_custom_fee_exchange AS DECIMAL(10, 2)) AS display_custom_fee_exchange,
            cast(o_raw.display_plussize_fee_exchange AS DECIMAL(10, 2)) AS display_plussize_fee_exchange,
            cast(o_raw.display_rush_order_fee_exchange AS DECIMAL(10, 2)) AS display_rush_order_fee_exchange,
            cast(o_raw.display_wrap_price_exchange AS DECIMAL(10, 2)) AS display_wrap_price_exchange,
            cast(o_raw.heel_type_price AS DECIMAL(10, 2)) AS heel_type_price,
            cast(o_raw.heel_type_price_exchange AS DECIMAL(10, 2)) AS heel_type_price_exchange,
            cast(o_raw.display_heel_type_price_exchange AS DECIMAL(10, 2)) AS display_heel_type_price_exchange,
            row_number () OVER (PARTITION BY o_raw.rec_id ORDER BY o_raw.xid DESC) AS rank,
            pt,
            hour
    FROM    pdb.fd_vb_order_goods
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type', 'kafka_old','rec_id','order_id', 'goods_style_id', 'sku', 'sku_id', 'goods_id', 'goods_name', 'goods_sn', 'goods_sku', 'goods_number', 'market_price', 'shop_price', 'shop_price_exchange', 'shop_price_amount_exchange', 'bonus', 'coupon_code', 'goods_attr', 'send_number', 'is_real', 'extension_code', 'parent_id', 'is_gift', 'goods_status', 'action_amt', 'action_reason_cat', 'action_note', 'carrier_bill_id', 'provider_id', 'invoice_num', 'return_points', 'return_bonus', 'biaoju_store_goods_id', 'subtitle', 'addtional_shipping_fee', 'style_id', 'customized', 'status_id', 'added_fee', 'custom_fee', 'custom_fee_exchange', 'plussize_fee', 'plussize_fee_exchange', 'rush_order_fee', 'rush_order_fee_exchange', 'coupon_goods_id', 'coupon_cat_id', 'coupon_config_value', 'coupon_config_coupon_type', 'styles', 'img_type', 'goods_gallery', 'goods_price_original', 'wrap_price', 'wrap_price_exchange', 'display_shop_price_exchange', 'display_shop_price_amount_exchange', 'display_custom_fee_exchange', 'display_plussize_fee_exchange', 'display_rush_order_fee_exchange', 'display_wrap_price_exchange', 'heel_type_price', 'heel_type_price_exchange', 'display_heel_type_price_exchange') o_raw
    AS `table`, ts, `commit`, xid, type, old, rec_id, order_id, goods_style_id, sku, sku_id, goods_id, goods_name, goods_sn, goods_sku, goods_number, market_price, shop_price, shop_price_exchange, shop_price_amount_exchange, bonus, coupon_code, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, custom_fee, custom_fee_exchange, plussize_fee, plussize_fee_exchange, rush_order_fee, rush_order_fee_exchange, coupon_goods_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, styles, img_type, goods_gallery, goods_price_original, wrap_price, wrap_price_exchange, display_shop_price_exchange, display_shop_price_amount_exchange, display_custom_fee_exchange, display_plussize_fee_exchange, display_rush_order_fee_exchange, display_wrap_price_exchange, heel_type_price, heel_type_price_exchange, display_heel_type_price_exchange
    WHERE pt >= '${hiveconf:pt}'
) inc where inc.rank = 1;
