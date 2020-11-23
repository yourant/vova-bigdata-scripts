CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_goods_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    goods_id               bigint comment 'erp商品id(unique)',
    goods_party_id         bigint comment '分隔不同用户',
    cat_id                 bigint,
    goods_sn               string,
    sku                    string,
    goods_name             string comment '商品名',
    click_count            bigint,
    brand_id               bigint,
    provider_name          string,
    goods_number           bigint,
    goods_weight           decimal(15, 4),
    goods_volume           decimal(15, 4) comment '商品体积',
    market_price           decimal(15, 4),
    shop_price             decimal(15, 4),
    fitting_price          decimal(15, 4),
    promote_price          decimal(15, 4),
    promote_start          string,
    promote_end            string,
    warn_number            bigint,
    keywords               string,
    goods_brief            string,
    goods_desc             string,
    goods_thumb            string,
    goods_img              string,
    original_img           string,
    is_real                bigint,
    extension_code         string,
    is_on_sale             bigint,
    is_alone_sale          bigint,
    is_linked              bigint,
    is_basic               bigint,
    is_gift                bigint,
    can_handsel            bigint,
    integral               bigint,
    add_time               bigint,
    sort_order             bigint,
    is_delete              bigint,
    is_best                bigint,
    is_new                 bigint,
    is_hot                 bigint,
    is_promote             bigint,
    bonus_type_id          bigint,
    last_update            bigint,
    goods_type             bigint,
    seller_note            string,
    cycle_img              string,
    provider_id            bigint,
    goods_details          string,
    vote_times             bigint,
    vote_score             bigint,
    is_on_sale_pending     bigint,
    boost                  float,
    limit_integral         bigint,
    top_cat_id             bigint,
    adptional_shipping_fee bigint,
    vip_price              decimal(15, 4),
    is_vip                 bigint,
    is_remains             bigint,
    return_ratio           decimal(15, 4),
    customized             string,
    is_same_price          bigint,
    sale_status            string,
    sale_status_detail     string,
    commonsense            string,
    is_shield              bigint,
    is_display             bigint,
    price_range            decimal(15, 4) comment '降价区间',
    goods_name_short       string comment '商品简称',
    identify               string,
    suit                   string comment '适用机型',
    clerk_comment          string comment '小二点评',
    media_comment          string,
    os                     bigint,
    resolution             bigint,
    java_support           bigint,
    bill                   string,
    extra                  string comment '附加信息',
    barcode                string,
    uniq_sku               string comment '唯一的sku',
    is_maintain_weight     bigint,
    external_cat_id        bigint,
    is_batch               bigint,
    product_id             string,
    external_goods_id      bigint,
    erp_sku                string,
    create_time            bigint comment 'goods创建时间戳bigint'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_goods_inc  PARTITION (pt='${hiveconf:pt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.goods_id
    ,o_raw.goods_party_id
    ,o_raw.cat_id
    ,o_raw.goods_sn
    ,o_raw.sku
    ,o_raw.goods_name
    ,o_raw.click_count
    ,o_raw.brand_id
    ,o_raw.provider_name
    ,o_raw.goods_number
    ,o_raw.goods_weight
    ,o_raw.goods_volume
    ,o_raw.market_price
    ,o_raw.shop_price
    ,o_raw.fitting_price
    ,o_raw.promote_price
    ,o_raw.promote_start
    ,o_raw.promote_end
    ,o_raw.warn_number
    ,o_raw.keywords
    ,o_raw.goods_brief
    ,o_raw.goods_desc
    ,o_raw.goods_thumb
    ,o_raw.goods_img
    ,o_raw.original_img
    ,o_raw.is_real
    ,o_raw.extension_code
    ,o_raw.is_on_sale
    ,o_raw.is_alone_sale
    ,o_raw.is_linked
    ,o_raw.is_basic
    ,o_raw.is_gift
    ,o_raw.can_handsel
    ,o_raw.integral
    ,o_raw.add_time
    ,o_raw.sort_order
    ,o_raw.is_delete
    ,o_raw.is_best
    ,o_raw.is_new
    ,o_raw.is_hot
    ,o_raw.is_promote
    ,o_raw.bonus_type_id
    ,o_raw.last_update
    ,o_raw.goods_type
    ,o_raw.seller_note
    ,o_raw.cycle_img
    ,o_raw.provider_id
    ,o_raw.goods_details
    ,o_raw.vote_times
    ,o_raw.vote_score
    ,o_raw.is_on_sale_pending
    ,o_raw.boost
    ,o_raw.limit_integral
    ,o_raw.top_cat_id
    ,o_raw.adptional_shipping_fee
    ,o_raw.vip_price
    ,o_raw.is_vip
    ,o_raw.is_remains
    ,o_raw.return_ratio
    ,o_raw.customized
    ,o_raw.is_same_price
    ,o_raw.sale_status
    ,o_raw.sale_status_detail
    ,o_raw.commonsense
    ,o_raw.is_shield
    ,o_raw.is_display
    ,o_raw.price_range
    ,o_raw.goods_name_short
    ,o_raw.identify
    ,o_raw.suit
    ,o_raw.clerk_comment
    ,o_raw.media_comment
    ,o_raw.os
    ,o_raw.resolution
    ,o_raw.java_support
    ,o_raw.bill
    ,o_raw.extra
    ,o_raw.barcode
    ,o_raw.uniq_sku
    ,o_raw.is_maintain_weight
    ,o_raw.external_cat_id
    ,o_raw.is_batch
    ,o_raw.product_id
    ,o_raw.external_goods_id
    ,o_raw.erp_sku
    ,o_raw.create_time,
    ,hour as hour
from tmp.tmp_fd_ecs_goods
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'goods_id', 'goods_party_id', 'cat_id', 'goods_sn', 'sku', 'goods_name', 'click_count', 'brand_id', 'provider_name', 'goods_number', 'goods_weight', 'goods_volume', 'market_price', 'shop_price', 'fitting_price', 'promote_price', 'promote_start', 'promote_end', 'warn_number', 'keywords', 'goods_brief', 'goods_desc', 'goods_thumb', 'goods_img', 'original_img', 'is_real', 'extension_code', 'is_on_sale', 'is_alone_sale', 'is_linked', 'is_basic', 'is_gift', 'can_handsel', 'integral', 'add_time', 'sort_order', 'is_delete', 'is_best', 'is_new', 'is_hot', 'is_promote', 'bonus_type_id', 'last_update', 'goods_type', 'seller_note', 'cycle_img', 'provider_id', 'goods_details', 'vote_times', 'vote_score', 'is_on_sale_pending', 'boost', 'limit_integral', 'top_cat_id', 'adptional_shipping_fee', 'vip_price', 'is_vip', 'is_remains', 'return_ratio', 'customized', 'is_same_price', 'sale_status', 'sale_status_detail', 'commonsense', 'is_shield', 'is_display', 'price_range', 'goods_name_short', 'identify', 'suit', 'clerk_comment', 'media_comment', 'os', 'resolution', 'java_support', 'bill', 'extra', 'barcode', 'uniq_sku', 'is_maintain_weight', 'external_cat_id', 'is_batch', 'product_id', 'external_goods_id', 'erp_sku', 'create_time') o_raw
AS `table`, ts, `commit`, xid, type, old, goods_id, goods_party_id, cat_id, goods_sn, sku, goods_name, click_count, brand_id, provider_name, goods_number, goods_weight, goods_volume, market_price, shop_price, fitting_price, promote_price, promote_start, promote_end, warn_number, keywords, goods_brief, goods_desc, goods_thumb, goods_img, original_img, is_real, extension_code, is_on_sale, is_alone_sale, is_linked, is_basic, is_gift, can_handsel, integral, add_time, sort_order, is_delete, is_best, is_new, is_hot, is_promote, bonus_type_id, last_update, goods_type, seller_note, cycle_img, provider_id, goods_details, vote_times, vote_score, is_on_sale_pending, boost, limit_integral, top_cat_id, adptional_shipping_fee, vip_price, is_vip, is_remains, return_ratio, customized, is_same_price, sale_status, sale_status_detail, commonsense, is_shield, is_display, price_range, goods_name_short, identify, suit, clerk_comment, media_comment, os, resolution, java_support, bill, extra, barcode, uniq_sku, is_maintain_weight, external_cat_id, is_batch, product_id, external_goods_id, erp_sku, create_time
where pt = '${hiveconf:pt}';
