CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_arc(
    `goods_id` bigint,
    `goods_party_id` bigint,
    `cat_id` bigint,
    `goods_sn` string,
    `sku` string,
    `goods_name` string,
    `goods_url_name` string,
    `brand_id` bigint,
    `goods_number` bigint,
    `goods_weight` decimal(15, 4),
    `goods_weight_bak` bigint,
    `market_price` decimal(15, 4),
    `shop_price` decimal(15, 4),
    `group_price` decimal(15, 4),
    `no_deal_price` decimal(15, 4),
    `wrap_price` decimal(15, 4),
    `fitting_price` decimal(15, 4),
    `promote_price` decimal(15, 4),
    `promote_start` string,
    `promote_end` string,
    `warn_number` bigint,
    `keywords` string,
    `goods_brief` string,
    `goods_desc` string,
    `goods_thumb` string,
    `goods_img` string,
    `original_img` string,
    `is_on_sale` bigint,
    `add_time` bigint,
    `is_delete` bigint,
    `is_promote` bigint,
    `last_update_time` string,
    `goods_type` bigint,
    `goods_details` string,
    `is_on_sale_pending` bigint,
    `top_cat_id` bigint,
    `sale_status` string,
    `is_display` bigint,
    `is_complete` bigint,
    `comment_count` bigint,
    `question_count` bigint,
    `is_new` bigint,
    `is_best_sellers` bigint,
    `fb_like_count` bigint,
    `model_card` string,
    `old_goods_id` bigint,
    `on_sale_time` string,
    `cp_goods_id` bigint
  )COMMENT '数据库同步过来的商品信息表'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_goods_arc PARTITION (dt='${hiveconf:dt}')
select  
    goods_id,
    goods_party_id,
    cat_id,
    goods_sn,
    sku,
    goods_name,
    goods_url_name,
    brand_id,
    goods_number,
    goods_weight,
    goods_weight_bak,
    market_price,
    shop_price,
    group_price,
    no_deal_price,
    wrap_price,
    fitting_price,
    promote_price,
    promote_start,
    promote_end,
    warn_number,
    keywords,
    goods_brief,
    goods_desc,
    goods_thumb,
    goods_img,
    original_img,
    is_on_sale,
    add_time,
    is_delete,
    is_promote,
    last_update_time,
    goods_type,
    goods_details,
    is_on_sale_pending,
    top_cat_id,
    sale_status,
    is_display,
    is_complete,
    comment_count,
    question_count,
    is_new,
    is_best_sellers,
    fb_like_count,
    model_card,
    old_goods_id,
    on_sale_time,
    cp_goods_id
from tmp.tmp_fd_goods_full;
