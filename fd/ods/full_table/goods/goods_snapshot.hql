CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods(
  `goods_id` int,
  `goods_party_id` int,
  `cat_id` smallint,
  `goods_sn` string,
  `sku` string,
  `goods_name` string,
  `goods_url_name` string,
  `brand_id` smallint,
  `goods_number` smallint,
  `goods_weight` double,
  `goods_weight_bak` int,
  `market_price` double,
  `shop_price` double,
  `group_price` double,
  `no_deal_price` double,
  `wrap_price` double,
  `fitting_price` double,
  `promote_price` double,
  `promote_start` string,
  `promote_end` string,
  `warn_number` int,
  `keywords` string,
  `goods_brief` string,
  `goods_desc` string,
  `goods_thumb` string,
  `goods_img` string,
  `original_img` string,
  `is_on_sale` tinyint,
  `add_time` int,
  `is_delete` tinyint,
  `is_promote` tinyint,
  `last_update_time` string,
  `goods_type` smallint,
  `goods_details` string,
  `is_on_sale_pending` tinyint,
  `top_cat_id` smallint,
  `sale_status` string,
  `is_display` tinyint,
  `is_complete` tinyint,
  `comment_count` int,
  `question_count` int,
  `is_new` tinyint,
  `is_best_sellers` tinyint,
  `fb_like_count` int,
  `model_card` string,
  `old_goods_id` int,
  `on_sale_time` string,
  `cp_goods_id` int
  )COMMENT '数据库同步过来的商品信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_goods_arc 
where dt = '${hiveconf:dt}';
