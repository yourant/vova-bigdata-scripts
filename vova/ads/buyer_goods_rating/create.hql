drop table ads.ads_vova_buyer_goods_rating;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_buyer_goods_rating
(
    user_id                bigint COMMENT '用户id',
    goods_id               bigint COMMENT '商品id',
    rating                 double COMMENT '评分'
) COMMENT '用户商品评分'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE external TABLE tmp.tmp_ads_vova_buyer_goods_rating_expre(
  `buyer_id` bigint,
  `virtual_goods_id` bigint,
  `expre_rating` double,
  `clk_rating` double,
  `cart_rating` double,
  `wish_rating` double,
  `order_rating` double)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE external TABLE tmp.tmp_ads_vova_buyer_goods_rating_clk(
  `buyer_id` bigint,
  `virtual_goods_id` bigint,
  `expre_rating` double,
  `clk_rating` double,
  `cart_rating` double,
  `wish_rating` double,
  `order_rating` double)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE external TABLE tmp.tmp_ads_vova_buyer_goods_rating_cart(
  `buyer_id` bigint,
  `virtual_goods_id` bigint,
  `expre_rating` double,
  `clk_rating` double,
  `cart_rating` double,
  `wish_rating` double,
  `order_rating` double)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE external TABLE tmp.tmp_ads_vova_buyer_goods_rating_wish(
  `buyer_id` bigint,
  `virtual_goods_id` bigint,
  `expre_rating` double,
  `clk_rating` double,
  `cart_rating` double,
  `wish_rating` double,
  `order_rating` double)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE external TABLE tmp.tmp_ads_vova_buyer_goods_rating_order(
  `buyer_id` bigint,
  `virtual_goods_id` bigint,
  `expre_rating` double,
  `clk_rating` double,
  `cart_rating` double,
  `wish_rating` double,
  `order_rating` double)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
