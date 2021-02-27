create external table if  not exists  ads.ads_vova_goods_portrait_brand_price_range_likes_top20 (
    `goods_id`                    bigint COMMENT 'd_商品id',
    `brand_id`                    bigint COMMENT 'd_品牌',
    `price_range`                 bigint COMMENT 'd_价格区间',
    `rk`                          bigint COMMENt 'i_排名'
 ) COMMENT '商品画像品牌加价格区间偏好top10统计表' STORED AS PARQUETFILE;