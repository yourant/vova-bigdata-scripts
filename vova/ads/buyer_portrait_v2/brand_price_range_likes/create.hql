-- 用户画像品牌加价格区间偏好统计表
drop table if exists ads.ads_buyer_portrait_brand_price_range_likes_weight;
create table if  not exists  ads.ads_buyer_portrait_brand_price_range_likes_weight (
    `buyer_id`                    bigint COMMENT 'd_买家id',
    `brand_id`                    bigint COMMENT 'd_品牌',
    `price_range`                 bigint COMMENT 'd_价格区间',
    `likes_weight`                decimal(13,4) COMMENt 'i_品牌偏好度'
 )PARTITIONED BY (pt string) COMMENT '用户画像品牌加价格区间偏好统计表' STORED AS PARQUETFILE;


 create table if  not exists  ads.ads_buyer_portrait_brand_price_range_likes_top10 (
     `buyer_id`                    bigint COMMENT 'd_买家id',
     `brand_id`                    bigint COMMENT 'd_品牌',
     `price_range`                 bigint COMMENT 'd_价格区间',
     `rk`                          bigint COMMENt 'i_排名'
  ) COMMENT '用户画像品牌加价格区间偏好top10统计表' STORED AS PARQUETFILE;

 create table if  not exists  ads.ads_buyer_portrait_brand_price_range_likes_top10_b (
     `buyer_id`                    bigint COMMENT 'd_买家id',
     `brand_id`                    bigint COMMENT 'd_品牌',
     `price_range`                 bigint COMMENT 'd_价格区间',
     `rk`                          bigint COMMENt 'i_排名'
  ) COMMENT '用户画像品牌加价格区间偏好top10统计表' STORED AS PARQUETFILE;


