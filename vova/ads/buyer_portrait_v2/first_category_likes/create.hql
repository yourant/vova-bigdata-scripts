
-- 用户画像一级品类价格区间偏好统计表品牌偏好度表
drop table if exists ads.ads_buyer_portrait_first_category_likes_weight;
create table if  not exists  ads.ads_buyer_portrait_first_category_likes_weight (
   `buyer_id`                    bigint COMMENT 'd_买家id',
   `first_cat_id`                bigint COMMENT 'd_一级品类id',
   `likes_weight`                decimal(13,4) COMMENt 'i_品牌偏好度'
)PARTITIONED BY (pt string) COMMENT '用户画像二级品类价格区间偏好统计表品牌偏好度表' STORED AS PARQUETFILE;
