-- 用户画像二级品类偏好统计表偏好度表(ads_buyer_portrait_second_category_likes_weight代表是二级品类加价格区间，故此加上v2以示区别)
drop table if exists ads.ads_buyer_portrait_second_category_likes_weight_v2;
create table if  not exists  ads.ads_buyer_portrait_second_category_likes_weight_v2 (
   `buyer_id`                    bigint COMMENT 'd_买家id',
   `second_cat_id`               bigint COMMENT 'd_二级品类id',
   `likes_weight`                decimal(13,4) COMMENt 'i_品牌偏好度'
)PARTITIONED BY (pt string) COMMENT '用户画像二级品类偏好统计表偏好度表(ads_buyer_portrait_second_category_likes_weight代表是二级品类加价格区间，故此加上v2以示区别)' STORED AS PARQUETFILE;
