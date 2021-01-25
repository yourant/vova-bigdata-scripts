-- 用户画像二级品类加价格区间偏好统计表
drop table if exists ads.ads_buyer_portrait_second_category_likes;
create table if  not exists  ads.ads_buyer_portrait_second_category_likes (
    `buyer_id`                    bigint COMMENT 'd_买家id',
    `second_cat_id`               bigint COMMENT 'd_品类id',
    `price_range`                 bigint COMMENT 'i_价格区间',
    `expre_cnt_1w`                bigint COMMENT 'i_近7天品牌曝光次数',
    `expre_cnt_15d`               bigint COMMENT 'i_近15天品牌曝光次数',
    `expre_cnt_1m`                bigint COMMENT 'i_近30天品牌曝光次数',
    `clk_cnt_1w`                  bigint COMMENT 'i_近7天品牌点击次数',
    `clk_cnt_15d`                 bigint COMMENT 'i_近15天品牌点击次数',
    `clk_cnt_1m`                  bigint COMMENT 'i_近30天品牌点击次数',
    `clk_valid_cnt_1w`            bigint COMMENT 'i_近7天品牌有效点击次数',
    `clk_valid_cnt_15d`           bigint COMMENT 'i_近15天品牌有效点击次数',
    `clk_valid_cnt_1m`            bigint COMMENT 'i_近30天品牌有效点击次数',
    `collect_cnt_1w`              bigint COMMENT 'i_近7天品牌收藏次数',
    `collect_cnt_15d`             bigint COMMENT 'i_近15天品牌收藏次数',
    `collect_cnt_1m`              bigint COMMENT 'i_近30天品牌收藏次数',
    `add_cat_cnt_1w`              bigint COMMENT 'i_近7天品牌加购次数',
    `add_cat_cnt_15d`             bigint COMMENT 'i_近15天品牌加购次数',
    `add_cat_cnt_1m`              bigint COMMENT 'i_近30天品牌加购次数',
    `ord_cnt_1w`                  bigint COMMENT 'i_近7天品牌购买次数',
    `ord_cnt_15d`                 bigint COMMENT 'i_近15天品牌购买次数',
    `ord_cnt_1m`                  bigint COMMENT 'i_近30天品牌购买次数'
)PARTITIONED BY (pt string)   COMMENT '用户画像二级品类加价格区间偏好统计表'
     STORED AS PARQUETFILE;

-- 用户画像二级品类价格区间偏好统计表品牌偏好度表
drop table if exists ads.ads_buyer_portrait_second_category_likes_weight;
create table if  not exists  ads.ads_buyer_portrait_second_category_likes_weight (
   `buyer_id`                    bigint COMMENT 'd_买家id',
   `second_cat_id`               bigint COMMENT 'd_二级品类id',
   `price_range`                 bigint COMMENT 'i_价格区间',
   `likes_weight`                decimal(13,4) COMMENt 'i_品牌偏好度'
)PARTITIONED BY (pt string) COMMENT '用户画像二级品类价格区间偏好统计表品牌偏好度表' STORED AS PARQUETFILE;
