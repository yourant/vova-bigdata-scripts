-- 用户画像子品类偏好统计表
drop table if exists ads.ads_vova_buyer_portrait_category_likes;
create table if  not exists  ads.ads_vova_buyer_portrait_category_likes (
    `buyer_id`                    bigint COMMENT 'd_买家id',
    `cat_id`                      bigint COMMENT 'd_品类id',
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
    `ord_cnt_1m`                  bigint COMMENT 'i_近30天品牌购买次数',
    `gmv_1w`            decimal(14, 4) COMMENT 'i_近7天品类购买gmv',
    `gmv_15d`           decimal(14, 4) COMMENT 'i_近15天品类购买gmv',
    `gmv_1m`            decimal(14, 4) COMMENT 'i_近30天品类购买gmv',
    `min_clk_day_gap`   string COMMENT 'i_用户最后一次点击该二级品类到现在的时间'
)COMMENT '用户画像子品类偏好统计表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;





CREATE TABLE `ads.ads_vova_buyer_portrait_category_likes_with_click_15d`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `cat_id` bigint COMMENT 'd_品类id',
  `expre_cnt_1w` bigint COMMENT 'i_近7天品牌曝光次数',
  `expre_cnt_15d` bigint COMMENT 'i_近15天品牌曝光次数',
  `expre_cnt_1m` bigint COMMENT 'i_近30天品牌曝光次数',
  `clk_cnt_1w` bigint COMMENT 'i_近7天品牌点击次数',
  `clk_cnt_15d` bigint COMMENT 'i_近15天品牌点击次数',
  `clk_cnt_1m` bigint COMMENT 'i_近30天品牌点击次数',
  `clk_valid_cnt_1w` bigint COMMENT 'i_近7天品牌有效点击次数',
  `clk_valid_cnt_15d` bigint COMMENT 'i_近15天品牌有效点击次数',
  `clk_valid_cnt_1m` bigint COMMENT 'i_近30天品牌有效点击次数',
  `collect_cnt_1w` bigint COMMENT 'i_近7天品牌收藏次数',
  `collect_cnt_15d` bigint COMMENT 'i_近15天品牌收藏次数',
  `collect_cnt_1m` bigint COMMENT 'i_近30天品牌收藏次数',
  `add_cat_cnt_1w` bigint COMMENT 'i_近7天品牌加购次数',
  `add_cat_cnt_15d` bigint COMMENT 'i_近15天品牌加购次数',
  `add_cat_cnt_1m` bigint COMMENT 'i_近30天品牌加购次数',
  `ord_cnt_1w` bigint COMMENT 'i_近7天品牌购买次数',
  `ord_cnt_15d` bigint COMMENT 'i_近15天品牌购买次数',
  `ord_cnt_1m` bigint COMMENT 'i_近30天品牌购买次数'
  ) COMMENT '用户画像子品类偏好统计表(有近15日点击数据)' STORED AS PARQUETFILE;


drop table if exists ads.ads_vova_buyer_portrait_category_likes_medium_term;
create external table ads.ads_vova_buyer_portrait_category_likes_medium_term (
    `buyer_id`          bigint         COMMENT 'd_买家id',
    `cat_id`            bigint         COMMENT 'd_二级品类id',
    `expre_cnt`         int            COMMENT 'i_曝光次数',
    `clk_cnt`           int            COMMENT 'i_点击次数',
    `clk_valid_cnt`     int            COMMENT 'i_有效点击次数',
    `collect_cnt`       int            COMMENT 'i_收藏次数',
    `add_cat_cnt`       int            COMMENT 'i_加购次数',
    `ord_cnt`           int            COMMENT 'i_购买次数',
    `gmv`               decimal(14, 4) COMMENT 'i_购买gmv',
    `min_clk_day_gap`   string         COMMENT 'i_用户最后一次点击该二级品类到现在的时间'
) COMMENT '用户画像子品类中期偏好统计表(近16-60天)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


-- 长期的定义： 61天之前
drop table if exists ads.ads_vova_buyer_portrait_category_likes_long_term;
create external table ads.ads_vova_buyer_portrait_category_likes_long_term (
    `buyer_id`          bigint         COMMENT 'd_买家id',
    `cat_id`     bigint         COMMENT 'd_二级品类id',
    `ord_cnt`           int            COMMENT 'i_购买次数',
    `gmv`               decimal(14, 4) COMMENT 'i_购买gmv'
    ) COMMENT '用户画像子品类长期偏好统计表(61天之前)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table if exists ads.ads_vova_buyer_portrait_category_likes_exp;
CREATE external TABLE `ads.ads_vova_buyer_portrait_category_likes_exp`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `cat_id` bigint COMMENT   'd_子品类id',
  `likes_weight` decimal(10,6) COMMENT 'i_二级品类偏好度',
  `likes_weight_short` decimal(13,3) COMMENT 'i_短期偏好度',
  `likes_weight_medium` decimal(13,3) COMMENT 'i_中期偏好度',
  `likes_weight_long` decimal(13,3) COMMENT 'i_长期偏好度',
  `likes_weight_synth` decimal(13,3) COMMENT 'i_综合偏好度')
COMMENT '用户画像子品类偏好统计表品牌偏好度表(自然数计算)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
