
CREATE external TABLE `ads.ads_vova_buyer_portrait_second_category_likes_with_click_15d`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `second_cat_id` bigint COMMENT 'd_品类id',
  `price_range` bigint COMMENT 'i_价格区间',
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
  `ord_cnt_1m` bigint COMMENT 'i_近30天品牌购买次数')
COMMENT '用户画像二级品类加价格区间偏好统计表(有近15日点击数据)'  STORED AS PARQUETFILE;

drop table if exists ads.ads_vova_buyer_portrait_second_category_price_range_likes;
CREATE external table ads.ads_vova_buyer_portrait_second_category_price_range_likes (
    `buyer_id`          bigint COMMENT 'd_买家id',
    `second_cat_id`     int    COMMENT 'd_二级品类id',
    `price_range`       int    COMMENT 'd_价格区间',
    `expre_cnt_1w`      int    COMMENT 'i_近7天品类曝光次数',
    `expre_cnt_15d`     int    COMMENT 'i_近15天品类曝光次数',
    `expre_cnt_1m`      int    COMMENT 'i_近30天品类曝光次数',
    `clk_cnt_1w`        int    COMMENT 'i_近7天品类点击次数',
    `clk_cnt_15d`       int    COMMENT 'i_近15天品类点击次数',
    `clk_cnt_1m`        int    COMMENT 'i_近30天品类点击次数',
    `clk_valid_cnt_1w`  int    COMMENT 'i_近7天品类有效点击次数',
    `clk_valid_cnt_15d` int    COMMENT 'i_近15天品类有效点击次数',
    `clk_valid_cnt_1m`  int    COMMENT 'i_近30天品类有效点击次数',
    `collect_cnt_1w`    int    COMMENT 'i_近7天品类收藏次数',
    `collect_cnt_15d`   int    COMMENT 'i_近15天品类收藏次数',
    `collect_cnt_1m`    int    COMMENT 'i_近30天品类收藏次数',
    `add_cat_cnt_1w`    int    COMMENT 'i_近7天品类加购次数',
    `add_cat_cnt_15d`   int    COMMENT 'i_近15天品类加购次数',
    `add_cat_cnt_1m`    int    COMMENT 'i_近30天品类加购次数',
    `ord_cnt_1w`        int    COMMENT 'i_近7天品类购买次数',
    `ord_cnt_15d`       int    COMMENT 'i_近15天品类购买次数',
    `ord_cnt_1m`        int    COMMENT 'i_近30天品类购买次数',
    `gmv_1w`            decimal(14, 4) COMMENT 'i_近7天品类购买gmv',
    `gmv_15d`           decimal(14, 4) COMMENT 'i_近15天品类购买gmv',
    `gmv_1m`            decimal(14, 4) COMMENT 'i_近30天品类购买gmv',
    `min_clk_day_gap`   string COMMENT 'i_用户最后一次点击该二级品类到现在的时间'
) COMMENT '用户画像二级品类价格区间偏好统计表'
  PARTITIONED BY (pt string)
  STORED AS PARQUETFILE;



drop table if exists ads.ads_vova_buyer_portrait_second_category_likes_price_range_exp;
CREATE external TABLE `ads.ads_vova_buyer_portrait_second_category_likes_price_range_exp`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `second_cat_id` bigint COMMENT 'd_二级品类id',
  `price_range` int COMMENT 'd_价格区间',
  `likes_weight` decimal(10,6) COMMENT 'i_品牌偏好度',
  `likes_weight_short` decimal(13,3) COMMENT 'i_短期偏好度',
  `likes_weight_medium` decimal(13,3) COMMENT 'i_中期偏好度',
  `likes_weight_long` decimal(13,3) COMMENT 'i_长期偏好度',
  `likes_weight_synth` decimal(13,3) COMMENT 'i_综合偏好度')
   COMMENT '用户画像二级品类加价格区间偏好统计表品牌偏好度表(自然数计算)'
  PARTITIONED BY (pt string)
   STORED AS PARQUETFILE;




2020-09-24:
-- 中期的定义： 近16-60天
drop table if exists ads.ads_vova_buyer_portrait_second_category_likes_price_range_medium_term;
create table ads.ads_vova_buyer_portrait_second_category_likes_price_range_medium_term (
    `buyer_id`          bigint         COMMENT 'd_买家id',
    `second_cat_id`     bigint         COMMENT 'd_二级品类id',
    `price_range`       int            COMMENT 'd_价格区间',
    `expre_cnt`         int            COMMENT 'i_曝光次数',
    `clk_cnt`           int            COMMENT 'i_点击次数',
    `clk_valid_cnt`     int            COMMENT 'i_有效点击次数',
    `collect_cnt`       int            COMMENT 'i_收藏次数',
    `add_cat_cnt`       int            COMMENT 'i_加购次数',
    `ord_cnt`           int            COMMENT 'i_购买次数',
    `gmv`               decimal(14, 4) COMMENT 'i_购买gmv',
    `min_clk_day_gap`   string         COMMENT 'i_用户最后一次点击到现在的时间'
) COMMENT '用户画像二级品类加价格区间中期偏好统计表(近16-60天)'
  PARTITIONED BY (pt string) STORED AS PARQUETFILE;


-- 长期的定义： 61天之前
drop table if exists ads.ads_vova_buyer_portrait_second_category_likes_price_range_long_term;
create external table ads.ads_vova_buyer_portrait_second_category_likes_price_range_long_term (
    `buyer_id`          bigint         COMMENT 'd_买家id',
    `second_cat_id`     bigint         COMMENT 'd_二级品类id',
    `price_range`       int            COMMENT 'd_价格区间',
    `ord_cnt`           int            COMMENT 'i_购买次数',
    `gmv`               decimal(14, 4) COMMENT 'i_购买gmv'
    ) COMMENT '用户画像二级品类加价格区间长期偏好统计表(61天之前)'
  PARTITIONED BY (pt string)  STORED AS PARQUETFILE;


-- 短中长期偏好， 综合得分
-- https://docs.google.com/spreadsheets/d/1ZeI4WjDXAM6c1fPwF2V4Q75GXoN0_Oh2Yr0RR5tZrus/edit#gid=306827190