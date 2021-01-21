ads.ads_buyer_portrait_first_category_likes
hadoop fs -du -h /user/hive/warehouse/ads.db/ads_buyer_portrait_first_category_likes

drop table if exists ads.ads_buyer_portrait_first_category_likes;
create table ads.ads_buyer_portrait_first_category_likes (
    `buyer_id`                    bigint COMMENT 'd_买家id',
    `first_cat_id`                bigint COMMENT 'd_一级品类id',
    `expre_cnt_1w`                int    COMMENT 'i_近7天品牌曝光次数',
    `expre_cnt_15d`               int    COMMENT 'i_近15天品牌曝光次数',
    `expre_cnt_1m`                int    COMMENT 'i_近30天品牌曝光次数',
    `clk_cnt_1w`                  int    COMMENT 'i_近7天品牌点击次数',
    `clk_cnt_15d`                 int    COMMENT 'i_近15天品牌点击次数',
    `clk_cnt_1m`                  int    COMMENT 'i_近30天品牌点击次数',
    `clk_valid_cnt_1w`            int    COMMENT 'i_近7天品牌有效点击次数',
    `clk_valid_cnt_15d`           int    COMMENT 'i_近15天品牌有效点击次数',
    `clk_valid_cnt_1m`            int    COMMENT 'i_近30天品牌有效点击次数',
    `collect_cnt_1w`              int    COMMENT 'i_近7天品牌收藏次数',
    `collect_cnt_15d`             int    COMMENT 'i_近15天品牌收藏次数',
    `collect_cnt_1m`              int    COMMENT 'i_近30天品牌收藏次数',
    `add_cat_cnt_1w`              int    COMMENT 'i_近7天品牌加购次数',
    `add_cat_cnt_15d`             int    COMMENT 'i_近15天品牌加购次数',
    `add_cat_cnt_1m`              int    COMMENT 'i_近30天品牌加购次数',
    `ord_cnt_1w`                  int    COMMENT 'i_近7天品牌购买次数',
    `ord_cnt_15d`                 int    COMMENT 'i_近15天品牌购买次数',
    `ord_cnt_1m`                  int    COMMENT 'i_近30天品牌购买次数',
    `gmv_1w`                      decimal(14, 4) COMMENT 'i_近7天品牌购买gmv',
    `gmv_15d`                     decimal(14, 4) COMMENT 'i_近15天品牌购买gmv',
    `gmv_1m`                      decimal(14, 4) COMMENT 'i_近30天品牌购买gmv',
    `min_clk_day_gap`             string COMMENT 'i_用户最后一次点击该一级品类到现在的时间'
) COMMENT '用户画像一级品类偏好统计表'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


ads.ads_buyer_portrait_first_category_likes_exp
hadoop fs -du -h /user/hive/warehouse/ads.db/ads_buyer_portrait_first_category_likes_exp
drop table if exists ads.ads_buyer_portrait_first_category_likes_exp;
create table ads.ads_buyer_portrait_first_category_likes_exp (
    `buyer_id`         bigint COMMENT 'd_买家id',
    `first_cat_id`     bigint COMMENT 'd_一级品类id',
    `likes_weight`     decimal(10,6) COMMENt 'i_一级品类偏好度'
) COMMENT '用户画像一级品类偏好统计表品牌偏好度表(自然数计算)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


2020-09-23:
-- 中期的定义： 近16-60天
ads.ads_buyer_portrait_first_category_likes_medium_term
hadoop fs -du -h /user/hive/warehouse/ads.db/ads_buyer_portrait_first_category_likes_medium_term
drop table if exists ads.ads_buyer_portrait_first_category_likes_medium_term;
create table ads.ads_buyer_portrait_first_category_likes_medium_term (
    `buyer_id`          bigint         COMMENT 'd_买家id',
    `first_cat_id`      bigint         COMMENT 'd_一级品类id',
    `expre_cnt`         int            COMMENT 'i_曝光次数',
    `clk_cnt`           int            COMMENT 'i_点击次数',
    `clk_valid_cnt`     int            COMMENT 'i_有效点击次数',
    `collect_cnt`       int            COMMENT 'i_收藏次数',
    `add_cat_cnt`       int            COMMENT 'i_加购次数',
    `ord_cnt`           int            COMMENT 'i_购买次数',
    `gmv`               decimal(14, 4) COMMENT 'i_购买gmv',
    `min_clk_day_gap`   string         COMMENT 'i_用户最后一次点击该一级品类到现在的时间'
) COMMENT '用户画像一级品类中期偏好统计表(近16-60天)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


-- 长期的定义： 61天之前
ads.ads_buyer_portrait_first_category_likes_long_term
hadoop fs -du -h /user/hive/warehouse/ads.db/ads_buyer_portrait_first_category_likes_long_term
drop table if exists ads.ads_buyer_portrait_first_category_likes_long_term;
create table ads.ads_buyer_portrait_first_category_likes_long_term (
    `buyer_id`          bigint         COMMENT 'd_买家id',
    `first_cat_id`      bigint         COMMENT 'd_一级品类id',
    `ord_cnt`           int            COMMENT 'i_购买次数',
    `gmv`               decimal(14, 4) COMMENT 'i_购买gmv'
    ) COMMENT '用户画像一级品类长期偏好统计表(61天之前)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


-- 短中长期偏好， 综合得分
-- https://docs.google.com/spreadsheets/d/1ZeI4WjDXAM6c1fPwF2V4Q75GXoN0_Oh2Yr0RR5tZrus/edit#gid=306827190

alter table ads.ads_buyer_portrait_first_category_likes_exp add columns (likes_weight_short decimal(10, 6)) cascade;
alter table ads.ads_buyer_portrait_first_category_likes_exp CHANGE COLUMN likes_weight_short likes_weight_short  decimal(10, 6) comment 'i_一级品类短期偏好度';

alter table ads.ads_buyer_portrait_first_category_likes_exp add columns (likes_weight_medium decimal(10, 6)) cascade;
alter table ads.ads_buyer_portrait_first_category_likes_exp CHANGE COLUMN likes_weight_medium likes_weight_medium  decimal(10, 6) comment 'i_一级品类中期偏好度' cascade;

alter table ads.ads_buyer_portrait_first_category_likes_exp add columns (likes_weight_long decimal(10, 6)) cascade;
alter table ads.ads_buyer_portrait_first_category_likes_exp CHANGE COLUMN likes_weight_long likes_weight_long  decimal(10, 6) comment 'i_一级品类长期偏好度';

alter table ads.ads_buyer_portrait_first_category_likes_exp add columns (likes_weight_synth decimal(10, 6)) cascade;
alter table ads.ads_buyer_portrait_first_category_likes_exp CHANGE COLUMN likes_weight_synth likes_weight_synth  decimal(10, 6) comment 'i_一级品类综合偏好度';

alter table ads.ads_buyer_portrait_first_category_likes_exp change column likes_weight_short likes_weight_short decimal(13,3) comment 'i_短期偏好度';
alter table ads.ads_buyer_portrait_first_category_likes_exp change column likes_weight_medium likes_weight_medium decimal(13,3) comment 'i_中期偏好度';
alter table ads.ads_buyer_portrait_first_category_likes_exp change column likes_weight_long likes_weight_long decimal(13,3) comment 'i_长期偏好度';
alter table ads.ads_buyer_portrait_first_category_likes_exp change column likes_weight_synth likes_weight_synth decimal(13,3) comment 'i_综合偏好度';


