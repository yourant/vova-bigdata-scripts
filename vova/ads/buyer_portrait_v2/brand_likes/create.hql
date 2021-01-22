-- 用户画像品牌偏好统计表
drop table if exists ads.ads_buyer_portrait_brand_likes;
create table if  not exists  ads.ads_buyer_portrait_brand_likes (
    `buyer_id`                    bigint COMMENT 'd_买家id',
    `brand_id`                    bigint COMMENT 'd_品牌id',
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
) PARTITIONED BY (pt string) COMMENT '用户画像品牌偏好统计表'
     STORED AS PARQUETFILE;


-- 用户画像品牌偏好统计表品牌偏好度历史快照表
drop table if exists ads.ads_buyer_portrait_brand_likes_weight;
create table if  not exists  ads.ads_buyer_portrait_brand_likes_weight (
   `buyer_id`                    bigint COMMENT 'd_买家id',
   `brand_id`                    bigint COMMENT 'd_品牌id',
   `likes_weight`                decimal(13,4) COMMENt 'i_品牌偏好度'
)PARTITIONED BY (pt string) COMMENT '用户画像品牌偏好统计表品牌偏好度历史快照表' STORED AS PARQUETFILE;


-- 20200911
-- 新增字段 ads.ads_buyer_portrait_brand_likes 表中新增 min_clk_day_gap 用户最后一次点击该品牌到现在的时间
alter table ads.ads_buyer_portrait_brand_likes add columns(min_clk_day_gap string) cascade;
alter table ads.ads_buyer_portrait_brand_likes CHANGE COLUMN min_clk_day_gap min_clk_day_gap string comment '用户最后一次点击该品牌到现在的时间';
-- 新增表 ads.ads_buyer_portrait_brand_likes_exp
drop table if exists ads.ads_buyer_portrait_brand_likes_exp;
create table ads.ads_buyer_portrait_brand_likes_exp (
    `buyer_id`         bigint COMMENT 'd_买家id',
    `brand_id`         bigint COMMENT 'd_品牌id',
    `likes_weight`     decimal(10,6) COMMENt 'i_品牌偏好度'
    ) COMMENT '用户画像品牌偏好统计表品牌偏好度表(自然数计算)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


2020-09-23:

alter table ads.ads_buyer_portrait_brand_likes add columns(gmv_1w decimal(14, 4)) cascade;
alter table ads.ads_buyer_portrait_brand_likes CHANGE COLUMN gmv_1w gmv_1w decimal(14, 4) comment 'i_近7天品牌购买gmv';

alter table ads.ads_buyer_portrait_brand_likes add columns(gmv_15d decimal(14, 4)) cascade;
alter table ads.ads_buyer_portrait_brand_likes CHANGE COLUMN gmv_15d gmv_15d decimal(14, 4) comment 'i_近15天品牌购买gmv';

alter table ads.ads_buyer_portrait_brand_likes add columns(gmv_1m decimal(14, 4)) cascade;
alter table ads.ads_buyer_portrait_brand_likes CHANGE COLUMN gmv_1m gmv_1m decimal(14, 4) comment 'i_近30天品牌购买gmv';

-- 中期的定义： 近16-60天
ads.ads_buyer_portrait_brand_likes_medium_term
hadoop fs -du -h /user/hive/warehouse/ads.db/ads_buyer_portrait_brand_likes_medium_term
drop table if exists ads.ads_buyer_portrait_brand_likes_medium_term;
create table ads.ads_buyer_portrait_brand_likes_medium_term (
    `buyer_id`          bigint         COMMENT 'd_买家id',
    `brand_id`          bigint         COMMENT 'd_品牌id',
    `expre_cnt`         int            COMMENT 'i_曝光次数',
    `clk_cnt`           int            COMMENT 'i_点击次数',
    `clk_valid_cnt`     int            COMMENT 'i_有效点击次数',
    `collect_cnt`       int            COMMENT 'i_收藏次数',
    `add_cat_cnt`       int            COMMENT 'i_加购次数',
    `ord_cnt`           int            COMMENT 'i_购买次数',
    `gmv`               decimal(14, 4) COMMENT 'i_购买gmv',
    `min_clk_day_gap`   string         COMMENT 'i_用户最后一次点击该品牌到现在的时间'
) COMMENT '用户画像品牌中期偏好统计表(近16-60天)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

-- 长期的定义： 61天之前
ads.ads_buyer_portrait_brand_likes_long_term
hadoop fs -du -h /user/hive/warehouse/ads.db/ads_buyer_portrait_brand_likes_long_term
drop table if exists ads.ads_buyer_portrait_brand_likes_long_term;
create table ads.ads_buyer_portrait_brand_likes_long_term (
    `buyer_id`          bigint         COMMENT 'd_买家id',
    `brand_id`          bigint         COMMENT 'd_品牌id',
    `ord_cnt`           int            COMMENT 'i_购买次数',
    `gmv`               decimal(14, 4) COMMENT 'i_购买gmv'
    ) COMMENT '用户画像品牌长期偏好统计表(61天之前)'
  PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

-- 短中长期偏好， 综合得分
-- https://docs.google.com/spreadsheets/d/1ZeI4WjDXAM6c1fPwF2V4Q75GXoN0_Oh2Yr0RR5tZrus/edit#gid=306827190
alter table ads.ads_buyer_portrait_brand_likes_exp add columns (likes_weight_short decimal(10, 6)) cascade;
alter table ads.ads_buyer_portrait_brand_likes_exp CHANGE COLUMN likes_weight_short likes_weight_short  decimal(10, 6) comment 'i_品牌短期偏好度';

alter table ads.ads_buyer_portrait_brand_likes_exp add columns (likes_weight_medium decimal(10, 6)) cascade;
alter table ads.ads_buyer_portrait_brand_likes_exp CHANGE COLUMN likes_weight_medium likes_weight_medium  decimal(10, 6) comment 'i_品牌中期偏好度' cascade;

alter table ads.ads_buyer_portrait_brand_likes_exp add columns (likes_weight_long decimal(10, 6)) cascade;
alter table ads.ads_buyer_portrait_brand_likes_exp CHANGE COLUMN likes_weight_long likes_weight_long  decimal(10, 6) comment 'i_品牌长期偏好度';

alter table ads.ads_buyer_portrait_brand_likes_exp add columns (likes_weight_synth decimal(10, 6)) cascade;
alter table ads.ads_buyer_portrait_brand_likes_exp CHANGE COLUMN likes_weight_synth likes_weight_synth  decimal(10, 6) comment 'i_品牌综合偏好度';



alter table ads.ads_buyer_portrait_brand_likes_exp change column likes_weight likes_weight decimal(13,3) comment 'i_品牌偏好度';
alter table ads.ads_buyer_portrait_brand_likes_exp change column likes_weight_short likes_weight_short decimal(13,3) comment 'i_品牌短期偏好度';
alter table ads.ads_buyer_portrait_brand_likes_exp change column likes_weight_medium likes_weight_medium decimal(13,3) comment 'i_品牌中期偏好度';
alter table ads.ads_buyer_portrait_brand_likes_exp change column likes_weight_long likes_weight_long decimal(13,3) comment 'i_品牌长期偏好度';
alter table ads.ads_buyer_portrait_brand_likes_exp change column likes_weight_synth likes_weight_synth decimal(13,3) comment 'i_品牌综合偏好度';


