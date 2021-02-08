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
    `ord_cnt_1m`                  bigint COMMENT 'i_近30天品牌购买次数'
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
