-- 用户画像二级品类加品牌偏好统计表
drop table if exists ads.ads_vova_buyer_portrait_second_category_brand_likes;
create external table if  not exists  ads.ads_vova_buyer_portrait_second_category_brand_likes (
    `buyer_id`                    bigint COMMENT 'd_买家id',
    `second_cat_id`               bigint COMMENT 'd_品类id',
    `brand_id`                    bigint COMMENT 'i_品牌',
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
    `gmv_1w`                      decimal(13,2) COMMENT 'i_近7天gmv',
    `gmv_15d`                     decimal(13,2) COMMENT 'i_近15天gmv',
    `gmv_1m`                      decimal(13,2) COMMENT 'i_近30天gmv'
)COMMENT '用户画像二级品类加品牌偏好统计表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;
