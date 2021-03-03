-- women cloth 活动表
drop table if exists ads.ads_vova_activity_women_cloth;
create external table if  not exists  ads.ads_vova_activity_women_cloth (
    `goods_id`                  bigint COMMENT 'd_商品ID',
    `region_id`                 int    COMMENT 'd_国家id',
    `biz_type`                  STRING COMMENT 'd_biz type',
    `rp_type`                   int    COMMENT 'd_rp type',
    `first_cat_id`              int    COMMENT 'i_一级品类id',
    `second_cat_id`             int    COMMENT 'i_二级品类id',
    `rank`                      bigint COMMENT 'i_排名'
)COMMENT 'women cloth 活动表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;