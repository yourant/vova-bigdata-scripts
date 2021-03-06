-- ac 品牌会场
drop table if exists ads.ads_vova_activity_user_only;
create external table if  not exists  ads.ads_vova_activity_user_only (
    `goods_id`                  bigint COMMENT 'i_商品ID',
    `region_id`                 int    COMMENT 'i_国家id',
    `biz_type`                  STRING COMMENT 'i_biz type',
    `rp_type`                   int    COMMENT 'i_rp type',
    `first_cat_id`              int    COMMENT 'd_一级品类id',
    `second_cat_id`             int    COMMENT 'd_二级品类id',
    `rank`                      bigint COMMENT 'd_排名'
)COMMENT 'user only 活动表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;