drop table if exists ads.ads_vova_activity_outlets;
create external table if  not exists  ads.ads_vova_activity_outlets (
    `biz_type`                  string COMMENT 'i_biz_type',
    `event_type`                bigint COMMENT 'i_类型',
    `region_id`                 bigint COMMENT 'i_国家id',
    `first_cat_id`              bigint COMMENT 'i_一级品类id',
    `second_cat_id`             bigint COMMENT 'i_二级品类id',
    `goods_id`                  bigint COMMENT 'd_商品id',
    `rank`                      bigint COMMENT 'd_排名'
) COMMENT 'outlets活动数据' PARTITIONED BY (pt STRING)
     STORED AS PARQUETFILE;