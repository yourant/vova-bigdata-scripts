-- 运营精选活动数据

drop table if exists ads.ads_vova_activity_daily_selection;
create external table if  not exists  ads.ads_vova_activity_daily_selection (
    `biz_type`                  string COMMENT 'i_biz_type',
    `event_type`                int    COMMENT 'i_类型',
    `region_id`                 bigint COMMENT 'i_国家id',
    `first_cat_id`              bigint COMMENT 'i_一级品类id',
    `goods_id`                  bigint COMMENT 'd_商品id',
    `rank`                      bigint COMMENT 'd_排名'
)COMMENT 'deaily se_v2数据' PARTITIONED BY (pt STRING)
     STORED AS PARQUETFILE;