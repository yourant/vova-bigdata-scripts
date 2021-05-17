https://docs.google.com/spreadsheets/d/1cDefnI7V9MOTiOX-H6Sy_Mju-4tBgfhSK-KEM423kRA/edit#gid=0
drop table if exists ads.ads_vova_activity_no_brand_goods_pool;
create external table if  not exists  ads.ads_vova_activity_no_brand_goods_pool (
    `goods_id`                  bigint COMMENT 'i_商品ID',
    `region_id`                 int    COMMENT 'i_国家id',
    `biz_type`                  STRING COMMENT 'i_biz type',
    `rp_type`                   int    COMMENT 'i_rp type',
    `first_cat_id`              int    COMMENT 'd_一级品类id',
    `second_cat_id`             int    COMMENT 'd_二级品类id',
    `rank`                      bigint COMMENT 'd_排名'
)COMMENT 'VOVA非Brand商品货品池需求表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;