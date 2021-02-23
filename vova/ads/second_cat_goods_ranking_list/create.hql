-- req6962，二级品类商品榜单表
drop table if exists ads.ads_vova_second_cat_goods_ranking_list;
create external table if  not exists  ads.ads_vova_second_cat_goods_ranking_list (
    `goods_id`                      int COMMENT 'i_商品id',
    `region_id`                     int COMMENT 'i_国家id',
    `second_cat_id`                 int COMMENT 'd_一级品类id',
    `is_brand`                      int COMMENT 'd_是否brand',
    `list_type`                     int COMMENT 'i_榜单类型，etc:1.热销榜，2.好评榜，3.人气榜',
    `list_val`                      int COMMENT 'd_榜单值，热销榜代表销量，好评表代表好评数，人气榜代表人气数',
    `rank`                          int COMMENT 'd_排名'
)COMMENT '二级品类商品榜单表'  PARTITIONED BY ( pt string)
     STORED AS PARQUETFILE;