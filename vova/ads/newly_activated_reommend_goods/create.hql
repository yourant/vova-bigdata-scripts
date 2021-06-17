drop table if exists ads.ads_vova_newly_activated_recommend_goods;
create external table if  not exists  ads.ads_vova_newly_activated_recommend_goods (
    `goods_id`                  bigint COMMENT 'i_商品ID',
    `first_cat_id`              int    COMMENT 'd_一级品类id',
    `second_cat_id`             int    COMMENT 'd_二级品类id'
)COMMENT '#9805 新客推荐商品增加评论和退款率规则' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;

