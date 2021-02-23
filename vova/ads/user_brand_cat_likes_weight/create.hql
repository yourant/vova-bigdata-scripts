create external table if  not exists  ads.ads_vova_user_brand_cat_likes_weight (
    `user_id`                     bigint COMMENT 'd_买家id',
    `brand`                       STRING COMMENT 'i_品牌偏好top20压缩字符串',
    `first_cat`                   STRING COMMENT 'i_一级品类偏好top10压缩字符串',
    `second_cat`                  STRING COMMENT 'i_二级品类偏好top10压缩字符串'
) COMMENT '品牌品类排序-数据需求表'
     STORED AS PARQUETFILE;


create external table if  not exists  ads.ads_vova_default_brand_cat_likes_weight (
    `type`                        bigint COMMENT '1：first_cat，1：second_cat，3：brand',
    `biz_id`                      bigint COMMENT '该type下的id',
    `count`                       bigint COMMENT '统计权重'
) COMMENT '默认品牌品类排序-数据需求表'
     STORED AS PARQUETFILE;