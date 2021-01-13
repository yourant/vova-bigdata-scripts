--商品核心数据表
drop table if exists dws.dws_vova_goods_core_feature_his;
create table if  not exists  dws.dws_vova_goods_core_feature_his (
    `goods_id`                    bigint COMMENT 'd_商品id',
    `expre_cnt`                   bigint COMMENT 'i_曝光数',
    `cr`                          decimal(13,2) COMMENT 'i_转化率',
    `ctr`                         decimal(13,2) COMMENT 'i_点击率',
    `gmv`                         decimal(13,2) COMMENT 'i_gmv',
    `shop_price`                  decimal(13,2) COMMENT 'i_售价'
) PARTITIONED BY ( pt string)
 COMMENT '商品核心数据表历史表'
     STORED AS PARQUETFILE;





