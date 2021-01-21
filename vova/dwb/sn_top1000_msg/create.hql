drop table if exists dwb.dwb_vova_goods_sn_gmv_top1000_1w;
create table if  not exists dwb.dwb_vova_goods_sn_gmv_top1000_1w (
    `goods_sn`                      string        COMMENT 'd_商品id',
    `goods_id`                      bigint        COMMENT 'i_商品id',
    `virtual_goods_id`              bigint        COMMENT 'i_虚拟商品id',
    `first_cat_name`                string        COMMENT 'i_一级品类名称',
    `second_cat_name`               string        COMMENT 'i_二级品类名称',
    `gmv_1d`                        decimal(13,2) COMMENT 'i_前一日gmv',
    `gmv_7d`                        decimal(13,2) COMMENT 'i_近一日gmv',
    `brand_name`                    string        COMMENT 'i_品牌名称',
    `mct_name`                      string        COMMENT 'i_商家名称',
    `sn_min_shop_price`             decimal(13,2) COMMENT 'i_在售最低价'
)  COMMENT '7日gmv top1000商品表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;
