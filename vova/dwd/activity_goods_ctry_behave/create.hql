create external table if  not exists  dwd.dwd_vova_activity_goods_ctry_behave (
    `goods_id`                  bigint    COMMENT 'd_商品id',
    `first_cat_id`              int       COMMENT 'i_一级品类id',
    `second_cat_id`             int       COMMENT 'i_二级品类id',
    `region_id`                 int       COMMENT 'd_国家id',
    `is_brand`                  int       COMMENT 'i_是否品牌，1.是品牌，0.不是品牌',
    `expre_cnt`                 int       COMMENT 'i_曝光量',
    `clk_cnt`                   int       COMMENT 'i_点击量',
    `ord_cnt`                   int       COMMENT 'i_订单量',
    `gmv`                       decimal(13,2)      COMMENT 'i_gmv',
    `expre_uv`                  int       COMMENT 'i_曝光uv',
    `click_uv`                  int       COMMENT 'i_点击uv',
    `sales_vol`                 int       COMMENT 'i_销量'
) COMMENT '活动商品表现表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;