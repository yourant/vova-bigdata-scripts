drop table ads.ads_vova_goods_id_behave_2m;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_id_behave_2m
(
    goods_id                  bigint COMMENT '商品id',
    virtual_goods_id          bigint COMMENT '虚拟id',
    goods_sn                  string COMMENT 'goods_sn',
    date_diff                 bigint COMMENT '在架天数',
    expre_uv                  bigint COMMENT '曝光uv',
    expre                     bigint COMMENT '曝光量',
    avg_expre                 decimal(15, 4) COMMENT '日均曝光量',
    clk_uv                    bigint COMMENT '点击uv',
    clk                       bigint COMMENT '点击量',
    avg_clk                   decimal(15, 4) COMMENT '日均点击量',
    cart_pv                   bigint COMMENT '加车量',
    avg_cart_pv               decimal(15, 4) COMMENT '日均加车量',
    sales_order               bigint COMMENT '销量',
    avg_sales_order           decimal(15, 4) COMMENT '日均销量',
    gmv                       decimal(15, 4) COMMENT 'gmv',
    avg_gmv                   decimal(15, 4) COMMENT '日均gmv',
    avg_sor_div_clk           decimal(15, 4) COMMENT '日均销量/日均点击量',
    ctr                       decimal(15, 4) COMMENT 'ctr',
    cr                        decimal(15, 4) COMMENT '支付转化率',
    gcr                       decimal(15, 4) COMMENT 'gcr',
    cart_pv_div_clk           decimal(15, 4) COMMENT '加购量/点击量'
) COMMENT 'goods_id_behave_2m'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

