DROP TABLE ads.ads_fn_goods_portrait;
CREATE external TABLE IF NOT EXISTS ads.ads_fn_goods_portrait
(
    goods_id         bigint           COMMENT '商品id',
    datasource       string           COMMENT '数据源',
    cat_id           int              COMMENT '品类id',
    first_cat_id     int              COMMENT '一级品类id',
    price            int              COMMENT '价格（包括运费）',
    expre_cnt_1w     int              COMMENT '1周曝光量',
    clk_cnt_1w       int              COMMENT '一周点击量',
    add_cart_cnt_1w  int              COMMENT '一周加车量',
    collect_cnt_1w   int              COMMENT '一周加车量',
    sales_vol_1w     int              COMMENT '一周销量',
    ord_cnt_1w       int              COMMENT '一周订单量',
    gmv_1w           decimal(15, 4)   COMMENT '一周gmv'
) COMMENT 'fn商品画像表' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;