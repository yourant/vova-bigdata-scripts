CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_goods_expre_analysis_h
(
    goods_id           bigint        comment '商品id',
    first_cat_name     String        comment '一级品类',
    is_brand           bigint        comment '是否brand',
    page_code          String        comment '页面代码',
    expre_cnt          bigint        comment '曝光数量',
    mct_rank           bigint        comment '商家等级'
) COMMENT '分页面商品曝光量小时表' PARTITIONED BY (pt String, hour String) STORED AS PARQUETFILE;