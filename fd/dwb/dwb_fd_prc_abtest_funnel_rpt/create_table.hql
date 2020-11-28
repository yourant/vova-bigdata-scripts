CREATE TABLE IF NOT EXISTS dwb.dwb_fd_prc_abtest_funnel_rpt
(
    project                    string,
    platform_type              string,
    country                    string,
    app_version                string,
    abtest_name                string,
    abtest_version             string,

    uv                bigint,
    homepage_uv       bigint,
    list_uv           bigint,
    product_uv        bigint,
    cart_uv            bigint,
    add_uv            bigint,
    remove_uv          bigint,
    checkout_uv        bigint,
    checkout_option_uv bigint,
    purchase_uv        bigint,
    checkout_page_uv  bigint,
    orders                   bigint,
    goods_amount               DECIMAL(15, 4) comment '商品美元价格总和',
    bonus                      DECIMAL(15, 4) comment '订单折扣美元价格,负数',
    shipping_fee               DECIMAL(15, 4) comment '订单运费美元价格'

)
    PARTITIONED BY ( pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS ORC
    TBLPROPERTIES ("orc.compress"="SNAPPY");