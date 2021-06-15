CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_funnel_analysis_h
(
    dau                bigint        comment 'dau',
    pd_uv              bigint        comment '商品详情页UV',
    cart_success_uv    bigint        comment '加车成功UV',
    checkout_uv        bigint        comment 'checkout页UV',
    payment_uv         bigint        comment 'payment页UV',
    payment_confirm_uv bigint        comment '支付成功UV'
) COMMENT '各级页面漏斗转化uv' PARTITIONED BY (pt String, hour String) STORED AS PARQUETFILE;