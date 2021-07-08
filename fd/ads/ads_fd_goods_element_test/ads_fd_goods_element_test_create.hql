create external table if not exists ads.ads_fd_goods_element_test
(
    goods_id                   bigint          comment '商品ID',
    project                    string          comment '组织',
    platform                   string          comment '平台',
    country                    string          comment '国家',
    element_tag                string          comment '元素标签',
    element_batch              string          comment '元素批次',
    session_common_impression  bigint          comment '曝光UV',
    session_common_click       bigint          comment '点击UV',
    session_common_ctr         Decimal(38,2)   comment 'ctr',
    views                      bigint          comment '',
    cart                       bigint          comment '',
    video_impression           bigint          comment '',
    video_play                 bigint          comment ''
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
