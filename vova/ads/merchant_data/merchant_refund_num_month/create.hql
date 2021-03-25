drop table ads.ads_vova_mct_refund_cnt_m;
create external table if not exists ads.ads_vova_mct_refund_cnt_m
(
    mct_id BIGINT COMMENT '商品所属商家',
    country STRING COMMENT '国家',
    shipping_type INTEGER COMMENT '物流渠道',
    count_date TIMESTAMP ,
    refund_num BIGINT COMMENT '退款子订单数',
    item_dont_fit BIGINT COMMENT '商品不合适退款数',
    poor_quality BIGINT COMMENT '质量差退款数',
    item_not_as_described BIGINT COMMENT '与描述不符退款数',
    defective_item BIGINT COMMENT '损坏退款数',
    shipment_late BIGINT COMMENT '晚发货退款数',
    wrong_product BIGINT COMMENT '错误商品退款数',
    wrong_quantity BIGINT COMMENT '错误数量退款数',
    not_receive BIGINT COMMENT '未收到退款数',
    others BIGINT COMMENT '其他退款数',
    empty_package BIGINT COMMENT '空包裹退款数'
) COMMENT '商家退款订单数(月)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;