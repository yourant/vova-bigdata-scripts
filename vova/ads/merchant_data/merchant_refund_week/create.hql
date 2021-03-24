drop table ads.ads_vova_mct_refund_w;
create external table if not exists ads.ads_vova_mct_refund_w
(
    mct_id BIGINT COMMENT '商品所属商家',
    first_cat_id BIGINT COMMENT '商品一级类目ID',
    country     STRING COMMENT '国家',
    shipping_type INTEGER COMMENT '物流渠道',
    count_date TIMESTAMP ,
    refund_amount DECIMAL(15,4) COMMENT '退款金额',
    refund_number BIGINT COMMENT '退款子订单数',
    refund_rate DECIMAL(15,4) COMMENT '退款率',
    refund_rate_item_dont_fit DECIMAL(15,4) COMMENT '商品不合适退款率',
    refund_rate_poor_quality DECIMAL(15,4) COMMENT '质量差退款率',
    refund_rate_item_not_as_described DECIMAL(15,4) COMMENT '与描述不符退款率',
    refund_rate_defective_item DECIMAL(15,4) COMMENT '损坏退款率',
    refund_rate_shipment_late DECIMAL(15,4) COMMENT '晚发货退款率',
    refund_rate_wrong_product DECIMAL(15,4) COMMENT '错误商品退款率',
    refund_rate_wrong_quantity DECIMAL(15,4) COMMENT '错误数量退款率',
    refund_rate_not_receive DECIMAL(15,4) COMMENT '未收到退款率',
    refund_rate_others DECIMAL(15,4) COMMENT '其他退款率',
    refund_rate_empty_package DECIMAL(15,4) COMMENT '空包裹退款率'
) COMMENT '商家退款数据(周)' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;