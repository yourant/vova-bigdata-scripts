DROP TABLE dwb.dwb_vova_tw_order;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_tw_order
(
    event_date                                               date COMMENT 'd_日期',
    platform                                                 string COMMENT 'd_平台',
    mct_type                                                 string COMMENT 'd_商家类型,是否自营店铺',
    channel                                                  string COMMENT 'd_渠道',
    order_cnt                                                bigint COMMENT 'i_下单父订单',
    paid_order_cnt                                           bigint COMMENT 'i_支付成功父订单',
    paid_order_goods_cnt                                     bigint COMMENT 'i_支付成功子订单',
    paid_buyer_cnt                                           bigint COMMENT 'i_支付成功用户数',
    gmv                                                      decimal(15, 2) COMMENT 'i_gmv',
    receive_order_goods_cnt                                  bigint COMMENT 'i_妥投子订单',
    total_delivered_days                                     decimal(15, 2) COMMENT 'i_妥投总天数',
    paid_cod_order_goods_cnt                                 bigint COMMENT 'i_cod支付子订单',
    receive_order_goods_cod_cnt                              bigint COMMENT 'i_cod妥投子订单',
    discount_order_goods_cnt                                 bigint COMMENT 'i_集运优惠子订单',
    container_transportation_shipping_fee_discount           decimal(15, 2) COMMENT 'i_促销优惠总额',
    total_shipping_fee                                       decimal(15, 2) COMMENT 'i_总运费（含集运）',
    paid_order_cnt_div_order_cnt                             decimal(15, 4) COMMENT 'i_支付成功率',
    paid_order_goods_cnt_div_paid_buyer_cnt                  decimal(15, 2) COMMENT 'i_人均子订单数',
    gmv_div_paid_buyer_cnt                                   decimal(15, 2) COMMENT 'i_客单价',
    gmv_div_paid_order_goods_cnt                             decimal(15, 2) COMMENT 'i_笔单价(父订单)',
    receive_order_goods_cnt_div_paid_order_goods_cnt         decimal(15, 4) COMMENT 'i_妥投率',
    avg_receive_days                                         decimal(15, 2) COMMENT 'i_平均妥投天数',
    paid_cod_order_goods_cnt_div_paid_order_goods_cnt        decimal(15, 4) COMMENT 'i_COD订单占比',
    receive_order_goods_cod_cnt_div_paid_cod_order_goods_cnt decimal(15, 4) COMMENT 'i_COD订单妥投率',
    discount_order_goods_cnt_div_paid_order_goods_cnt    decimal(15, 4) COMMENT 'i_促销参与率',
    total_delivered_cnt    bigint COMMENT 'i_有妥投天数的子订单'
) COMMENT 'tw集运订单报表'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_tw_order_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_tw_order_detail
(
    event_date                                     date COMMENT 'd_日期',
    order_sn                                       string COMMENT 'i_订单号',
    order_time                                     TIMESTAMP COMMENT 'i_下单时间',
    shipping_method_name                           string COMMENT 'i_物流方式',
    shipping_status                                string COMMENT 'i_发货状态',
    shipping_type                                  string COMMENT 'i_运送方式',
    payment_name                                   string COMMENT 'i_支付类型',
    pay_time                                       TIMESTAMP COMMENT 'i_支付时间',
    order_goods_cnt                                bigint COMMENT 'i_包含子订单数',
    order_goods_sku_cnt                            bigint COMMENT 'i_SKU件数',
    gmv                                            decimal(15, 2) COMMENT 'i_GMV贡献',
    goods_amount                                   decimal(15, 2) COMMENT 'i_商品总金额',
    bonus                                          decimal(15, 2) COMMENT 'i_优惠券优惠金额',
    shipping_fee                                   decimal(15, 2) COMMENT '',
    container_transportation_shipping_fee          decimal(15, 2) COMMENT '',
    tot_shipping_fee                               decimal(15, 2) COMMENT 'i_运费总额',
    container_transportation_shipping_fee_discount decimal(15, 2) COMMENT 'i_集运促销优惠总额'
) COMMENT 'tw集运订单父订单明细'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=vts --table_name=shipping_method --mapers=1 --etl_type=ALL  --period_type=day --partition_num=3
