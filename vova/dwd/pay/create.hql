ALTER TABLE dwd.dwd_vova_fact_pay SET TBLPROPERTIES('EXTERNAL'='False')
drop table dwd.dwd_vova_fact_pay;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_pay
(
    datasource       string comment '数据平台',
    order_goods_id   bigint comment '子订单ID',
    order_goods_sn   string comment '子订单SN',
    order_id         bigint comment '父订单ID',
    buyer_id         bigint comment '买家ID',
    gender           string comment '买家性别',
    buyer_email      string comment '卖家邮箱',
    from_domain      string comment '订单来源',
    payment_id       bigint comment '支付ID',
    payment_name     string comment '支付方式名称',
    mct_id           bigint comment '店铺ID',
    sku_id           bigint comment '商品sku级别ID',
    goods_id         bigint comment '商品ID',
    goods_sn         string comment '商品SN',
    goods_name       string comment '商品名称',
    cat_id           BIGINT COMMENT '商品类目ID',
    cat_name         string COMMENT '商品类目name',
    first_cat_id     BIGINT COMMENT '商品一级类目ID',
    first_cat_name   string COMMENT '商品一级类目name',
    second_cat_id    BIGINT COMMENT '商品二级类目ID',
    second_cat_name  string COMMENT '商品二级类目name',
    order_time       timestamp comment '下单时间',
    confirm_time     timestamp comment '子订单确认时间',
    pay_time         timestamp comment '支付时间',
    device_id        string comment '设备id',
    platform         string comment '设备平台(android, ios, pc, mob, unknown)',
    market_priclikee decimal(10, 4) comment '展示价格',
    goods_number     bigint comment '销售数量',
    shop_price       decimal(10, 4) comment '销售价格',
    shipping_fee     decimal(10, 4) comment '运费',
    goods_weight     decimal(10, 4) comment '商品重量',
    bonus            decimal(10, 4) comment '商品使用的优惠券',
    mct_shop_price   decimal(10, 4) comment '给商家展示的价格',
    mct_shipping_fee decimal(10, 4) comment '给商家展示的运费',
    region_id        bigint comment '交易的国家ID',
    region_code      string comment '交易的国家CODE'
) COMMENT '订单商品事实表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_pay_h
(
    datasource       string comment '数据平台',
    order_goods_id   bigint comment '子订单ID',
    order_goods_sn   string comment '子订单SN',
    order_id         bigint comment '父订单ID',
    buyer_id         bigint comment '买家ID',
    gender           string comment '买家性别',
    buyer_email      string comment '卖家邮箱',
    from_domain      string comment '订单来源',
    payment_id       bigint comment '支付ID',
    payment_name     string comment '支付方式名称',
    mct_id           bigint comment '店铺ID',
    sku_id           bigint comment '商品sku级别ID',
    goods_id         bigint comment '商品ID',
    goods_sn         string comment '商品SN',
    goods_name       string comment '商品名称',
    cat_id           BIGINT COMMENT '商品类目ID',
    cat_name         string COMMENT '商品类目name',
    first_cat_id     BIGINT COMMENT '商品一级类目ID',
    first_cat_name   string COMMENT '商品一级类目name',
    second_cat_id    BIGINT COMMENT '商品二级类目ID',
    second_cat_name  string COMMENT '商品二级类目name',
    order_time       timestamp comment '下单时间',
    confirm_time     timestamp comment '子订单确认时间',
    pay_time         timestamp comment '支付时间',
    device_id        string comment '设备id',
    platform         string comment '设备平台(android, ios, pc, mob, unknown)',
    market_priclikee decimal(10, 4) comment '展示价格',
    goods_number     bigint comment '销售数量',
    shop_price       decimal(10, 4) comment '销售价格',
    shipping_fee     decimal(10, 4) comment '运费',
    goods_weight     decimal(10, 4) comment '商品重量',
    bonus            decimal(10, 4) comment '商品使用的优惠券',
    mct_shop_price   decimal(10, 4) comment '给商家展示的价格',
    mct_shipping_fee decimal(10, 4) comment '给商家展示的运费',
    region_id        bigint comment '交易的国家ID',
    region_code      string comment '交易的国家CODE'
) COMMENT '订单商品事实表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



