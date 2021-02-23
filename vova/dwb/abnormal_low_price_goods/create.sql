drop table if exists dwb.dwb_vova_abnormal_low_price_goods;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_abnormal_low_price_goods
(
    virtual_goods_id       bigint COMMENT '虚拟id',
    goods_name             string COMMENT '商品名称',
    avg_price              decimal(20, 2) COMMENT '商品均价',
    group_avg_price        decimal(20, 2) COMMENT '商品同组均价',
    gmv                    decimal(20, 2) COMMENT '30天gmv',
    mct_id               bigint COMMENT '店铺id',
    mct_name               string COMMENT '店铺名称',
    diff_price             decimal(20, 2) COMMENT '商品便宜金额',
    diff_price_rate        string COMMENT '商品便宜百分比',
    performance_order_num  bigint        COMMENT '履约订单数',
    confirm_order_num      bigint         COMMENT '已确认订单数',
    vote_order_num         bigint         COMMENT '已妥投订单数',
    vote_refund_order_rate string COMMENT '妥投订单退款率'
) COMMENT '异常低价取数'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;