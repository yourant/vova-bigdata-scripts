5027 自营店铺商品退款率数据报表

历史数据不用迁

drop table if exists dwb.dwb_vova_sel_mct_goods_refund;
create external table if not exists dwb.dwb_vova_sel_mct_goods_refund(
datasource                              STRING         COMMENT '平台数据源',
virtual_goods_id                        STRING         COMMENT '商品虚拟ID',
goods_sn                                STRING         COMMENT '显示商品单号',
goods_number                            bigint         COMMENT '商品2020.4.1至今历史销量',
goods_number_last7                      bigint         COMMENT '商品的近7天销量',
gmv                                     decimal(10, 2) COMMENT '2020.4.1至今历时销量对应gmv',
refund_order_goods_cnt                  bigint         COMMENT '45天前确认订单数',
confirm_order_goods_cnt_before45        bigint         COMMENT '45天内退款订单数',
refund_order_goods_cnt_within45         bigint         COMMENT '90天前确认订单数',
confirm_order_goods_cnt_before90        bigint         COMMENT '90天内退款订单数',
refund_order_goods_cnt_within90         bigint         COMMENT '历史确认订单数',
confirm_order_goods_cnt                 bigint         COMMENT '历史退款订单数',
defective_refund_order_goods_cnt        bigint         COMMENT '退款原因为Defective item的订单数量',
doesnt_fit_refund_order_goods_cnt       bigint         COMMENT '退款原因为Item doesn’t fit的订单数量',
not_as_described_refund_order_goods_cnt bigint         COMMENT '退款原因为Item not as described的订单数量',
not_receive_yet_refund_order_goods_cnt  bigint         COMMENT '退款原因为Not receive the item yet的订单数量',
others_refund_order_goods_cnt           bigint         COMMENT '退款原因为Others的订单数量',
poor_quality_refund_order_goods_cnt     bigint         COMMENT '退款原因为Poor quality的订单数量',
wrong_product_refund_order_goods_cnt    bigint         COMMENT '退款原因为Wrong product的订单数量',
wrong_quantity_refund_order_goods_cnt   bigint         COMMENT '退款原因为Wrong quantity的订单数量',
pt                                      STRING         COMMENT '日期'
) COMMENT '自营店铺商品退款率数据报表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_sel_mct_goods_refund/"
;