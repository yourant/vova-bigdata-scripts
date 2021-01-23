-- [4823]【报表需求】自营店铺日报表
tmp_financial_self_final

DROP TABLE dwb.dwb_vova_finance_self_mct_summary;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_finance_self_mct_summary(
datasource                       string         COMMENT 'd_平台数据源',
mct_name                         string         COMMENT 'd_店铺名',
confirm_ord_gs_cnt               bigint         COMMENT 'i_已确认订单数量:当日已确认订单数量',
out_warehouse_ord_gs_cnt         bigint         COMMENT 'i_已出库订单数量:当日已出库订单数量',
shipping_rate                    decimal(10, 2) COMMENT 'i_发货完成率:出库订单量/已确认订单量',
mct_amount                       decimal(10, 2) COMMENT 'i_商品销售收入',
confirm_mct_amount               decimal(10, 2) COMMENT 'i_商品销售收入（发货）',
refund_mct_amount                decimal(10, 2) COMMENT 'i_商品销售收入（退货）',
shipping_fee                     decimal(10, 2) COMMENT 'i_代收快递费',
confirm_mct_shipping_fee         decimal(10, 2) COMMENT 'i_已发货，VOVA订单状态为【已确认】+【已取消】，商家结算价格中的运费价格，注意:b+e=商家结算价格',
refund_mct_shipping_fee          decimal(10, 2) COMMENT 'i_已发货，VOVA订单状态为【已取消】，商家结算价格中的运费价格',
mct_cost                         decimal(10, 2) COMMENT 'i_商品成本,已发货，VOVA订单状态为【已确认】+【已取消】，商品的采购价格/汇率 （每月汇率由财务进行设置）',
carrier_cost                     decimal(10, 2) COMMENT 'i_物流成本',
last_waybill_fee                 decimal(10, 2) COMMENT 'i_—尾程运费（集运仓-国外）,已发货，VOVA订单状态为【已确认】+【已取消】，对应订单的预估运费/汇率',
warehouse_operate_fee            decimal(10, 2) COMMENT 'i_—库内操作费,当日已发货订单笔数*2.3/汇率',
storage_fee                      decimal(10, 2) COMMENT 'i_—仓储费',
inventory_loss                   decimal(10, 2) COMMENT 'i_存货跌价损失,未发货，供应链支付状态【已支付】，VOVA订单状态为【已取消】，商品的采购价格/汇率（每月汇率由财务进行设置）',
platform_service_fee             decimal(10, 2) COMMENT 'i_交易平台手续费',
gross_margin                     decimal(10, 2) COMMENT 'i_毛利',
net_margin                       decimal(10, 2) COMMENT 'i_净利润',
refund_of_shipped_amount_rate    decimal(10, 2) COMMENT 'i_售后退款/发货收入',
shipping_of_shipped_amount_rate  decimal(10, 2) COMMENT 'i_代收快递费/发货收入',
mct_cost_of_shipped_amount_rate  decimal(10, 2) COMMENT 'i_商品成本/发货收入',
carrier_of_shipped_amount_rate   decimal(10, 2) COMMENT 'i_物流成本/发货收入',
service_of_shipped_amount_rate   decimal(10, 2) COMMENT 'i_交易平台手续费/发货收入',
inventory_loss_of_shipped_amount_rate  decimal(10, 2) COMMENT 'i_存货跌价损失/发货收入',
net_margin_of_shipped_amount_rate decimal(10, 2) COMMENT 'i_净利润/发货收入'

) COMMENT '自营店铺日汇总报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_mct_summary/"
;


自营店铺日报表
dwb.dwb_vova_finance_self_mct_summary

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_mct_summary/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_mct_summary/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_mct_summary/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_finance_self_mct_summary/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_finance_self_mct_summary/  s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_mct_summary

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_mct_summary/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_finance_self_mct_summary/

msck repair table dwb.dwb_vova_finance_self_mct_summary;
select * from dwb.dwb_vova_finance_self_mct_summary limit 20;

