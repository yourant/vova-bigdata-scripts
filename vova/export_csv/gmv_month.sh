#gmv
sh /mnt/vova-bd-scripts/rpt/finance/gmv/update.sh;
;
spark-sql -e "
select
'时间范围','商家','渠道','支付平台','商品收入','运费收入','红包','总收款金额','扣佣金退款','不扣佣金退款','总退款金额（财务实际退款金额）','退款红包','应退款金额','收款中包含的品牌增值','退款中包含的品牌增值','收款中包含的集运增值','退款中包含的集运增值','收款中包含的拍卖增值','退款中包含的拍卖增值','夺宝订单收款金额','夺宝订单bonus','pt'
union all
select * from dwb.dwb_vova_finance_gmv where pt = '2021-03-01'
;
"  > vova_gmv_2103.csv


