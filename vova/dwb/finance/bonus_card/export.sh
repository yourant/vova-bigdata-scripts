
#sh /mnt/vova-bigdata-scripts/vova/dwb/finance/bonus_card/read_es.sh

spark-sql -e "
select
'渠道','时间范围','现处周期','月卡订单ID','用户ID','开卡费用','生效日期','失效日期','发放金额币种','本月发放金额','本月抵扣金额(USD)','本月未使用金额','本月转化的订单量','本月转化的订单金额(USD)','累计发放金额','累计抵扣金额(USD)','累计未使用金额','累计转化的订单量','累计转化的订单金额(USD)'
union all
select
datasource,
interval_date,
life_cycle,
bonus_card_id,
user_id,
price,
bonus_start,
bonus_end,
currency,
issue_amount,
bonus,
valid_amount,
order_cnt,
order_amount,
issue_amount_interval,
bonus_interval,
valid_amount_interval,
order_cnt_interval,
order_amount_interval
from
dwb.dwb_vova_finance_bonus_card
where pt = '2021-04-01'
;
"  > vova_bonus_card_2104.csv

