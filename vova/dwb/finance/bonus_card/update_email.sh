#!/bin/bash
#指定日期和引擎
last_month=$1
#默认日期为昨天
if [ ! -n "$1" ];then
last_month=`date -d "-1 month" +%Y-%m-01`
fi
###逻辑sql

query_sql="
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
order_amount_interval,
income
from
dwb.dwb_vova_finance_bonus_card
where pt = '${last_month}'
"

head="
渠道,
时间范围,
现处周期,
月卡订单ID,
用户ID,
开卡费用,
生效日期,
失效日期,
发放金额币种,
本月发放金额,
本月抵扣金额(USD),
本月未使用金额,
本月转化的订单量,
本月转化的订单金额(USD),
累计发放金额,
累计抵扣金额(USD),
累计未使用金额,
累计转化的订单量,
累计转化的订单金额(USD),
收入
;
"


spark-submit \
--deploy-mode client \
--name 'dwb_vova_finance_bonus_card' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "${query_sql}"  \
-head "${head}"  \
-receiver "ethan.zheng@i9i8.com, yuange@i9i8.com" \
-title "vova月卡报表(${last_month})" \
--type attachment \
--fileName "vova月卡报表(${last_month})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi
#-receiver "ethan.zheng@i9i8.com, yuange@i9i8.com" \
