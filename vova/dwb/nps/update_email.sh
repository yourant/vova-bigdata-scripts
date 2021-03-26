#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

query_sql="
select
nps_submit_time,
email,
order_sn,
rate,
reason,
order_type,
order_time,
order_goods_cnt,
cancel_order_goods_cnt,
ra_order_goods_cnt,
ro_order_goods_cnt,
fin_order_goods_cnt,
buyer_level,
min_pay_time,
max_pay_time,
his_gmv,
his_paid_order_cnt,
region_code
from
dwb.dwb_vova_nps_email
where pt = '${cur_date}'
"

head="
提交utc时间,
用户邮箱,
父订单号,
NPS评分,
理由内容,
提交NPS评价时的订单状态,
父订单下单时间,
子订单量,
子订单取消量,
子订单退款量,
子订单退货量,
子订单退货退款申请是否通过量,
用户阶段,
该用户第一次支付日期,
该用户最近一次支付日期,
该用户总gmv,
该用户总支付父订单数,
用户国家
;
"


spark-submit \
--deploy-mode client \
--name 'dwb_vova_nps_email' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "${query_sql}"  \
-head "${head}"  \
-receiver "suzi@vova.com.hk,sanlian@vova.com.hk,jianxiangyun@vova.com.hk,ethan.zheng@i9i8.com" \
-title "vova nps (${cur_date})" \
--type attachment \
--fileName "vova nps (${cur_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi

