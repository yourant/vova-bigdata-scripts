#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=`date +%Y-%m-%d`
fi

src_weekday=`date -d $cur_date +%w`
if [ $src_weekday == 0 ]
then
src_weekday=7
fi
src_day=`date -d "$cur_date - $((src_weekday )) days" +%F`
end_day=`date -d $src_day"+6 day" +%Y-%m-%d`
echo "start time:$src_day,end time:$end_day"
sql="
with tmp_invalid_buyer as
(select
tmp1.email,
sum(is_refund_not_pass) as refund_not_pass_cnt,
sum(if(is_refund=1,total_amount,0))/sum(total_amount)*100 as redund_monery_rate
from
(select
db.email,
fp.goods_number*fp.shop_price+fp.shipping_fee as total_amount,
if(fr.refund_type_id=2 and ogs.sku_pay_status not in (3,4),1,0) as is_refund_not_pass,
if(fr.refund_type_id=2 and ogs.sku_pay_status in (3,4),1,0) as is_refund
from
dwd.dwd_vova_fact_pay fp
left join dwd.dwd_vova_fact_refund fr on fp.order_goods_id = fr.order_goods_id
left join ods_vova_vts.ods_vova_order_goods_status ogs on fp.order_goods_id = ogs.order_goods_id
inner join dim.dim_vova_buyers db on fp.buyer_id = db.buyer_id
where date(fp.pay_time)>date_sub('${cur_date}',365))tmp1
group by
tmp1.email
having refund_not_pass_cnt>0 or redund_monery_rate>=5)

insert overwrite table ads.ads_vova_email_good_comment partition(pt='${cur_date}')
select
db.email,
db.language_code
from
dwd.dwd_vova_fact_pay fp
left join dwd.dwd_vova_fact_logistics fl  on fl.order_goods_id = fp.order_goods_id
inner join dim.dim_vova_buyers db on fp.buyer_id = db.buyer_id
left join dwd.dwd_vova_fact_comment fc on fc.order_goods_id = fp.order_goods_id
left anti join tmp_invalid_buyer on db.email = tmp_invalid_buyer.email
where
date(pay_time)>date_sub('${cur_date}',365)
and db.email NOT REGEXP '@vovaopen.com'
and date(fl.delivered_time)>='${src_day}' and date(fl.delivered_time)<='${end_day}'
and db.language_code in ('da','nl','en','fi','it','no','pl','pt','ru','es','se','fr','de')
and rating =5
group by
db.email,
db.language_code
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=vova_email_good_comment" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
select
email,
language_code
from
ads.ads_vova_email_good_comment
where pt='${cur_date}'
"

head="
用户邮箱,
语言
"

spark-submit \
--deploy-mode client \
--name 'vova_email_good_comment' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "${sql}"  \
-head "${head}"  \
-receiver "cecilia.qi@vova.com.hk,ted.wan@vova.com.hk" \
-title "VOVA平台监管组TP邀好评数据需求(${src_day}-${end_day})" \
--type attachment \
--fileName "VOVA平台监管组TP邀好评数据需求(${src_day}-${end_day})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi