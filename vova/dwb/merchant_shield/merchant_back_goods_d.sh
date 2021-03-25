#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
new_sql="
insert overwrite table dwb.dwb_vova_goods_id_1d partition(pt='$pre_date')
select
/*+ REPARTITION(1) */
t1.goods_id,
(sum(t1.nlrf_order_cnt_5_8w)+0.1*5)/(count(t1.order_goods_id)+5) as refund_rate_nonlogistics_8w
from
(
select
og.goods_id,
og.order_goods_id,
case when fr.refund_reason_type_id != 8 and fr.refund_type_id=2 then 1 else 0 end nlrf_order_cnt_5_8w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
where datediff('${pre_date}', date(og.confirm_time)) between 35 and 56
and og.sku_pay_status>1
and og.sku_shipping_status > 0
) t1
group by t1.goods_id
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_goods_id_d" \
-e "$new_sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
  -Dorg.apache.sqoop.export.text.dump_data_on_error=true \
  -Dmapreduce.job.queuename=default \
  --connect jdbc:mysql://vovadb.cei8p8whxxwd.us-east-1.rds.amazonaws.com:3306/themis?rewriteBatchedStatements=true \
  --username dbg20191029 --password lz5KtWHH8tIgGEYU5hYUbPGpkufmsfup \
  --m 1 \
  --table rpt_goods_id_1d \
  --update-key goods_id \
  --update-mode allowinsert \
  --hcatalog-database dwb \
  --hcatalog-table dwb_vova_goods_id_1d \
  --hcatalog-partition-keys pt \
  --hcatalog-partition-values ${pre_date} \
  --columns goods_id,refund_rate_nonlogistics_8w \
  --fields-terminated-by '\001' \
  --batch

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "export  tale failed"
  exit 1
fi