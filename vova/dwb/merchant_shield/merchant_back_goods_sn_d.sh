#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
new_sql="
insert overwrite table dwb.dwb_vova_goods_sn_1d partition(pt='$pre_date')
select
/*+ REPARTITION(4) */
t1.goods_sn,
nvl(sum(t2.impressions),0) pv_goods_impression_1w,
nvl(sum(t2.clicks),0)  pv_goods_click_1w,
nvl(sum(t2.users),0) uv_1w,
nvl(sum(t2.gmv),0) gmv_1w,
nvl(nvl(sum(t2.clicks),0)*nvl(sum(t2.gmv),0)/(nvl(sum(t2.impressions),0)*nvl(sum(t2.users),0)),0) gcr_1w,
nvl(nvl(sum(t2.clicks),0)/nvl(sum(t2.impressions),0),0) ctr_1w,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP()
from  dim.dim_vova_goods t1
left outer join
ods_vova_vts.ods_vova_goods_display_sort t2
on t1.goods_id=t2.goods_id
where t1.goods_sn is not null and t1.goods_sn <> ''
group by t1.goods_sn;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_goods_sn_d" \
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
  --table rpt_goods_sn_1d \
  --m 1 \
  --update-key goods_sn \
  --update-mode allowinsert \
  --hcatalog-database dwb \
  --hcatalog-table dwb_vova_goods_sn_1d \
  --hcatalog-partition-keys pt \
  --hcatalog-partition-values ${pre_date} \
  --fields-terminated-by '\001' \
  --batch

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "export  tale failed"
  exit 1
fi