#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
add jar hdfs:///tmp/jar/vova-db-dataprocess-1.0-SNAPSHOT.jar;
create temporary function same_price_rate as 'com.vova.udf.SamePriceRateUDF';
insert overwrite table ads.ads_vova_risk_merchant_reg_check partition(pt='${pre_date}')
select
dm.mct_id,
dm.mct_name,
concat_ws(',',collect_list(dg.shop_price)) as price_list,
count(distinct goods_id) as goods_cnt,
same_price_rate(concat_ws(',',collect_list(dg.shop_price))) same_price_rate
from
dim.dim_vova_goods dg
left join dim.dim_vova_merchant dm
on dg.mct_id = dm.mct_id
where dg.is_on_sale=1
group by
dm.mct_id,
dm.mct_name
having  same_price_rate>=0.95 and goods_cnt>=3

"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=ads_vova_risk_merchant_reg_check" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi


spark-submit \
--deploy-mode client \
--name 'ads_vova_risk_merchant_reg_check' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=20 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select mct_id,mct_name from ads.ads_vova_risk_merchant_reg_check where pt='${pre_date}'"  \
-head "店铺ID,店铺名称"  \
-receiver "qizi@vova.com.hk,loda.luo@vova.com.hk,yvon.dai@vova.com.hk,jonathan.li@vova.com.hk,lawrence.song@vova.com.hk,alex.chen@vova.com.hk,ted.wan@vova.com.hk" \
-title "问题商家列表(${pre_date})" \
--type attachment \
--fileName "问题商家列表(${pre_date})"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  echo "发送邮件失败"
  exit 1
fi