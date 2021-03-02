#!/bin/bash
cur_date=$1
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
now_date=`date -d "-1 days ago ${cur_date}" +%Y-%m-%d`

spark-submit \
--deploy-mode client \
--master yarn  \
--driver-memory 8G \
--conf spark.dynamicAllocation.maxExecutors=120 \
--conf spark.default.parallelism=380 \
--conf spark.sql.shuffle.partitions=380 \
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.join.enabled=true \
--conf spark.shuffle.sort.bypassMergeThreshold=10000 \
--conf spark.sql.inMemoryColumnarStorage.compressed=true \
--conf spark.sql.inMemoryColumnarStorage.partitionPruning=true \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.network.timeout=300 \
--conf spark.app.name=rec_vova_user_clk_behave_link_d \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsClickCause s3://vomkt-emr-rec/jar/vova-goods-click-cause-v2.jar \
--pt ${cur_date} --now_date ${now_date}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
spark-sql --conf "spark.app.name=ads_vova_user_behave_link_d" --conf "spark.dynamicAllocation.maxExecutors=100"   -e "
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE ads.ads_vova_user_behave_link_d partition(pt='$cur_date',pagecode)
select
session_id,
buyer_id,
gender,
language_id,
country_id,
os_type,
device_model,
goods_id,
first_cat_id,
second_cat_id,
cat_id,
mct_id,
brand_id,
shop_price,
shipping_fee,
click_time,
page_code,
list_type,
if(page_code != 'product_detail',lower(trim(regexp_replace(regexp_replace(clk_from, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),clk_from) clk_from,
enter_ts,
leave_ts,
stay_time,
is_add_cart,
is_collect,
device_id,
lower(trim(regexp_replace(regexp_replace(goods_name, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) goods_name,
expre_time,
is_click,
is_order,page_code pagecode
from tmp.tmp_vova_user_clk_behave_link_d
where pt = '$cur_date'
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


