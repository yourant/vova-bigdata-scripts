#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y/%m/%d/%H`
echo "pt=${cur_date}"
fi
max_pt=$(hive -e "show partitions ads.ads_vova_goods_id_behave_m" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
last_pt=${max_pt:3}
echo "last_pt=$last_pt"
pt=`date -d "-1 day" +%Y-%m-%d`
echo "$pt"

spark-submit \
--master yarn \
--executor-memory 6G \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf spark.app.name=ads_vova_min_price_goods_d_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.MinPrice s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $cur_date --last_pt $last_pt --is_hour false

if [ $? -ne 0 ];then
  echo "goods_pic_similar job error"
  exit 1
fi