#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "0 day" +%Y/%m/%d/%H`
echo "cur_date=${cur_date}"
fi
pt=`date -d "0 day" +%Y-%m-%d`
echo "pt=$pt"

spark-submit \
--master yarn \
--executor-memory 8G \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf spark.app.name=ads_vova_flash_sale_goods_d_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.FlashSaleGoods s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product  --pt $cur_date

if [ $? -ne 0 ];then
  echo "ads_flash_sale_goods_d job error"
  exit 1
fi