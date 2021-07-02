#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 hour" +%Y/%m/%d/%H`
echo "pt=${cur_date}"
fi
max_pt=$(hive -e "show partitions ads.ads_vova_goods_id_behave_m" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
last_pt=${max_pt:3}
echo "last_pt=$last_pt"
pt=`date -d "-1 hour" +%Y-%m-%d`
echo "$pt"
spark-submit \
--master yarn \
--executor-memory 6G \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf spark.app.name=ads_vova_min_price_goods_test_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsPicSimilar s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $cur_date --last_pt $last_pt

if [ $? -ne 0 ];then
  echo "goods_pic_similar job error"
  exit 1
fi


sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_min_price_goods_test \
--update-key "goods_id,strategy" \
--m 1 \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_min_price_goods \
--hcatalog-partition-keys pt,strategy \
--hcatalog-partition-values 2021-03-11,b \
--fields-terminated-by '\001' \
--columns "goods_id,min_price_goods_id,strategy,group_number,min_show_price,avg_sku_price"