#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 hour" +%Y/%m/%d/%H`
echo "pt=${cur_date}"
fi
pt=`date -d "-1 hour" +%Y-%m-%d`
echo "$pt"
spark-submit \
--master yarn --deploy-mode cluster \
--num-executors 10 \
--queue important \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.app.name=min_price_goods \
--class com.vova.data.GoodsPicSimilar s3://vomkt-emr-rec/jar/vova-bd-min-price-goods-1.0.0.jar \
--env product --pt $cur_date

if [ $? -ne 0 ];then
  echo "goods_pic_similar job error"
  exit 1
fi


sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
-Dsqoop.export.records.per.statement=10000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bimaster --password sYG2Ri3yIDu2NPki \
--table ads_min_price_goods_h \
--update-key "goods_id,strategy" \
--m 1 \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_min_price_goods_h \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,min_price_goods_id,strategy,group_number,min_show_price,avg_sku_price"

if [ $? -ne 0 ];then
  echo "goods_pic_similar sqoop error"
  exit 1
fi