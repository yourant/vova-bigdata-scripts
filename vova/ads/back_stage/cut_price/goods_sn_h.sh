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
--master yarn \
 --conf "spark.dynamicAllocation.maxExecutors=100" \
--conf spark.app.name=ads_vova_back_stage_goods_sn_avg_price_h_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.BackStageGoodsCutPrice s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $cur_date

if [ $? -ne 0 ];then
  echo "goods_pic_similar job error"
  exit 1
fi


#sqoop export \
#-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
#-Dmapreduce.job.queuename=important \
#--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
#--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
#--table ads_gsn_avg_price \
#--update-key "goods_sn" \
#--update-mode allowinsert \
#--hcatalog-database ads \
#--hcatalog-table ads_gsn_avg_price_h \
#--hcatalog-partition-keys pt \
#--hcatalog-partition-values ${pt} \
#--fields-terminated-by '\001' \
#--columns "goods_sn,gsn_avg_price"
#
#if [ $? -ne 0 ];then
#  echo "goods_pic_similar sqoop error"
#  exit 1
#fi