#!/bin/bash
#指定日期和引擎
pre_hour=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_hour=$(date -d "-1 hour" +%Y/%m/%d/%H)
fi
echo "$pre_hour "
#默认小时
cur_hour=$2
if [ ! -n "$2" ];then
cur_hour=`date -d "0 hour" +%Y/%m/%d/%H`
fi
echo "$cur_hour "

spark-submit --master yarn --deploy-mode client --queue important --conf spark.dynamicAllocation.maxExecutors=100  --name "test_goods" --packages com.snowplowanalytics:snowplow-scala-analytics-sdk_2.11:0.4.1  --class com.vova.data.tables.test.Main s3://vomkt-emr-rec/jar/test-goods-1.0.1.jar  ${pre_hour} ${cur_hour}
if [ $? -ne 0 ];then
  echo "test_goods job error"
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
-Dsqoop.export.records.per.statement=100 \
-Dsqoop.export.statements.per.transaction=1 \
--connect jdbc:mysql://rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com/backend?characterEncoding=utf-8 \
--username bimaster --password kkooxGjFy7Vgu21x \
-m 1 \
--table test_goods_behave \
--update-key "datasource,goods_id,platform,region_codes" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_test_goods_h \
--fields-terminated-by '\t' \
--columns "id,datasource,goods_id,platform,region_codes,region_ids,users,clicks,impressions,sales_order,gmv,ctr,gcr,test_status,test_result,create_time,last_update_time"

if [ $? -ne 0 ];then
   exit 1
fi