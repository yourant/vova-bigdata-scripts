#!/bin/bash
#指定日期和引擎
stime=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  stime=`date -d "-1hour" "+%Y-%m-%d %H:00:00"`
fi
echo "$stime"
hour=`date -d "$stime" +%H`
echo "hour=$hour"
#默认小时
etime=$2
if [ ! -n "$1" ]; then
  etime=`date -d "0 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "etime=$etime"
pt=`date -d "$etime" +%Y-%m-%d`
echo "pt=$pt"


spark-submit --master yarn \
--deploy-mode client \
--driver-memory 8G \
--executor-memory 8G \
--conf spark.dynamicAllocation.maxExecutors=100 \
--name "test_goods" \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsTest s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--stime "${stime}" --hour ${hour} --etime "${etime}" --pt ${pt} --env product
if [ $? -ne 0 ];then
  echo "test_goods job error"
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=100 \
-Dsqoop.export.statements.per.transaction=1 \
--connect jdbc:mysql://rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com/backend?characterEncoding=utf-8 \
--username bimaster --password kkooxGjFy7Vgu21x \
-m 1 \
--table test_goods_behave \
--update-key "datasource,goods_id,platform,region_codes" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_test_goods_h \
--fields-terminated-by '\001' \
--columns "id,datasource,goods_id,platform,region_codes,region_ids,users,clicks,impressions,sales_order,gmv,ctr,gcr,test_status,test_result,create_time,last_update_time"

if [ $? -ne 0 ];then
   exit 1
fi