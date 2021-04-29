#!/bin/bash
end_date=`date -d "-1 day" +%Y%m%d`
start_date=`date -d "-7 day" +%Y%m%d`
#start_date='20200525'
while [ ${start_date} -le ${end_date} ]; do
    cur_date=`date -d "${start_date}" +%Y-%m-%d`
    sh /mnt/vova-bigdata-scripts/vova/dwb/payment/update.sh ${cur_date}
    echo "${cur_date}"
    start_date=`date -d "${start_date} +1 day" +%Y%m%d`
done
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
cur_date=`date -d "-1 day" +%Y-%m-%d`
spark-submit  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.maxExecutors=60 \
--conf spark.app.name=vova_mlb_pay_send_knock \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.bigdata.sparkbatch.dataprocess.dwb.PaymentRate  s3://vomkt-emr-rec/jar/vova_mlb_pay_send_knock.jar \
${cur_date}


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi