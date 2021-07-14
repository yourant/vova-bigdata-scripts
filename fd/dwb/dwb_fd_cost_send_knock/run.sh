#bin/sh
table="dwb_fd_cost_send_knock"
user="zhubao"

#订单维度退款比率
fd_refund_order_cnt_rate=0.1950
ad_refund_order_cnt_rate=0.1327
#红包比率
fd_bouns_rate=0.0691
ad_bouns_rate=0.0598

base_path="/mnt/vova-bigdata-scripts/fd/dwb"

if [ ! -n "$1" ]; then
  pt=$(date -d "- 1 days" +"%Y-%m-%d")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
  pt=$1
fi
echo "pt: ${pt}"

spark-submit \
  --deploy-mode client \
  --conf spark.dynamicAllocation.maxExecutors=100 \
  --conf spark.app.name=fd_cost_send_knock_zhubao \
  --driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
  --class com.fd.bigdata.sparkbatch.dwb.FdCostSendKnock s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
  --pt ${pt} \
  --fd_refund_order_cnt_rate ${fd_refund_order_cnt_rate} \
  --ad_refund_order_cnt_rate ${ad_refund_order_cnt_rate} \
  --fd_bouns_rate ${fd_bouns_rate} \
  --ad_bouns_rate ${ad_bouns_rate} \
  --knock_array bob.zhu

if [ $? -ne 0 ]; then
  exit 1
fi
echo "${dwb_fd_cost_send_knock} is finished!"
