#!/bin/bash
#指定日期和引擎
stime=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  stime=`date -d "-168 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "$stime"
#默认小时
pre_pt=`date -d "${stime}" +%Y-%m-%d`
echo "$pre_pt"
etime=$2
if [ ! -n "$1" ]; then
  etime=`date -d "0 hour" "+%Y-%m-%d %H:00:00"`
fi
echo "etime=$etime"
pt=`date -d "$etime" +%Y-%m-%d`
echo "pt=$pt"
pre_month=`date -d "30 day ago ${pt}" +%Y-%m-%d`
echo "pre_month =$pre_month"
pre_last_year=`date -d "180 day ago ${pt}" +%Y-%m-%d`
echo "pre_last_year=$pre_last_year"

echo "
spark-submit --master yarn --deploy-mode cluster \
--class com.vova.data.GoodsIdBehave s3://vomkt-emr-rec/jar/goods_id_behave.jar \
--env product --pt $pt --pre_pt $pre_pt --stime $stime --etime $etime --pre_month $pre_month --pre_last_year $pre_last_year
"
spark-submit --master yarn  \
--conf "spark.app.name=ads_vova_goods_id_behave_zhangyin" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf spark.executor.memory=6G \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsIdBehave \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $pt --pre_pt $pre_pt --stime "$stime" --etime "$etime" --pre_month $pre_month --pre_last_year $pre_last_year

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
