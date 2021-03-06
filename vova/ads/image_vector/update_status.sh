#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
   hive -e "msck repair table ads.ads_vova_image_vector_target_d;"
if [ $? -ne 0 ];then
  exit 1
fi
max_pt=$(hive -e "show partitions ads.ads_vova_image_vector_target_d" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
pt=${max_pt:3}
fi
echo "pt=$pt"

hive -e "ALTER TABLE ads.ads_vova_image_vector_target_his DROP if exists partition(pt = '$pt');"
if [ $? -ne 0 ];then
  exit 1
fi
max_pt=$(hive -e "show partitions ads.ads_vova_image_vector_target_his" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
last_pt=${max_pt:3}
echo "last_pt=$last_pt"

m_pt=$(hive -e "show partitions ads.ads_vova_min_price_goods_d" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
m_price_pt=${m_pt:3}
echo "m_price_pt=$m_price_pt"

spark-submit --master yarn \
--deploy-mode client  \
--executor-memory 10G \
--name GoodsImgVector \
--conf spark.dynamicAllocation.maxExecutors=100 \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.GoodsImgVector \
s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $pt --last_pt $last_pt --m_price_pt $m_price_pt --pt_num 5

if [ $? -ne 0 ];then
   exit 1
fi

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=vova_image_vector_extract_data_dst --from=data --to=service --jtype=2D --retry=0
if [ $? -ne 0 ];then
  exit 1
fi