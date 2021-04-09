#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
   hive -e "msck repair table ads.ads_image_vector_target_d;"
if [ $? -ne 0 ];then
  exit 1
fi
max_pt=$(hive -e "show partitions ads.ads_image_vector_target_d" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
pt=${max_pt:3}
fi
echo "pt=$pt"

hive -e "ALTER TABLE ads.ads_image_vector_target_his DROP if exists partition(pt = '$pt');"
if [ $? -ne 0 ];then
  exit 1
fi
max_pt=$(hive -e "show partitions ads.ads_image_vector_target_his" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
last_pt=${max_pt:3}
echo "last_pt=$last_pt"

m_pt=$(hive -e "show partitions ads.ads_min_price_goods_h" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
m_price_pt=${m_pt:3}
echo "m_price_pt=$m_price_pt"

spark-submit --master yarn \
--deploy-mode client  \
--name  GoodsImgVectorV2 \
--class com.vova.data.GoodsImgVectorV2 \
s3://vomkt-emr-rec/jar/vova-bd-hudi-assembly-2.6.jar \
--env product --pt $pt --last_pt $last_pt --m_price_pt $m_price_pt

if [ $? -ne 0 ];then
   exit 1
fi