#!/bin/bash
# 由 job messager 启动的任务, 会有 freedoms
freedoms=$1
echo "freedoms: ${freedoms}"

if [ ! -n "$1" ]; then
  echo "Error: freedoms 为必传参数！！！"
  exit 1
fi

# 从 freedoms 拿到 table_name 和 dt
pt=`echo $freedoms | jq '.pt' | sed $'s/\"//g'`
if [ ! -n "${pt}" ]; then
  echo "Error: freedoms 为必传参数！！！"
  exit 1
fi

# 判断对应表、对应分区 是否有数据
hive -e "msck repair table ads.ads_vova_img_enhance_result_d;"
if [ $? -ne 0 ];then
  echo "Failed: msck repair table ads.ads_vova_img_enhance_result_d;"
  exit 1
fi

cnt=$(spark-sql -e "select count(*) from ads.ads_vova_img_enhance_result_d where pt ='${pt}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: ads.ads_vova_img_enhance_result_d, pt=${pt}, 数据条数异常 count(*)=${cnt} -le 0"
  exit 1
fi
echo ${cnt}

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_images?tinyInt1isBit=false \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_vova_goods_img_enhance \
--update-key "img_id" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_img_enhance_result_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values $pt \
--fields-terminated-by ',' \
--columns "goods_id,img_id,img_url_aws,img_url_gcs,is_default"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi