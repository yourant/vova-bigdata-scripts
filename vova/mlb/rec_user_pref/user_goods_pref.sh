#!/bin/bash

pt=$1
bayes_a=$2
bayes_b=$3
clk_weight=$4
collect_weight=$5
add_cart_weight=$6
ord_weight=$7
decay=$8

if [ ! -n "$1" ];then
   pt=`date -d "-1 day" +%Y-%m-%d`
fi

if [ ! -n "$2" ];then
   bayes_a=1
   bayes_b=100
   clk_weight=1
   collect_weight=7
   add_cart_weight=9
   ord_weight=9
   decay=0.93
fi

pt_before1=`date -d "${pt} -1 day" +%Y-%m-%d`

script_dir=$(cd "$(dirname "$0")";pwd)

sed "s/{pt}/${pt}/g; \
      s/{pt_before1}/${pt_before1}/g; \
      s/{bayes_a}/${bayes_a}/g; \
      s/{bayes_b}/${bayes_b}/g; \
      s/{clk_weight}/${clk_weight}/g; \
      s/{collect_weight}/${collect_weight}/g; \
      s/{add_cart_weight}/${add_cart_weight}/g; \
      s/{ord_weight}/${ord_weight}/g; \
      s/{decay}/${decay}/g;" ${script_dir}/user_goods_pref.sql > ${script_dir}/tmp_user_goods_pref.sql
sql=$(cat ${script_dir}/tmp_user_goods_pref.sql)
cat ${script_dir}/tmp_user_goods_pref.sql

echo "===========run scrpit...... ==============="
spark-sql --conf "spark.app.name=dyshu_user_goods_pref_${pt}" \
          --conf "spark.dynamicAllocation.maxExecutors=100" \
          --driver-memory 6g \
          -e "$sql"

# if error
if [ $? -ne 0 ];then
   exit 1
fi