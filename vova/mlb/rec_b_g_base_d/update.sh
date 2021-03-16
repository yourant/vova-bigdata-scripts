#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date +%Y-%m-%d`
fi

echo "cur_date: ${cur_date}"

job_name="mlb_vova_rec_b_g_base_d_req7591_gongrui_chenkai"

###逻辑sql
sql="
insert overwrite table mlb.mlb_vova_rec_b_g_base_d PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(100) */
  datasource      ,
  goods_id        ,
  virtual_goods_id,
  cp_goods_id     ,
  brand_id        ,
  goods_sn        ,
  goods_name      ,
  goods_desc      ,
  sale_status     ,
  keywords        ,
  add_time        ,
  is_on_sale      ,
  is_complete     ,
  is_new          ,
  cat_id          ,
  first_cat_id    ,
  first_cat_name  ,
  second_cat_id   ,
  second_cat_name ,
  third_cat_id    ,
  third_cat_name  ,
  mct_id          ,
  shop_price      ,
  shipping_fee    ,
  goods_weight    ,
  first_on_time   ,
  first_off_time  ,
  last_on_time    ,
  last_off_time   ,
  goods_thumb
from
dim.dim_vova_goods
where is_on_sale = 1
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

