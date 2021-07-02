#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

# cur_hour=`date +%H`
# if [ ${cur_hour} -eq 00 ]; then
#   echo "0 点不执行！"
#   exit 0
# fi
# echo "cur_hour: ${cur_hour}"

#指定日期和引擎
cur_date=$1
# 每小时执行一次，每次执行当天全部时间
#默认日期为今天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: ${cur_date}"

job_name="dws_vova_buyer_goods_behave_h_req9592_gongrui_chenkai"

###逻辑sql
sql="
insert overwrite table dws.dws_vova_buyer_goods_behave_h partition(pt='${cur_date}')
select /*+ REPARTITION(5) */
  buyer_id,
  gs_id goods_id,
  cat_id,
  first_cat_id,
  second_cat_id,
  null third_cat_id,
  brand_id,
  expre_cnt impression_cnt,
  clk_cnt,
  collect_cnt,
  add_cat_cnt,
  ord_cnt
from
  dws.dws_vova_buyer_goods_behave
where expre_cnt > 0 and pt ='${cur_date}'
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


