#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: ${cur_date}"

job_name="ads_vova_royalty_threshold_detail_d_req9531_chenkai"

###逻辑sql
sql="
insert overwrite table ads.ads_vova_royalty_threshold_detail_d partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  'vova',
  fp.first_cat_id,
  '0',
  rgps.group_id,
  sum(fp.shop_price * fp.goods_number + fp.shipping_fee) gmv
from
  dwd.dwd_vova_fact_pay fp
left join
  ods_vova_vbts.ods_vova_rec_gid_pic_similar rgps
on fp.goods_id = rgps.goods_id
where from_unixtime(to_unix_timestamp(fp.pay_time), 'yyyy-MM') = substr('${cur_date}',0,7)
  and fp.datasource = 'vova'
group by fp.first_cat_id, rgps.group_id
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
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

