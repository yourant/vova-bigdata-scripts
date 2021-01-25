#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_month=`date -d $cur_date"-1 month" +%Y-%m-%d`
###逻辑sql

echo "cur_date:$cur_date,pre_month:$pre_month"

sql="
with mct_limited as(
select merchant_id as mct_id,limit_end_time  from ods_vova_vts.ods_vova_merchant_assessment_score
where  limit_status = 1 and to_date(limit_end_time)>='${cur_date}'
),
mct_limited_gmv as(
select
sum(fp.shop_price * fp.goods_number + fp.shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay fp
 inner join (select mct_id from  mct_limited) mcl on fp.mct_id=mcl.mct_id
where to_date(fp.pay_time) <='${cur_date}' and to_date(fp.pay_time) >='${pre_month}'
),
mct_limited_goods_cnt(
select count(distinct goods_id) as goods_cnt from
(SELECT
        dg.goods_id
      FROM
        dim.dim_vova_goods dg
      inner join (select mct_id from  mct_limited) mcl on dg.mct_id=mcl.mct_id
      WHERE
        dg.is_on_sale = 1
union
        SELECT DISTINCT(gos.goods_id) FROM ods_vova_vts.ods_vova_goods_on_sale_record gos
        inner join dim.dim_vova_goods dg on dg.goods_id= gos.goods_id
        inner join (select mct_id from  mct_limited) mcl on dg.mct_id=mcl.mct_id
        WHERE action = 'on'
        and to_date(create_time) <='${cur_date}' and to_date(create_time) >='${pre_month}')
)
insert overwrite table  dwb.dwb_vova_mct_limited PARTITION (pt = '${cur_date}')
select
tmp_data.cmt_cnt,
tmp_data.min_limit_end_time,
tmp_data.max_limit_end_time,
mct_limited_gmv.gmv,
mct_limited_goods_cnt.goods_cnt
from
(select  count(*) cmt_cnt,
        min(mct_limited.limit_end_time) as min_limit_end_time,
        max(mct_limited.limit_end_time) as max_limit_end_time
        from mct_limited) tmp_data
        left join mct_limited_gmv
        left join mct_limited_goods_cnt

"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 5G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_vova_mct_limited" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 500" \
--conf "spark.sql.shuffle.partitions=500" \
--conf "spark.dynamicAllocation.maxExecutors=50" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi