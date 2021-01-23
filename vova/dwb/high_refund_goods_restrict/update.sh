#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

job_name="dwb_vova_high_refund_goods_restrict_monitor_req5742_chenkai_${cur_date}"

###逻辑sql
sql="
insert OVERWRITE TABLE dwb.dwb_vova_high_refund_goods_restrict_monitor PARTITION (pt='${cur_date}')
select /*+ REPARTITION(1) */
restrict_goods_cnt, -- 累计屏蔽商品总数
new_add_restrict_goods_cnt, -- 每日新增屏蔽商品数
round(gmv_day14/14, 4), -- 新增屏蔽商品过去14天日均GMV
goods_cnt -- 总商品数
from
(
  select
    '${cur_date}' pt,
    count(distinct(tmp1.goods_id)) new_add_restrict_goods_cnt, -- 新增屏蔽商品数量
    sum(dog.shipping_fee+dog.shop_price*dog.goods_number) gmv_day14 -- 新增屏蔽商品GMV
  from
  (
    select
      t1.goods_id -- 当日新增屏蔽商品
    from
    (
      select
        distinct goods_id -- 当日屏蔽商品
      from ads.ads_vova_goods_restrict_d where pt = '${cur_date}'
    ) t1
    left join
    (
      select
        distinct goods_id -- 历史屏蔽商品
      from ads.ads_vova_goods_restrict_d where pt < '${cur_date}'
    ) t2
    on t1.goods_id = t2.goods_id
    where t2.goods_id is null
  ) tmp1
  left join
    (
      select * from
        dim.dim_vova_order_goods
      where pay_status >= 1
      and datasource = 'vova'
      and to_date(pay_time) > date_sub('${cur_date}', 14) and to_date(pay_time) <= '${cur_date}'
    ) dog
  on tmp1.goods_id = dog.goods_id
) t0
left join
(
  select
    '${cur_date}' pt,
    count(distinct(goods_id)) restrict_goods_cnt -- 累计屏蔽商品
  from
    ads.ads_vova_goods_restrict_d
  where pt <= '${cur_date}'
) t1
on t0.pt = t1.pt
left join
(
  select
    '${cur_date}' pt,
    count(distinct(goods_id)) goods_cnt -- 总商品数
  from
    dim.dim_vova_goods
  where datasource = 'vova'
) t2
on t1.pt = t2.pt
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`