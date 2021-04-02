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
-- 当天新增屏蔽商品当天对应的 goods_sn
insert overwrite table dwb.dwb_vova_goods_gsn partition(pt='${cur_date}')
select
  t1.goods_id,
  dg.goods_sn,
  dg.is_on_sale
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
) t1
left join
  dim.dim_vova_goods dg
on t1.goods_id = dg.goods_id
where dg.datasource = 'vova'
  and dg.goods_id is not null
;

-- 当天的数据，只计算当天的数据
insert OVERWRITE TABLE dwb.dwb_vova_high_refund_goods_restrict_today PARTITION (pt='${cur_date}')
select /*+ REPARTITION(1) */
restrict_goods_cnt, -- 累计屏蔽商品总数
new_add_restrict_goods_cnt, -- 每日新增屏蔽商品数
round(gmv_day14/14, 4), -- 新增屏蔽商品过去14天日均GMV
goods_cnt, -- 总商品数
nlrf_rate_gt_20_goods_cnt, -- 非物流退款率＞20%的商品数
sales_goods_cnt, -- 有销量的商品数
restrict_gsn_cnt -- 当日新增屏蔽的（id为0的GSN数及SN数量）
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
left join
(
  select
    '${cur_date}' pt,
    count(distinct gs_id) nlrf_rate_gt_20_goods_cnt -- 非物流退款率＞20%的商品数
  from
    ads.ads_vova_goods_portrait
  where pt ='${cur_date}'
    and nlrf_rate_5_8w >= 0.2
) t3
on t1.pt = t3.pt
left join
(
  select
    '${cur_date}' pt,
    count(distinct goods_id) sales_goods_cnt -- 有销量的商品数
  from
    dwd.dwd_vova_fact_pay
  where to_date(pay_time) <= '${cur_date}'
    AND date(pay_time) > date_sub('${cur_date}', 63)
) t4
on t1.pt = t4.pt
left join
(
  select
    '${cur_date}' pt,
    count(if((goods_sn like 'GSN%' and gsn_on_sale_goods_cnt > 0) or goods_sn like 'SN%', goods_sn, null)) restrict_gsn_cnt -- 当日新增屏蔽的（id为0的GSN数及SN数量）
  from
  (
    select
      t1.goods_sn,
      count(distinct(if(dg.is_on_sale = 1, dg.goods_id, null))) gsn_on_sale_goods_cnt -- goods_sn 下在售商品数
    from
    (
      select
        goods_sn
      from
        dwb.dwb_vova_goods_gsn
      where pt = '${cur_date}'
      group by goods_sn
    ) t1
    left join
      dim.dim_vova_goods dg
    on t1.goods_sn = dg.goods_sn
    group by t1.goods_sn
  )
) t5
on t1.pt = t5.pt
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


###逻辑sql
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert OVERWRITE TABLE dwb.dwb_vova_high_refund_goods_restrict_monitor PARTITION (pt)
select /*+ REPARTITION(1) */
restrict_goods_cnt, -- 累计屏蔽商品总数
new_add_restrict_goods_cnt, -- 每日新增屏蔽商品数
gmv_day14, -- 新增屏蔽商品过去14天日均GMV
goods_cnt, -- 总商品数
nlrf_rate_gt_20_goods_cnt, -- 非物流退款率＞20%的商品数
sales_goods_cnt, -- 有销量的商品数
restrict_gsn_cnt, -- 当日新增屏蔽的（id为0的GSN数及SN数量）
restrict_sold_out_gsn_cnt,
round(nvl(nlrf_rate_gt_20_goods_cnt / sales_goods_cnt, 0), 4) high_nlrf_goods_rate,
round(nvl(restrict_sold_out_gsn_cnt / restrict_gsn_cnt, 0), 4) restrict_gsn_rate,
t1.pt
from
  dwb.dwb_vova_high_refund_goods_restrict_today t1
left join
(
  select
    pt,
    count(distinct goods_sn_new) restrict_sold_out_gsn_cnt
  from
  (
    select
      dgg.pt pt,
      dg.goods_sn goods_sn_new, -- 变更后的 goods_sn
      count(distinct(if(dg.is_on_sale = 1, dg.goods_id, null))) on_sale_goods_cnt -- 在架商品数
    from
      dwb.dwb_vova_goods_gsn dgg
    left join
      dim.dim_vova_goods dg
    on dgg.goods_id = dg.goods_id
    where pt <= '${cur_date}' and pt >= date_sub('${cur_date}', 7)
      and dgg.goods_sn like 'SN%' and dg.goods_sn like 'GSN%'
    group by dgg.pt, dg.goods_sn
  )
  where on_sale_goods_cnt = 0
  group by pt
) t2
on t1.pt = t2.pt
where t1.pt<='${cur_date}' and t1.pt >= date_sub('${cur_date}', 7)
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

