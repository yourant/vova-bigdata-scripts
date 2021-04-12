#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="dwb_vova_buyer_life_cycle_monitor_req5359_chenkai_${cur_date}"

###逻辑sql
sql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwb.dwb_vova_buyer_life_cycle_monitor PARTITION (pt)
select /*+ REPARTITION(1) */
  datasource,
  region_code,
  platform,
  main_channel,
  activate_uv, -- 当天激活用户数
  bonus_day0 , -- 当日激活用户中在当日下单的用户所用补贴之和
  if(datediff('${cur_date}', pt)>=30, bonus_day0_30,'NA') bonus_day0_30, -- 当日激活用户中在0-30天内下单的用户所用补贴之和
  activate_pay_day0_uv, -- 当天新激活下单用户数
  if(datediff('${cur_date}', pt)>=3,  activate_pay_day3_uv ,'NA') activate_pay_day3_uv , -- 3天下单用户数
  if(datediff('${cur_date}', pt)>=7,  activate_pay_day7_uv ,'NA') activate_pay_day7_uv , -- 7天下单用户数
  if(datediff('${cur_date}', pt)>=14, activate_pay_day14_uv,'NA') activate_pay_day14_uv, -- 14天下单用户数
  if(datediff('${cur_date}', pt)>=28, activate_pay_day28_uv,'NA') activate_pay_day28_uv, -- 28天下单用户数
  gmv_day0, -- 当天GMV
  if(datediff('${cur_date}', pt)>=3,  gmv_day0_3 ,'NA') gmv_day0_3 , -- 3天GMV
  if(datediff('${cur_date}', pt)>=7,  gmv_day0_7 ,'NA') gmv_day0_7 , -- 7天GMV
  if(datediff('${cur_date}', pt)>=14, gmv_day0_14,'NA') gmv_day0_14, -- 14天GMV
  if(datediff('${cur_date}', pt)>=28, gmv_day0_28,'NA') gmv_day0_28, -- 28天GMV
  order_cnt_day0, -- 当天订单数
  if(datediff('${cur_date}', pt)>=3,  order_cnt_day0_3 ,'NA') order_cnt_day0_3 , -- 3天订单数
  if(datediff('${cur_date}', pt)>=7,  order_cnt_day0_7 ,'NA') order_cnt_day0_7 , -- 7天订单数
  if(datediff('${cur_date}', pt)>=14, order_cnt_day0_14,'NA') order_cnt_day0_14, -- 14天订单数
  if(datediff('${cur_date}', pt)>=28, order_cnt_day0_28,'NA') order_cnt_day0_28, -- 28天订单数
  avg_bonus_day0, -- 当日补贴成本
  if(datediff('${cur_date}', pt)>=30, avg_bonus_day0_30,'NA') avg_bonus_day0_30, -- 30天补贴成本
  liv_day0, -- 当天LTV
  if(datediff('${cur_date}', pt)>=3,  liv_day0_3 ,'NA') liv_day0_3 , -- 3天LTV
  if(datediff('${cur_date}', pt)>=7,  liv_day0_7 ,'NA') liv_day0_7 , -- 7天LTV
  if(datediff('${cur_date}', pt)>=14, liv_day0_14,'NA') liv_day0_14, -- 14天LTV
  if(datediff('${cur_date}', pt)>=28, liv_day0_28,'NA') liv_day0_28, -- 28天LTV
  cr, -- 当天转化率
  if(datediff('${cur_date}', pt)>=3,  cr_day0_3 ,'NA') cr_day0_3 ,-- 3天转化率
  if(datediff('${cur_date}', pt)>=7,  cr_day0_7 ,'NA') cr_day0_7 ,-- 7天转化率
  if(datediff('${cur_date}', pt)>=14, cr_day0_14,'NA') cr_day0_14,-- 14天转化率
  if(datediff('${cur_date}', pt)>=28, cr_day0_28,'NA') cr_day0_28,-- 28天转化率
  avg_order_cnt, -- 当天平均订单数
  if(datediff('${cur_date}', pt)>=3,  avg_order_cnt_day0_3 ,'NA') avg_order_cnt_day0_3 , -- 3天平均订单数
  if(datediff('${cur_date}', pt)>=7,  avg_order_cnt_day0_7 ,'NA') avg_order_cnt_day0_7 , -- 7天平均订单数
  if(datediff('${cur_date}', pt)>=14, avg_order_cnt_day0_14,'NA') avg_order_cnt_day0_14, -- 14天平均订单数
  if(datediff('${cur_date}', pt)>=28, avg_order_cnt_day0_28,'NA') avg_order_cnt_day0_28,  -- 28天平均订单数
  -- 新增
  if(datediff('${cur_date}', pt)>=60,  activate_pay_day60_uv,'NA')  activate_pay_day60_uv, -- 60天下单用户数
  if(datediff('${cur_date}', pt)>=90,  activate_pay_day90_uv,'NA')  activate_pay_day90_uv, -- 90天下单用户数
  if(datediff('${cur_date}', pt)>=180, activate_pay_day180_uv,'NA') activate_pay_day180_uv, -- 180天下单用户数
  if(datediff('${cur_date}', pt)>=60,  gmv_day0_60,'NA')  gmv_day0_60, -- 60天GMV
  if(datediff('${cur_date}', pt)>=90,  gmv_day0_90,'NA')  gmv_day0_90, -- 90天GMV
  if(datediff('${cur_date}', pt)>=180, gmv_day0_180,'NA') gmv_day0_180, -- 180天GMV
  if(datediff('${cur_date}', pt)>=60,  order_cnt_day0_60,'NA')  order_cnt_day0_60, -- 60天订单数
  if(datediff('${cur_date}', pt)>=90,  order_cnt_day0_90,'NA')  order_cnt_day0_90, -- 90天订单数
  if(datediff('${cur_date}', pt)>=180, order_cnt_day0_180,'NA') order_cnt_day0_180, -- 180天订单数
  if(datediff('${cur_date}', pt)>=60,  liv_day0_60,'NA')  liv_day0_60, -- 60天LTV
  if(datediff('${cur_date}', pt)>=90,  liv_day0_90,'NA')  liv_day0_90, -- 90天LTV
  if(datediff('${cur_date}', pt)>=180, liv_day0_180,'NA') liv_day0_180, -- 180天LTV
  if(datediff('${cur_date}', pt)>=60,  cr_day0_60,'NA')  cr_day0_60,-- 60天转化率
  if(datediff('${cur_date}', pt)>=90,  cr_day0_90,'NA')  cr_day0_90,-- 90天转化率
  if(datediff('${cur_date}', pt)>=180, cr_day0_180,'NA') cr_day0_180,-- 180天转化率
  if(datediff('${cur_date}', pt)>=60,  avg_order_cnt_day0_60,'NA')  avg_order_cnt_day0_60,  -- 60天平均订单数
  if(datediff('${cur_date}', pt)>=90,  avg_order_cnt_day0_90,'NA')  avg_order_cnt_day0_90,  -- 90天平均订单数
  if(datediff('${cur_date}', pt)>=180, avg_order_cnt_day0_180,'NA') avg_order_cnt_day0_180,  -- 180天平均订单数
  pt
from
(
select 
  nvl(datasource, 'all')    datasource,
  nvl(region_code, 'all')   region_code,
  nvl(platform, 'all')      platform,
  nvl(main_channel, 'all')  main_channel,
  count(distinct device_id) activate_uv, -- 当天激活用户数
  sum(bonus_day0)           bonus_day0, -- 当日激活用户中在当日下单的用户所用补贴之和
  sum(bonus_day0_30)        bonus_day0_30, -- 当日激活用户中在0-30天内下单的用户所用补贴之和
  count(distinct activate_pay_device_id)         activate_pay_day0_uv , -- 当天新激活下单用户数
  count(distinct activate_pay_device_id_day0_3 ) activate_pay_day3_uv , -- 3天下单用户数
  count(distinct activate_pay_device_id_day0_7 ) activate_pay_day7_uv , -- 7天下单用户数
  count(distinct activate_pay_device_id_day0_14) activate_pay_day14_uv, -- 14天下单用户数
  count(distinct activate_pay_device_id_day0_28) activate_pay_day28_uv, -- 28天下单用户数
  count(distinct activate_pay_device_id_day0_60) activate_pay_day60_uv, -- 60天下单用户数
  count(distinct activate_pay_device_id_day0_90) activate_pay_day90_uv, -- 90天下单用户数
  count(distinct activate_pay_device_id_day0_180) activate_pay_day180_uv, -- 180天下单用户数

  sum(gmv_day0   ) gmv_day0   , -- 当天GMV
  sum(gmv_day0_3 ) gmv_day0_3 , -- 3天GMV
  sum(gmv_day0_7 ) gmv_day0_7 , -- 7天GMV
  sum(gmv_day0_14) gmv_day0_14, -- 14天GMV
  sum(gmv_day0_28) gmv_day0_28, -- 28天GMV
  sum(gmv_day0_60) gmv_day0_60, -- 60天GMV
  sum(gmv_day0_90) gmv_day0_90, -- 90天GMV
  sum(gmv_day0_180) gmv_day0_180, -- 180天GMV

  count(distinct order_id_day0   ) order_cnt_day0   , -- 当天订单数
  count(distinct order_id_day0_3 ) order_cnt_day0_3 , -- 3天订单数
  count(distinct order_id_day0_7 ) order_cnt_day0_7 , -- 7天订单数
  count(distinct order_id_day0_14) order_cnt_day0_14, -- 14天订单数
  count(distinct order_id_day0_28) order_cnt_day0_28, -- 28天订单数
  count(distinct order_id_day0_60) order_cnt_day0_60, -- 60天订单数
  count(distinct order_id_day0_90) order_cnt_day0_90, -- 90天订单数
  count(distinct order_id_day0_180) order_cnt_day0_180, -- 180天订单数

  round(nvl(sum(bonus_day0) / count(distinct device_id), 0), 4) avg_bonus_day0, -- 当日补贴成本
  round(nvl(sum(bonus_day0_30) / count(distinct device_id), 0), 4) avg_bonus_day0_30, -- 30天补贴成本
  round(nvl(sum(gmv_day0   ) / count(distinct device_id), 0), 4) liv_day0   , -- 当天LTV
  round(nvl(sum(gmv_day0_3 ) / count(distinct device_id), 0), 4) liv_day0_3 , -- 3天LTV
  round(nvl(sum(gmv_day0_7 ) / count(distinct device_id), 0), 4) liv_day0_7 , -- 7天LTV
  round(nvl(sum(gmv_day0_14) / count(distinct device_id), 0), 4) liv_day0_14, -- 14天LTV
  round(nvl(sum(gmv_day0_28) / count(distinct device_id), 0), 4) liv_day0_28, -- 28天LTV
  round(nvl(sum(gmv_day0_60) / count(distinct device_id), 0), 4) liv_day0_60, -- 60天LTV
  round(nvl(sum(gmv_day0_90) / count(distinct device_id), 0), 4) liv_day0_90, -- 90天LTV
  round(nvl(sum(gmv_day0_180) / count(distinct device_id), 0), 4) liv_day0_180, -- 180天LTV

  round(nvl(count(distinct activate_pay_device_id) / count(distinct device_id), 0), 4) cr, -- 当天转化率
  round(nvl(count(distinct activate_pay_device_id_day0_3 ) / count(distinct device_id), 0), 4) cr_day0_3 ,-- 3天转化率
  round(nvl(count(distinct activate_pay_device_id_day0_7 ) / count(distinct device_id), 0), 4) cr_day0_7 ,-- 7天转化率
  round(nvl(count(distinct activate_pay_device_id_day0_14) / count(distinct device_id), 0), 4) cr_day0_14,-- 14天转化率
  round(nvl(count(distinct activate_pay_device_id_day0_28) / count(distinct device_id), 0), 4) cr_day0_28,-- 28天转化率
  round(nvl(count(distinct activate_pay_device_id_day0_60) / count(distinct device_id), 0), 4) cr_day0_60,-- 60天转化率
  round(nvl(count(distinct activate_pay_device_id_day0_90) / count(distinct device_id), 0), 4) cr_day0_90,-- 90天转化率
  round(nvl(count(distinct activate_pay_device_id_day0_180) / count(distinct device_id), 0), 4) cr_day0_180,-- 180天转化率

  round(nvl(count(distinct order_id_day0   ) / count(distinct activate_pay_device_id), 0), 4) avg_order_cnt, -- 当天平均订单数
  round(nvl(count(distinct order_id_day0_3 ) / count(distinct activate_pay_device_id_day0_3 ), 0), 4) avg_order_cnt_day0_3 , -- 3天平均订单数
  round(nvl(count(distinct order_id_day0_7 ) / count(distinct activate_pay_device_id_day0_7 ), 0), 4) avg_order_cnt_day0_7 , -- 7天平均订单数
  round(nvl(count(distinct order_id_day0_14) / count(distinct activate_pay_device_id_day0_14), 0), 4) avg_order_cnt_day0_14, -- 14天平均订单数
  round(nvl(count(distinct order_id_day0_28) / count(distinct activate_pay_device_id_day0_28), 0), 4) avg_order_cnt_day0_28,  -- 28天平均订单数
  round(nvl(count(distinct order_id_day0_60) / count(distinct activate_pay_device_id_day0_60), 0), 4) avg_order_cnt_day0_60,  -- 60天平均订单数
  round(nvl(count(distinct order_id_day0_90) / count(distinct activate_pay_device_id_day0_90), 0), 4) avg_order_cnt_day0_90,  -- 90天平均订单数
  round(nvl(count(distinct order_id_day0_180) / count(distinct activate_pay_device_id_day0_180), 0), 4) avg_order_cnt_day0_180,  -- 180天平均订单数
  nvl(pt, 'all') pt
from
(
  select
    dd.pt pt,
    if(dd.region_code in ('GB','FR','DE','IT','ES','TW','US','CH','CZ','PL'), region_code, 'others')  region_code,
    dd.datasource   datasource,
    dd.platform     platform,
    case when dd.main_channel in ('Facebook Ads') then 'Facebook Ads'
      when dd.main_channel in ('googleadwords_int') then 'googleadwords_int'
      when dd.main_channel in ('organic', 'Organic', 'youtube_organic') then 'organic'
      else 'other'
    end main_channel,
    dd.device_id    device_id, -- 当天激活用户
    fp.order_id     order_id,
    case when dd.pt = fp.pt and fp.gmv - fp.bonus > 0 then fp.bonus
      when dd.pt = fp.pt and fp.gmv - fp.bonus <= 0 then fp.gmv
      else 0
    end bonus_day0, -- 当日激活用户中在当日下单的用户所用补贴之和
    case when datediff(fp.pt, dd.pt) >= 0 and datediff(fp.pt, dd.pt) <= 30 and fp.gmv > fp.bonus then fp.bonus
      when datediff(fp.pt, dd.pt) >= 0 and datediff(fp.pt, dd.pt) <= 30 and fp.gmv <= fp.bonus then fp.gmv
      else 0 
    end bonus_day0_30, -- 当日激活用户中在0-30天内下单的用户所用补贴之和
    if(datediff(dd.first_pay_time, dd.pt) = 0, dd.device_id, null) activate_pay_device_id, -- 当天新激活下单用户
    if(datediff(dd.first_pay_time, dd.pt) >=0 and datediff(dd.first_pay_time, dd.pt) <= 3,  dd.device_id, null) activate_pay_device_id_day0_3, -- 3天下单用户数
    if(datediff(dd.first_pay_time, dd.pt) >=0 and datediff(dd.first_pay_time, dd.pt) <= 7,  dd.device_id, null) activate_pay_device_id_day0_7, -- 7天下单用户数
    if(datediff(dd.first_pay_time, dd.pt) >=0 and datediff(dd.first_pay_time, dd.pt) <= 14, dd.device_id, null) activate_pay_device_id_day0_14, -- 14天下单用户数
    if(datediff(dd.first_pay_time, dd.pt) >=0 and datediff(dd.first_pay_time, dd.pt) <= 28, dd.device_id, null) activate_pay_device_id_day0_28, -- 28天下单用户数
    if(datediff(dd.first_pay_time, dd.pt) >=0 and datediff(dd.first_pay_time, dd.pt) <= 60, dd.device_id, null) activate_pay_device_id_day0_60, -- 60天下单用户数
    if(datediff(dd.first_pay_time, dd.pt) >=0 and datediff(dd.first_pay_time, dd.pt) <= 90, dd.device_id, null) activate_pay_device_id_day0_90, -- 90天下单用户数
    if(datediff(dd.first_pay_time, dd.pt) >=0 and datediff(dd.first_pay_time, dd.pt) <= 180, dd.device_id, null) activate_pay_device_id_day0_180, -- 180天下单用户数

    if(datediff(fp.pt, dd.pt) = 0, gmv, 0) gmv_day0, -- 当天GMV
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 3,  fp.gmv, 0) gmv_day0_3, -- 3天GMV
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 7,  fp.gmv, 0) gmv_day0_7, -- 7天GMV
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 14, fp.gmv, 0) gmv_day0_14, -- 14天GMV
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 28, fp.gmv, 0) gmv_day0_28, -- 28天GMV
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 60, fp.gmv, 0) gmv_day0_60, -- 60天GMV
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 90, fp.gmv, 0) gmv_day0_90, -- 90天GMV
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 180, fp.gmv, 0) gmv_day0_180, -- 180天GMV

    if(datediff(fp.pt, dd.pt) = 0, fp.order_id, null) order_id_day0, -- 当天订单
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 3,  fp.order_id, null) order_id_day0_3, -- 3天订单数
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 7,  fp.order_id, null) order_id_day0_7, -- 7天订单数
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 14, fp.order_id, null) order_id_day0_14, -- 14天订单数
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 28, fp.order_id, null) order_id_day0_28, -- 28天订单数
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 60, fp.order_id, null) order_id_day0_60, -- 60天订单数
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 90, fp.order_id, null) order_id_day0_90, -- 90天订单数
    if(datediff(fp.pt, dd.pt) >=0 and datediff(fp.pt, dd.pt) <= 180, fp.order_id, null) order_id_day0_180 -- 180天订单数

  from
  (
    select 
      *,
      to_date(activate_time) pt
    from 
      dim.dim_vova_devices 
    where to_date(activate_time) <= '${cur_date}' and to_date(activate_time) >= date_sub('${cur_date}', 180)
      and platform in ('ios', 'android')
  ) dd
  left join
  (
    select 
      datasource,
      order_id,
      max(device_id) device_id,
      count(distinct order_goods_id) order_goods_cnt,
      sum(shipping_fee+shop_price*goods_number) gmv,
      sum(bonus)*-1 bonus, -- bonus原是负数，转为正值
      max(to_date(pay_time)) pt
    from
      dwd.dwd_vova_fact_pay
    where to_date(pay_time) >= date_sub('${cur_date}', 180)
    group by order_id,datasource
  ) fp
  on dd.datasource = fp.datasource and dd.device_id = fp.device_id
) group by cube(
  pt,
  region_code,
  datasource,
  platform,
  main_channel
) having pt != 'all'
)
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=300" \
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
