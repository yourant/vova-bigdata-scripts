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

job_name="dwb_vova_ac_order_daily_req5359_chenkai_${cur_date}"

###逻辑sql
sql="
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
with tmp_ac_daily as(
 select
 nvl(pt, 'all') pt,
 nvl(datasource,'all') datasource,
 nvl(region_code,'all') region_code,
 nvl(main_channel,'all') main_channel,
 sum(shipping_fee+shop_price*goods_number) gmv, -- 当日营收GMV
 sum(if(datediff(pay_time, activate_time) = 0, shipping_fee+shop_price*goods_number, 0)) new_buyer_gmv, -- 新客GMV
 count(distinct(device_id)) pay_uv, -- 主流程支付订单uv
 count(distinct(order_goods_id)) order_goods_cnt, -- 该日总子订单数
 count(distinct(fl_order_goods_id)) shop_online_day7_order_goods_cnt -- 该日产生的订单在7日内返回物流揽收的订单数
 from
 (
   select
   to_date(fp.pay_time) pt,
   fp.datasource,
   fp.region_code,
   nvl(dd.main_channel,'NA') main_channel,
   fp.device_id,
   dog.order_goods_id,
   fp.shipping_fee,
   fp.shop_price,
   fp.goods_number,
   case when fp.from_domain not like '%api%' and dd.activate_time is null and dwdu.activate_time is not null then dwdu.activate_time
        else dd.activate_time
        end activate_time,
   dog.order_id,
   fp.pay_time,
   fl.order_goods_id fl_order_goods_id
   from
   dwd.dwd_vova_fact_pay fp
   left join
   dim.dim_vova_devices dd
   on dd.device_id = fp.device_id and dd.datasource=fp.datasource
   left join
   (select buyer_id, MAX(activate_time) activate_time from dim.dim_vova_web_domain_userid group by buyer_id) dwdu
   on fp.buyer_id = dwdu.buyer_id
   inner join dim.dim_vova_order_goods dog
   on dog.order_goods_id = fp.order_goods_id
   left join
   dwd.dwd_vova_fact_logistics fl
   on fp.datasource = fl.datasource and fp.order_goods_id = fl.order_goods_id
    and datediff(fl.valid_tracking_date, fp.pay_time) > 0
    and datediff(fl.valid_tracking_date, fp.pay_time) <= 7
   where to_date(fp.pay_time) <= '${cur_date}' and to_date(fp.pay_time) >= date_sub('${cur_date}', 7)
    and fp.device_id is not null and fp.datasource = 'airyclub'
 ) tmp1
 group by cube(pt, datasource, region_code, main_channel)
 HAVING datasource != 'all' and pt != 'all'
)

insert overwrite table dwb.dwb_vova_ac_order_daily PARTITION (pt)
select
t1.datasource,
t1.region_code,
t1.main_channel,
t1.gmv,
t1.new_buyer_gmv,
t1.pay_uv,
t1.order_goods_cnt,
t1.shop_online_day7_order_goods_cnt,
t2.gmv yesterday_gmv,
t3.add_cart_uv, -- 加购成功uv
t4.dau, -- DAU
nvl(t5.no_sign_refund_order_goods_cnt, 0), -- 当日产生的收货前取消的订单总数
nvl(t5.sign_refund_order_goods_cnt, 0), -- 当日产生的收货后退货退款订单（且审核通过）的订单数
t1.pt
from
tmp_ac_daily t1
left join
tmp_ac_daily t2
on t1.datasource = t2.datasource and t1.region_code = t2.region_code and t1.main_channel = t2.main_channel
and datediff(t1.pt, t2.pt) = 1
left join
(
  select
  nvl(pt,'all') pt,
  nvl(geo_country,'all') region_code,
  nvl(main_channel,'all') main_channel,
  count(DISTINCT device_id) AS add_cart_uv -- 加购成功uv
  from
  (
   select
   flcc.pt,
   nvl(flcc.geo_country,'NA') geo_country,
   nvl(dd.main_channel,'NA') main_channel,
   flcc.device_id
   from
   dwd.dwd_vova_log_common_click flcc
   left join
   dim.dim_vova_devices dd
   on flcc.device_id = dd.device_id
   where flcc.pt <='${cur_date}' and flcc.pt > date_sub('${cur_date}', 7)
    AND flcc.element_name = 'pdAddToCartSuccess'
    and flcc.dp = 'airyclub'
  ) tmp1
  group by cube(pt, geo_country, main_channel)
  HAVING pt !='all'
) t3
on t1.region_code = t3.region_code and t1.main_channel = t3.main_channel
  and t1.pt = t3.pt
left join
(
  select
  nvl(pt,'all') pt,
  nvl(geo_country,'all') region_code,
  nvl(main_channel,'all') main_channel,
  count(DISTINCT device_id) AS dau -- dau
  from
  (
   select
   flsv.pt pt,
   nvl(flsv.geo_country, 'NA') geo_country,
   nvl(dd.main_channel, 'NA') main_channel,
   flsv.device_id
   from
   dwd.dwd_vova_log_screen_view flsv
   left join
   dim.dim_vova_devices dd
   on flsv.device_id = dd.device_id
   where flsv.pt<='${cur_date}' and flsv.pt >= date_sub('${cur_date}', 7)
    and flsv.dp = 'airyclub'
  ) tmp1
  group by cube(pt, geo_country, main_channel)
  HAVING pt !='all'
) t4
on t1.region_code = t4.region_code and t1.main_channel = t4.main_channel
  and t1.pt = t4.pt
left join
(
  select
    nvl(pt, 'all') pt,
    nvl(region_code,'all') region_code,
    nvl(main_channel,'all') main_channel,
    count(distinct(if(sku_shipping_status != 2, order_goods_id, null))) no_sign_refund_order_goods_cnt,
    count(distinct(if(sku_shipping_status = 2 and sku_pay_status = 4, order_goods_id, null))) sign_refund_order_goods_cnt
  from (
    select
      to_date(fr.create_time) pt,
      dog.region_code region_code,
      nvl(dd.main_channel,'NA') main_channel,
      dog.order_goods_id,
      dog.sku_shipping_status,
      dog.sku_pay_status
    from
      dwd.dwd_vova_fact_refund fr
    LEFT JOIN
      dim.dim_vova_order_goods dog
    on fr.datasource = dog.datasource and fr.order_goods_id = dog.order_goods_id
    LEFT JOIN
      dim.dim_vova_devices dd
    on dog.device_id = dd.device_id and dog.datasource = dd.datasource
    where fr.datasource='airyclub' and dog.datasource = 'airyclub' and dog.region_code is not null
      and fr.create_time <='${cur_date}' and fr.create_time >= date_sub('${cur_date}', 7)
  ) tmp1
  group by cube(pt, region_code, main_channel)
  HAVING pt != 'all'
) t5
on t1.region_code = t5.region_code and t1.main_channel = t5.main_channel
  and t1.pt = t5.pt
where t1.pt > date_sub('${cur_date}', 7)
  and t1.region_code in ('all','GB','FR','DE','IT','ES')
  and t1.main_channel in ('all','googleadwords_int','Facebook Ads')
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 10G --executor-cores 1 \
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
