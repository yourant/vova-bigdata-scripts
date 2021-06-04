#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
spark-sql  --conf "spark.app.name=dwb_vova_tw_first_cat_goods" --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=100"  -e "

insert overwrite table tmp.tmp_vova_TW_expre
select /*+ REPARTITION(2) */
       nvl(case
               when a.platform = 'pc' then 'pc'
               when a.platform = 'web' then 'mob'
               when a.platform = 'mob' and a.os_type = 'android' then 'android'
               when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
               else ''
               end, 'all') as                  platform,
       nvl(nvl(c.main_channel, 'NA'), 'all')   main_channel,
       nvl(CASE
               WHEN datediff(from_unixtime(bigint(a.collector_tstamp / 1000), 'yyyy-MM-dd'), c.activate_time) = 0
                   THEN 'new'
               ELSE 'old' END, 'all')          is_new,
       nvl(nvl(b.first_cat_name, 'NA'), 'all') first_cat_name,
       count(*)                                expre_pv,
       count(distinct a.device_id)             expre_uv,
       count(distinct a.virtual_goods_id)      expre_gd
from dwd.dwd_vova_log_goods_impression a
         left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
         join dim.dim_vova_devices c on a.device_id = c.device_id
where a.pt = '${cur_date}'
  and a.geo_country = 'TW'
  and a.dp = 'vova'
group by cube (
               case
                   when a.platform = 'pc' then 'pc'
                   when a.platform = 'web' then 'mob'
                   when a.platform = 'mob' and a.os_type = 'android' then 'android'
                   when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
                   else ''
                   end,
               nvl(c.main_channel, 'NA'),
               CASE
                   WHEN datediff(from_unixtime(bigint(a.collector_tstamp / 1000), 'yyyy-MM-dd'), c.activate_time) = 0
                       THEN 'new'
                   ELSE 'old' END, nvl(b.first_cat_name, 'NA'));


insert overwrite table tmp.tmp_vova_TW_clk as
select /*+ REPARTITION(2) */
       nvl(case
               when a.platform = 'pc' then 'pc'
               when a.platform = 'web' then 'mob'
               when a.platform = 'mob' and a.os_type = 'android' then 'android'
               when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
               else ''
               end, 'all') as                  platform,
       nvl(nvl(c.main_channel, 'NA'), 'all')   main_channel,
       nvl(CASE
               WHEN datediff(from_unixtime(bigint(a.collector_tstamp / 1000), 'yyyy-MM-dd'), c.activate_time) = 0
                   THEN 'new'
               ELSE 'old' END, 'all')          is_new,
       nvl(nvl(b.first_cat_name, 'NA'), 'all') first_cat_name,
       count(*)                                clk_pv
from dwd.dwd_vova_log_goods_click a
         left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
         join dim.dim_vova_devices c on a.device_id = c.device_id
where a.pt = '${cur_date}'
  and a.geo_country = 'TW'
  and a.dp = 'vova'
group by cube (
               case
                   when a.platform = 'pc' then 'pc'
                   when a.platform = 'web' then 'mob'
                   when a.platform = 'mob' and a.os_type = 'android' then 'android'
                   when a.platform = 'mob' and a.os_type = 'ios' then 'ios'
                   else ''
                   end,
               nvl(c.main_channel, 'NA'),
               CASE
                   WHEN datediff(from_unixtime(bigint(a.collector_tstamp / 1000), 'yyyy-MM-dd'), c.activate_time) = 0
                       THEN 'new'
                   ELSE 'old' END, nvl(b.first_cat_name, 'NA'));

insert overwrite table  tmp.tmp_vova_TW_pay
select /*+ REPARTITION(2) */
       nvl(nvl(a.platform,'NA'), 'all') as                  platform,
       nvl(nvl(c.main_channel, 'NA'), 'all')   main_channel,
       nvl(CASE
               WHEN datediff(a.pay_time, c.activate_time) = 0
                   THEN 'new'
               ELSE 'old' END, 'all')          is_new,
       nvl(nvl(a.first_cat_name, 'NA'), 'all') first_cat_name,
       sum(a.shop_price * a.goods_number + a.shipping_fee) gmv,
       count(distinct a.order_goods_id)             pay_sucess_order,
       count(distinct a.device_id)             pay_sucess_uv,
       count(distinct if(b.cnt > 1,b.goods_id,null))  pay_more_1_good
from dwd.dwd_vova_fact_pay a
         join dim.dim_vova_devices c on a.device_id = c.device_id
left join (select goods_id,count(1) cnt from  dwd.dwd_vova_fact_pay where to_date(pay_time) = '${cur_date}' group by goods_id) b on a.goods_id = b.goods_id
where to_date(a.pay_time) = '${cur_date}'
  and a.region_code = 'TW'
  and a.datasource = 'vova'
group by cube (
       nvl(a.platform,'NA'),
       nvl(c.main_channel, 'NA'),
       CASE
               WHEN datediff(a.pay_time, c.activate_time) = 0
                   THEN 'new'
               ELSE 'old' END,
       nvl(a.first_cat_name, 'NA'));

insert overwrite table dwb.dwb_vova_tw_first_cat_goods   PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(4) */
'${cur_date}' cur_date,
a.platform,
a.main_channel,
a.is_new,
a.first_cat_name,
a.expre_uv,
c.gmv,
c.pay_sucess_order,
round(nvl(c.pay_sucess_order / c.pay_sucess_uv,0),2)  avg_pay_cnt,
c.pay_more_1_good,
a.expre_gd,
round(nvl(c.gmv  / c.pay_sucess_uv,0),2)  avg_price,
concat(round(nvl(b.clk_pv * 100 / a.expre_pv,0),2),'%')  clk_rate,
concat(round(nvl(c.pay_sucess_uv * 100 / a.expre_uv,0),2),'%')  cge_rate
from tmp.tmp_vova_TW_expre a
join tmp.tmp_vova_TW_clk b
on a.platform = b.platform
and a.main_channel = b.main_channel
and a.is_new = b.is_new
and a.first_cat_name = b.first_cat_name
join tmp.tmp_vova_TW_pay c
on a.platform = c.platform
and a.main_channel = c.main_channel
and a.is_new = c.is_new
and a.first_cat_name = c.first_cat_name
"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

