#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
with rpt_shield_goods as (
select
sg.event_date,
g.goods_sn,
sg.goods_id,
g.first_cat_name,
g.second_cat_name,
g.virtual_goods_id,
sg.shield_cnt
from
(
select
date_add('$cur_date',2 - dayofweek('$cur_date')) event_date,
goods_id,
sum(shield_cnt) as shield_cnt
from ads.ads_vova_shield_goods gs
where gs.pt>= date_add('$cur_date',2 - dayofweek('$cur_date')) and gs.pt<='$cur_date'
group by date_add('$cur_date',2 - dayofweek('$cur_date')), goods_id
) sg inner join dim.dim_vova_goods g on g.goods_id = sg.goods_id
),
rpt_shield_goods_gsn_is_flow as (
select
t2.goods_sn,
'Y' is_flow
from
(
select
g.goods_sn,
case when mr.rank =5 then 'Y' else 'N' end is_flow
from
dim.dim_vova_goods g
inner join
(
select
goods_sn
from
rpt_shield_goods
group by goods_sn
) t1 on g.goods_sn = t1.goods_sn
left join ads.ads_vova_mct_rank mr on mr.mct_id = g.mct_id and mr.first_cat_id = g.first_cat_id
where mr.pt='$cur_date'
) t2 where t2.is_flow ='Y'
group by t2.goods_sn
),
--GSN维度
rpt_shield_goods_sn as (
select
event_date,
goods_sn,
first(first_cat_name) first_cat_name,
first(second_cat_name) second_cat_name,
sum(shield_cnt) shield_cnt
from rpt_shield_goods
group by
event_date,
goods_sn
)
insert overwrite table dwb.dwb_vova_shield_goods_sn PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
g.event_date,
g.goods_sn,
nvl(g.first_cat_name,'') first_cat_name,
nvl(g.second_cat_name,'') second_cat_name,
g.shield_cnt,
nvl(t1.gmv_1w,0) gmv_1w,
nvl(t2.gmv_last_w,0) gmv_last_w,
nvl(sgf.is_flow,'N') is_flow
from
rpt_shield_goods_sn g
left join
(
select
gs.goods_sn,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) gmv_1w
from rpt_shield_goods_sn gs
left join dwd.dwd_vova_fact_pay fp on gs.goods_sn = fp.goods_sn
where to_date(fp.pay_time)>= date_add('$cur_date',2 - dayofweek('$cur_date')) and to_date(fp.pay_time)<='$cur_date'
group by gs.goods_sn
) t1 on g.goods_sn = t1.goods_sn
left join
(
select
gs.goods_sn,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) gmv_last_w
from rpt_shield_goods_sn gs
left join dwd.dwd_vova_fact_pay fp on gs.goods_sn = fp.goods_sn
where to_date(fp.pay_time)>= date_sub('$cur_date',dayofweek('$cur_date')+5) and to_date(fp.pay_time)<=date_sub('$cur_date',dayofweek('$cur_date')-1)
group by gs.goods_sn
) t2 on g.goods_sn = t2.goods_sn
left join rpt_shield_goods_gsn_is_flow sgf on g.goods_sn = sgf.goods_sn;
"
spark-sql --conf "spark.app.name=dwb_vova_shield_goods_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=100" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
