#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
with tmp_buyers(
select
buyer_id,
datediff('${cur_date}',reg_time) as reg_gap_day
from dim.dim_vova_buyers
where to_date(reg_time)<='${cur_date}'
),
tmp_today_buyer as(
select distinct buyer_id from dwd.dwd_vova_log_screen_view where pt='${cur_date}'
),
tmp_today_login_buyer as(
select
tmp_buyers.buyer_id,
tmp_buyers.reg_gap_day
from
tmp_buyers
inner join tmp_today_buyer
on tmp_buyers.buyer_id=tmp_today_buyer.buyer_id
),
tmp_pay(
select
distinct fp.buyer_id
from dwd.dwd_vova_fact_pay fp
inner join dim.dim_vova_order_goods ddog on ddog.order_goods_id = fp.order_goods_id
where to_date(fp.pay_time)='${cur_date}' and (fp.from_domain like '%api.vova%' or fp.from_domain like '%api.airyclub%')
and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
),

tmp_today_pay_buyer as(
select
tmp_today_login_buyer.buyer_id,
tmp_pay.buyer_id as pay_buyer_id,
tmp_today_login_buyer.reg_gap_day
from
tmp_today_login_buyer
left join tmp_pay
on tmp_today_login_buyer.buyer_id=tmp_pay.buyer_id
),
tmp_add_cat(
select
      distinct buyer_id
      from
      dwd.dwd_vova_log_common_click cc
      where cc.pt = '${cur_date}' and element_name ='pdAddToCartSuccess'
),
tmp_today_add_cat_buyer as(
select
tmp_today_login_buyer.buyer_id,
tmp_add_cat.buyer_id as add_cat_buyer_id,
tmp_today_login_buyer.reg_gap_day
from
tmp_today_login_buyer
left join tmp_add_cat
on tmp_today_login_buyer.buyer_id=tmp_add_cat.buyer_id
)
insert overwrite table dwb.dwb_vova_register_time_dau_rate partition(pt='${cur_date}')
select
dau_data.dau as dau,
login_data.dua_1d/dau_data.dau*100 reg_rate_1d,
login_data.dua_2_7d/dau_data.dau*100 reg_rate_2_7d,
login_data.dua_8_30d/dau_data.dau*100 reg_rate_8_30d,
login_data.dua_30d/dau_data.dau*100 reg_rate_30d,

pay_data.dua_1d_rate*100 pay_rate_1d,
pay_data.dua_2_7d_rate*100 pay_rate_2_7d,
pay_data.dua_8_30d_rate*100 pay_rate_8_30d,
pay_data.dua_30d_rate*100 pay_rate_30d,

add_cat_data.dua_1d_rate*100 order_rate_1d,
add_cat_data.dua_2_7d_rate*100 order_rate_2_7d,
add_cat_data.dua_8_30d_rate*100 order_rate_8_30d,
add_cat_data.dua_30d_rate*100 order_rate_30d
from
(select
count(distinct(if(reg_gap_day=0,buyer_id,null)) ) as dua_1d,
count(distinct(if(reg_gap_day>=1 and reg_gap_day<=6,buyer_id,null)) ) as dua_2_7d,
count(distinct(if(reg_gap_day>=7 and reg_gap_day<=29,buyer_id,null)) ) as dua_8_30d,
count(distinct(if(reg_gap_day>=30,buyer_id,null)) ) as dua_30d
from
tmp_today_login_buyer)login_data
left join
(select
count(distinct(if(reg_gap_day=0,pay_buyer_id,null)) )/count(distinct(if(reg_gap_day=0,buyer_id,null)) ) as dua_1d_rate,
count(distinct(if(reg_gap_day>=1 and reg_gap_day<=6,pay_buyer_id,null)) ) /count(distinct(if(reg_gap_day>=1 and reg_gap_day<=6,buyer_id,null)) ) as dua_2_7d_rate,
count(distinct(if(reg_gap_day>=7 and reg_gap_day<=29,pay_buyer_id,null)) ) /count(distinct(if(reg_gap_day>=7 and reg_gap_day<=29,buyer_id,null)) ) as dua_8_30d_rate,
count(distinct(if(reg_gap_day>=30,pay_buyer_id,null)) )/count(distinct(if(reg_gap_day>=30,buyer_id,null)) ) as dua_30d_rate
from
tmp_today_pay_buyer)pay_data
left join
(select
count(distinct(if(reg_gap_day=0,add_cat_buyer_id,null)) )/count(distinct(if(reg_gap_day=0,buyer_id,null)) ) as dua_1d_rate,
count(distinct(if(reg_gap_day>=1 and reg_gap_day<=7,add_cat_buyer_id,null)) )/count(distinct(if(reg_gap_day>=1 and reg_gap_day<=7,buyer_id,null)) ) as dua_2_7d_rate,
count(distinct(if(reg_gap_day>=7 and reg_gap_day<=30,add_cat_buyer_id,null)) )/count(distinct(if(reg_gap_day>=7 and reg_gap_day<=30,buyer_id,null)) ) as dua_8_30d_rate,
count(distinct(if(reg_gap_day>=30,add_cat_buyer_id,null)) ) /count(distinct(if(reg_gap_day>=30,buyer_id,null)) ) as dua_30d_rate
from
tmp_today_add_cat_buyer)add_cat_data

left join
(select count(distinct device_id) as dau from dwd.dwd_vova_log_screen_view where pt='${cur_date}') dau_data
"

spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_vova_register_time_dau_rate" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi