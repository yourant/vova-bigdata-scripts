#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
with tmp_pay as(
select
month_gap,
goods_sn,
sales_vol,
gmv,
row_number() over(partition by month_gap order by gmv desc) rk
from
(select
cast(months_between(substr('${cur_date}',0,7),from_unixtime(unix_timestamp(fp.pay_time),'yyyy-MM')) as int) as month_gap,
dg.goods_sn,
sum(fp.goods_number) sales_vol,
sum(fp.goods_number*fp.shop_price+fp.shipping_fee) as gmv
from dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_goods dg on fp.goods_id = dg.goods_id
where months_between(substr('${cur_date}',0,7),from_unixtime(unix_timestamp(fp.pay_time),'yyyy-MM'))<=18
group by from_unixtime(unix_timestamp(fp.pay_time),'yyyy-MM'),dg.goods_sn)
),
tmp_all_gsn(
select distinct goods_sn from tmp_pay where rk<=500
),
tmp_last_week_sn_pay(
select
dg.goods_sn,
sum(goods_number) as sales_vol_1w
from dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_goods dg on fp.goods_id = dg.goods_id
where to_date(pay_time)<='${cur_date}' and to_date(pay_time)>date_sub('${cur_date}',7)
group by dg.goods_sn
),
tmp_last_week_goods_pay(
select
goods_sn,
gmv_1w/sales_vol_1w as price_avg_1w
from
(select
dg.goods_sn,
fp.goods_id,
sum(fp.goods_number) as sales_vol_1w,
sum(fp.goods_number*fp.shop_price+fp.shipping_fee) as gmv_1w,
row_number() over(partition by dg.goods_sn order by sum(fp.goods_number) desc) rk
from dwd.dwd_vova_fact_pay fp
left join dim.dim_vova_goods dg on fp.goods_id = dg.goods_id
where to_date(pay_time)<='${cur_date}' and to_date(pay_time)>date_sub('${cur_date}',7)
group by dg.goods_sn,fp.goods_id)
where rk=1
)

insert overwrite table dwb.dwb_vova_goods_sn_top500_avg_price_his partition(pt='${cur_date}')
select
tmp1.goods_sn,
nvl(tmp2.sales_vol_1w,0) as sales_vol_1w,
nvl(tmp3.price_avg_1w,0) as price_avg_1w,
tmp1.avg_price_current_month,
tmp1.avg_price_last_1_month,
tmp1.avg_price_last_2_month,
tmp1.avg_price_last_3_month,
tmp1.avg_price_last_4_month,
tmp1.avg_price_last_5_month,
tmp1.avg_price_last_6_month,
tmp1.avg_price_last_7_month,
tmp1.avg_price_last_8_month,
tmp1.avg_price_last_9_month,
tmp1.avg_price_last_10_month,
tmp1.avg_price_last_11_month,
tmp1.avg_price_last_12_month,
tmp1.avg_price_last_13_month,
tmp1.avg_price_last_14_month,
tmp1.avg_price_last_15_month,
tmp1.avg_price_last_16_month,
tmp1.avg_price_last_17_month,
tmp1.avg_price_last_18_month
from
(select
tmp_pay.goods_sn,
nvl(sum(if(month_gap=0,gmv,0))/sum(if(month_gap=0,sales_vol,0)),0) as avg_price_current_month,
nvl(sum(if(month_gap=1,gmv,0))/sum(if(month_gap=1,sales_vol,0)),0) as avg_price_last_1_month,
nvl(sum(if(month_gap=2,gmv,0))/sum(if(month_gap=2,sales_vol,0)),0) as avg_price_last_2_month,
nvl(sum(if(month_gap=3,gmv,0))/sum(if(month_gap=3,sales_vol,0)),0) as avg_price_last_3_month,
nvl(sum(if(month_gap=4,gmv,0))/sum(if(month_gap=4,sales_vol,0)),0) as avg_price_last_4_month,
nvl(sum(if(month_gap=5,gmv,0))/sum(if(month_gap=5,sales_vol,0)),0) as avg_price_last_5_month,
nvl(sum(if(month_gap=6,gmv,0))/sum(if(month_gap=6,sales_vol,0)),0) as avg_price_last_6_month,
nvl(sum(if(month_gap=7,gmv,0))/sum(if(month_gap=7,sales_vol,0)),0) as avg_price_last_7_month,
nvl(sum(if(month_gap=8,gmv,0))/sum(if(month_gap=8,sales_vol,0)),0) as avg_price_last_8_month,
nvl(sum(if(month_gap=9,gmv,0))/sum(if(month_gap=9,sales_vol,0)),0) as avg_price_last_9_month,
nvl(sum(if(month_gap=10,gmv,0))/sum(if(month_gap=10,sales_vol,0)),0) as avg_price_last_10_month,
nvl(sum(if(month_gap=11,gmv,0))/sum(if(month_gap=11,sales_vol,0)),0) as avg_price_last_11_month,
nvl(sum(if(month_gap=12,gmv,0))/sum(if(month_gap=12,sales_vol,0)),0) as avg_price_last_12_month,
nvl(sum(if(month_gap=13,gmv,0))/sum(if(month_gap=13,sales_vol,0)),0) as avg_price_last_13_month,
nvl(sum(if(month_gap=14,gmv,0))/sum(if(month_gap=14,sales_vol,0)),0) as avg_price_last_14_month,
nvl(sum(if(month_gap=15,gmv,0))/sum(if(month_gap=15,sales_vol,0)),0) as avg_price_last_15_month,
nvl(sum(if(month_gap=16,gmv,0))/sum(if(month_gap=16,sales_vol,0)),0) as avg_price_last_16_month,
nvl(sum(if(month_gap=17,gmv,0))/sum(if(month_gap=17,sales_vol,0)),0) as avg_price_last_17_month,
nvl(sum(if(month_gap=18,gmv,0))/sum(if(month_gap=18,sales_vol,0)),0) as avg_price_last_18_month
from
tmp_pay
inner join tmp_all_gsn
on tmp_pay.goods_sn = tmp_all_gsn.goods_sn
group by
tmp_pay.goods_sn)tmp1
left join tmp_last_week_sn_pay tmp2
on tmp1.goods_sn=tmp2.goods_sn
left join tmp_last_week_goods_pay tmp3
on tmp1.goods_sn=tmp3.goods_sn
"

spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_goods_sn_top500_avg_price_his" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=300000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi