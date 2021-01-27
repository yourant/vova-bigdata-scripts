#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-0 day" +%Y-%m-%d`
fi
###逻辑sql
echo "$cur_date"

sql="
insert overwrite table ads.ads_vova_mct_fulfillment_order
select
/*+ REPARTITION(1) */
fin.mct_id,
dm.reg_time,
case when datediff('${cur_date}',dm.reg_time)<=90 then 1 else 0 end is_new,
fin.fulfillment_order_cnt,
current_timestamp() AS last_update_time
from
(
select
t1.mct_id,
count(distinct t1.order_goods_id) as fulfillment_order_cnt
from
(
select
dog.order_goods_id,
dog.mct_id,
fl.delivered_time,
fr.refund_type_id,
if(fr.refund_type_id = 2 ,fr.create_time, null) as refund_time,
if(fr.refund_type_id = 2 ,datediff(fr.create_time, fl.delivered_time), null) as d_diff
from
dim.dim_vova_order_goods dog
inner join dwd.dwd_vova_fact_logistics fl on fl.order_goods_id = dog.order_goods_id
LEFT join dwd.dwd_vova_fact_refund fr on fr.order_goods_id = dog.order_goods_id
where dog.pay_status >= 1
and dog.sku_order_status != 5
and dog.sku_shipping_status = 2
and date(fl.delivered_time) < date_sub('${cur_date}', 10)
) t1
where d_diff is null or d_diff > 10
group by mct_id
) fin
inner join dim.dim_vova_merchant dm on dm.mct_id = fin.mct_id
;
"
spark-sql  --queue important --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=ads_vova_mct_fulfillment_order_zhengzhiyu" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi