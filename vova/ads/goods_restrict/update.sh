#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
insert overwrite table ads.ads_vova_goods_restrict_d PARTITION (pt = '${pt}')
select
/*+ REPARTITION(1) */
goods_id,
sales_order,
nlrf_rate_5_8w
from
(
select
t1.goods_id,
sum(t1.nlrf_order_cnt_5_8w)/count(t1.order_goods_id) as nlrf_rate_5_8w,
sum(t1.goods_number) sales_order
from
(
select
og.goods_id,
og.order_goods_id,
og.goods_number,
case when fr.refund_reason_type_id != 8 and fr.refund_type_id=2 then 1 else 0 end nlrf_order_cnt_5_8w
from dim.dim_vova_order_goods og
left join dwd.dwd_vova_fact_refund fr on fr.order_goods_id=og.order_goods_id
left join dwd.dwd_vova_fact_logistics fl on fr.order_goods_id=fl.order_goods_id
where datediff('${pt}', date(og.confirm_time)) between 35 and 56
and og.sku_pay_status>1
and og.sku_shipping_status > 0
) t1 group by t1.goods_id
) t where nlrf_rate_5_8w>0.2 and sales_order >=10 and sales_order<=100;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql --conf "spark.app.name=ads_vova_goods_restrict_d_zhangyin" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi