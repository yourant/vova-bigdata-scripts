#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "$cur_date"
#dependence
#dwd_vova_log_common_click
#dwd_vova_fact_pay
#dim_vova_devices
#dim_vova_goods
#dim_vova_order_goods
#dim_vova_merchant
sql="
INSERT OVERWRITE TABLE dwb.dwb_ac_activate_add_cart_goods PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
'${cur_date}' AS event_date,
dg.virtual_goods_id,
dg.goods_id,
dg.second_cat_name,
dm.mct_name,
base_goods.region_code,
dg.shop_price + dg.shipping_fee AS shop_price_amount,
base_goods.add_cart_uv,
nvl(pay_data.sale_cnt, 0) AS sale_cnt,
nvl(pay_data.sale_order_cnt, 0) AS paid_order_cnt,
nvl(pay_data.paid_uv, 0) AS paid_uv,
nvl(pay_data.gmv, 0) AS gmv
from
(
SELECT nvl(dg.goods_id, 'all') AS goods_id,
       nvl(nvl(log.geo_country, 'NALL') , 'all') AS region_code,
       count(distinct log.device_id) AS add_cart_uv
FROM dwd.dwd_vova_log_common_click log
INNER JOIN dim.dim_vova_devices dd on dd.device_id = log.device_id
INNER JOIN dim.dim_vova_goods dg on dg.virtual_goods_id = log.element_id
WHERE log.pt = '${cur_date}'
  AND log.platform = 'mob'
  AND log.datasource = 'airyclub'
  AND log.dp = 'airyclub'
  AND log.element_name = 'pdAddToCartClick'
  AND dd.datasource = 'airyclub'
  AND date(dd.activate_time) = '${cur_date}'
GROUP BY CUBE (dg.goods_id, nvl(log.geo_country, 'NALL'))
) base_goods
LEFT JOIN
(
SELECT
nvl(fp.goods_id, 'all') AS goods_id,
nvl(fp.region_code, 'all') AS region_code,
sum(fp.goods_number * fp.shop_price + fp.shipping_fee) AS gmv,
sum(fp.goods_number) AS sale_cnt,
count(DISTINCT fp.order_goods_id) AS sale_order_cnt,
count(DISTINCT fp.buyer_id) AS paid_uv
FROM
dwd.dwd_vova_fact_pay fp
INNER JOIN dim.dim_vova_order_goods dog on dog.order_goods_id = fp.order_goods_id
INNER JOIN (
SELECT dg.goods_id,
       log.device_id
FROM dwd.dwd_vova_log_common_click log
INNER JOIN dim.dim_vova_devices dd on dd.device_id = log.device_id
INNER JOIN dim.dim_vova_goods dg on dg.virtual_goods_id = log.element_id
WHERE log.pt = '${cur_date}'
  AND log.platform = 'mob'
  AND log.datasource = 'airyclub'
  AND log.dp = 'airyclub'
  AND log.element_name = 'pdAddToCartClick'
  AND dd.datasource = 'airyclub'
  AND date(dd.activate_time) = '${cur_date}'
  GROUP BY dg.goods_id,log.device_id
) t1 on t1.goods_id = fp.goods_id and t1.device_id = fp.device_id
WHERE date(fp.pay_time) = '${cur_date}'
AND fp.datasource = 'airyclub'
AND dog.order_source = 'app'
GROUP BY CUBE (fp.goods_id, fp.region_code)
) pay_data on pay_data.goods_id = base_goods.goods_id AND pay_data.region_code = base_goods.region_code
INNER JOIN dim.dim_vova_goods dg on dg.goods_id = base_goods.goods_id
INNER JOIN dim.dim_vova_merchant dm on dm.mct_id = dg.mct_id
WHERE base_goods.region_code IN ('all', 'GB', 'FR', 'DE', 'IT', 'ES')

"

#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=dwb_ac_activate_add_cart_goods" -e "$sql"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi