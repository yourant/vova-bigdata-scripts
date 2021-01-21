#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
INSERT OVERWRITE TABLE dim.dim_zq_order_goods
select
oi.project_name AS datasource,
r.region_code,
if(oi.from_domain like '%api%', 'web', 'pc') AS platform,
oi.from_domain,
oi.order_id,
oi.pay_time,
oi.pay_status,
oi.order_time,
og.rec_id AS order_goods_id,
og.goods_id,
vg.virtual_goods_id,
oi.user_id AS buyer_id,
og.goods_number,
og.shop_price,
og.goods_number * og.shop_price AS gmv,
oe.ext_value AS domain_userid
from
ods_zq_zsp.ods_zq_order_info oi
inner join ods_zq_zsp.ods_zq_order_goods og on oi.order_id = og.order_id
left join ods_zq_zsp.ods_zq_virtual_goods vg on vg.goods_id = og.goods_id
left join ods_zq_zsp.ods_zq_order_extension oe on oi.order_id = oe.order_id and oe.ext_name = 'user_domain_id'
left join ods_zq_zsp.ods_zq_region r ON r.region_id = oi.country
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=dim_zq_order_goods" --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.sql.output.merge=true"  --conf "spark.sql.output.coalesceNum=20" -e "$sql"
#如果脚本失败，则报错

if [ $? -ne 0 ];then
  exit 1
fi
