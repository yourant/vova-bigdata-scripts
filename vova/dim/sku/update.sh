#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
with dim_goods_sku_color as
(
select
sku_id,
img_color,
color_is_show
from
(
select
sku_id,
img_color,
color_is_show,
row_number() OVER (PARTITION BY sku_id ORDER BY color_is_show DESC) AS rank
from
(
select
ss.sku_id,
sv.value img_color,
if(ss.is_show=1 and ss.is_delete=0,1,0) color_is_show
from ods_vova_vts.ods_vova_sku_style ss
left join ods_vova_vts.ods_vova_style_value sv on ss.style_value_id = sv.style_value_id
where sv.style_name_id=457
) t
) t where rank = 1
)
insert overwrite table dim.dim_vova_goods_sku
select
'vova' datasource,
gs.sku_id,
gs.sku,
gs.goods_id,
gs.is_delete,
gs.img_id,
gc.img_color,
gc.color_is_show,
gs.shop_price,
gs.shipping_fee,
gs.goods_weight,
gs.sale_status,
gs.create_time
from ods_vova_vts.ods_vova_goods_sku gs
left join dim_goods_sku_color gc on gs.sku_id = gc.sku_id;
"
spark-sql --conf "spark.app.name=dim_vova_goods_sku"  --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



